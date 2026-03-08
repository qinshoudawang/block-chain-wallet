package evmprovider

import (
	"context"
	"encoding/asn1"
	"errors"
	"math/big"
	"strings"

	"wallet-system/internal/signer/derivation"
	rootprovider "wallet-system/internal/signer/provider"
	"wallet-system/internal/storage/repo"
	signpb "wallet-system/proto/signer"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	awskmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

type KMSSigner struct {
	keyID      string
	kms        *awskms.Client
	chainID    *big.Int
	deriver    *derivation.Deriver
	addrs      *repo.AddressRepo
	hotAddress common.Address
}

type ecdsaKMSSignature struct {
	R *big.Int
	S *big.Int
}

func NewKMSSigner(ctx context.Context, keyID string, chainID *big.Int, deriver *derivation.Deriver, addrs *repo.AddressRepo) (*KMSSigner, error) {
	keyID = strings.TrimSpace(keyID)
	if keyID == "" {
		return nil, errors.New("empty EVM KMS key id")
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	kmsClient := awskms.NewFromConfig(cfg)
	pubResp, err := kmsClient.GetPublicKey(ctx, &awskms.GetPublicKeyInput{KeyId: &keyID})
	if err != nil {
		return nil, err
	}
	addr, err := addressFromKMSPublicKey(pubResp.PublicKey)
	if err != nil {
		return nil, err
	}
	return &KMSSigner{
		keyID:      keyID,
		kms:        kmsClient,
		chainID:    chainID,
		deriver:    deriver,
		addrs:      addrs,
		hotAddress: addr,
	}, nil
}

func (s *KMSSigner) Sign(ctx context.Context, req *signpb.SignRequest) ([]byte, error) {
	if req == nil {
		return nil, errors.New("sign request is nil")
	}
	if strings.EqualFold(strings.TrimSpace(req.GetCaller()), "sweeper") {
		priv, expectedFrom, err := resolveSweepSigner(ctx, s.deriver, s.addrs, req)
		if err != nil {
			return nil, err
		}
		return signUnsignedWithKey(req.GetUnsignedTx(), s.chainID, priv, expectedFrom)
	}
	return s.signWithKMS(ctx, req.GetUnsignedTx())
}

func (s *KMSSigner) KeyID() string {
	if s == nil {
		return ""
	}
	return s.keyID
}

func (s *KMSSigner) HotAddress() string {
	if s == nil {
		return ""
	}
	return s.hotAddress.Hex()
}

func (s *KMSSigner) signWithKMS(ctx context.Context, unsignedTx []byte) ([]byte, error) {
	if s == nil || s.kms == nil {
		return nil, errors.New("evm kms signer not configured")
	}
	var tx types.Transaction
	if err := tx.UnmarshalBinary(unsignedTx); err != nil {
		return nil, err
	}
	chainID := tx.ChainId()
	if chainID == nil || chainID.Sign() <= 0 {
		return nil, errors.New("missing chain id in evm tx")
	}
	if s.chainID != nil && s.chainID.Sign() > 0 && chainID.Cmp(s.chainID) != 0 {
		return nil, errors.New("evm chain id mismatch")
	}
	signer := types.LatestSignerForChainID(chainID)
	digest := signer.Hash(&tx)
	signResp, err := s.kms.Sign(ctx, &awskms.SignInput{
		KeyId:            &s.keyID,
		Message:          digest.Bytes(),
		MessageType:      awskmstypes.MessageTypeDigest,
		SigningAlgorithm: awskmstypes.SigningAlgorithmSpecEcdsaSha256,
	})
	if err != nil {
		return nil, err
	}
	sig, err := normalizeKMSSignature(signResp.Signature)
	if err != nil {
		return nil, err
	}
	for recID := byte(0); recID < 2; recID++ {
		candidate := append(append([]byte{}, sig...), recID)
		signedTx, err := tx.WithSignature(signer, candidate)
		if err != nil {
			continue
		}
		from, err := types.Sender(signer, signedTx)
		if err != nil {
			continue
		}
		if from == s.hotAddress {
			return signedTx.MarshalBinary()
		}
	}
	return nil, errors.New("kms signature recovery id not found")
}

func addressFromKMSPublicKey(der []byte) (common.Address, error) {
	pub, err := rootprovider.ParseKMSSECP256K1PublicKey(der)
	if err != nil {
		return common.Address{}, err
	}
	return crypto.PubkeyToAddress(*pub.ToECDSA()), nil
}

func normalizeKMSSignature(der []byte) ([]byte, error) {
	var sig ecdsaKMSSignature
	if _, err := asn1.Unmarshal(der, &sig); err != nil {
		return nil, err
	}
	if sig.R == nil || sig.S == nil {
		return nil, errors.New("invalid kms signature")
	}
	curveN := crypto.S256().Params().N
	halfN := new(big.Int).Rsh(new(big.Int).Set(curveN), 1)
	if sig.S.Cmp(halfN) > 0 {
		sig.S = new(big.Int).Sub(curveN, sig.S)
	}
	out := make([]byte, 64)
	rb := sig.R.Bytes()
	sb := sig.S.Bytes()
	copy(out[32-len(rb):32], rb)
	copy(out[64-len(sb):], sb)
	return out, nil
}
