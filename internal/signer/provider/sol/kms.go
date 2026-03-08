package solprovider

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"errors"
	"strings"

	signpb "wallet-system/proto/signer"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	awskmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/gagliardetto/solana-go"
)

type KMSSigner struct {
	keyID      string
	kms        *awskms.Client
	pubKey     solana.PublicKey
	hotAddress string
}

func NewKMSSigner(ctx context.Context, keyID string) (*KMSSigner, error) {
	keyID = strings.TrimSpace(keyID)
	if keyID == "" {
		return nil, errors.New("empty SOL KMS key id")
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	kmsClient := awskms.NewFromConfig(cfg)
	pubResp, err := kmsClient.GetPublicKey(ctx, &awskms.GetPublicKeyInput{
		KeyId: &keyID,
	})
	if err != nil {
		return nil, err
	}
	pubKey, err := solAddressFromKMSPublicKey(pubResp.PublicKey)
	if err != nil {
		return nil, err
	}
	return &KMSSigner{
		keyID:      keyID,
		kms:        kmsClient,
		pubKey:     pubKey,
		hotAddress: pubKey.String(),
	}, nil
}

func (s *KMSSigner) Sign(ctx context.Context, req *signpb.SignRequest) ([]byte, error) {
	if s == nil || s.kms == nil {
		return nil, errors.New("sol kms signer not configured")
	}
	if req == nil {
		return nil, errors.New("sign request is nil")
	}
	unsignedReq, tx, err := parseUnsignedWithdrawTx(req.GetUnsignedTx())
	if err != nil {
		return nil, err
	}
	if unsignedReq.From != s.hotAddress {
		return nil, errors.New("solana kms key does not match from address")
	}

	messageContent, err := tx.Message.MarshalBinary()
	if err != nil {
		return nil, errors.New("unable to encode message for signing")
	}
	signResp, err := s.kms.Sign(ctx, &awskms.SignInput{
		KeyId:            &s.keyID,
		Message:          messageContent,
		MessageType:      awskmstypes.MessageTypeRaw,
		SigningAlgorithm: awskmstypes.SigningAlgorithmSpecEd25519Sha512,
	})
	if err != nil {
		return nil, err
	}
	if len(signResp.Signature) != ed25519.SignatureSize {
		return nil, errors.New("invalid solana kms signature size")
	}

	signersCount := int(tx.Message.Header.NumRequiredSignatures)
	if signersCount <= 0 || signersCount > len(tx.Message.AccountKeys) {
		return nil, errors.New("invalid solana signer set")
	}
	if len(tx.Signatures) == 0 {
		tx.Signatures = make([]solana.Signature, signersCount)
	} else if len(tx.Signatures) != signersCount {
		return nil, errors.New("invalid solana signatures length")
	}

	matched := false
	for i := range signersCount {
		if tx.Message.AccountKeys[i].Equals(s.pubKey) {
			copy(tx.Signatures[i][:], signResp.Signature)
			matched = true
			break
		}
	}
	if !matched {
		return nil, errors.New("solana kms signer key not found in signer set")
	}
	if err := tx.VerifySignatures(); err != nil {
		return nil, err
	}
	return tx.MarshalBinary()
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
	return s.hotAddress
}

func solAddressFromKMSPublicKey(der []byte) (solana.PublicKey, error) {
	pubAny, err := x509.ParsePKIXPublicKey(der)
	if err != nil {
		return solana.PublicKey{}, err
	}
	pub, ok := pubAny.(ed25519.PublicKey)
	if !ok {
		return solana.PublicKey{}, errors.New("kms public key is not ed25519")
	}
	if len(pub) != solana.PublicKeyLength {
		return solana.PublicKey{}, errors.New("invalid solana kms public key length")
	}
	var out solana.PublicKey
	copy(out[:], pub)
	return out, nil
}
