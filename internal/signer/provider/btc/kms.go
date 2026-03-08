package btcprovider

import (
	"context"
	"encoding/asn1"
	"errors"
	"math/big"
	"strings"

	"wallet-system/internal/helpers"
	rootprovider "wallet-system/internal/signer/provider"
	signpb "wallet-system/proto/signer"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	awskmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/btcsuite/btcd/btcec/v2"
	btcec_ecdsa "github.com/btcsuite/btcd/btcec/v2/ecdsa"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
)

type KMSSigner struct {
	keyID      string
	chain      string
	kms        *awskms.Client
	pubKey     *btcec.PublicKey
	hotAddress string
}

type ecdsaKMSSignature struct {
	R *big.Int
	S *big.Int
}

func NewKMSSigner(ctx context.Context, keyID string, chain string) (*KMSSigner, error) {
	keyID = strings.TrimSpace(keyID)
	chain = strings.ToLower(strings.TrimSpace(chain))
	if keyID == "" {
		return nil, errors.New("empty BTC KMS key id")
	}
	params, err := chainParams(chain)
	if err != nil {
		return nil, err
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
	pubKey, err := rootprovider.ParseKMSSECP256K1PublicKey(pubResp.PublicKey)
	if err != nil {
		return nil, err
	}
	pubKeyHash := btcutil.Hash160(pubKey.SerializeCompressed())
	addr, err := btcutil.NewAddressWitnessPubKeyHash(pubKeyHash, params)
	if err != nil {
		return nil, err
	}
	return &KMSSigner{
		keyID:      keyID,
		chain:      chain,
		kms:        kmsClient,
		pubKey:     pubKey,
		hotAddress: addr.EncodeAddress(),
	}, nil
}

func (s *KMSSigner) Sign(ctx context.Context, req *signpb.SignRequest) ([]byte, error) {
	if s == nil || s.kms == nil || s.pubKey == nil {
		return nil, errors.New("btc kms signer not configured")
	}
	if req == nil {
		return nil, errors.New("sign request is nil")
	}
	if req.GetChain() != "" {
		spec, err := helpers.ResolveChainSpec(req.GetChain())
		if err != nil {
			return nil, err
		}
		if spec.CanonicalChain != s.chain {
			return nil, errors.New("btc kms signer chain mismatch")
		}
	}
	if from := strings.TrimSpace(req.GetFromAddress()); from != "" && !strings.EqualFold(from, s.hotAddress) {
		return nil, errors.New("btc kms signer from_address mismatch")
	}
	unsignedReq, err := parseUnsignedWithdrawTx(req.GetUnsignedTx())
	if err != nil {
		return nil, err
	}
	tx, err := decodeUnsignedRawTx(unsignedReq.RawTx)
	if err != nil {
		return nil, err
	}
	inputValues, pkScripts, err := parsePrevoutsForSign(tx, unsignedReq.Prevouts)
	if err != nil {
		return nil, err
	}
	if err := s.signInputsWithKMS(ctx, tx, inputValues, pkScripts); err != nil {
		return nil, err
	}
	return serializeTx(tx)
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

func (s *KMSSigner) signInputsWithKMS(ctx context.Context, tx *wire.MsgTx, inputValues []int64, pkScripts [][]byte) error {
	prevOuts := make(map[wire.OutPoint]*wire.TxOut, len(tx.TxIn))
	for i := range tx.TxIn {
		prevOuts[tx.TxIn[i].PreviousOutPoint] = &wire.TxOut{
			Value:    inputValues[i],
			PkScript: pkScripts[i],
		}
	}
	fetcher := txscript.NewMultiPrevOutFetcher(prevOuts)
	sigHashes := txscript.NewTxSigHashes(tx, fetcher)
	pubKeyBytes := s.pubKey.SerializeCompressed()
	for i := range tx.TxIn {
		hash, err := txscript.CalcWitnessSigHash(pkScripts[i], sigHashes, txscript.SigHashAll, tx, i, inputValues[i])
		if err != nil {
			return err
		}
		sigDER, err := s.signDigestDER(ctx, hash)
		if err != nil {
			return err
		}
		tx.TxIn[i].Witness = wire.TxWitness{
			append(sigDER, byte(txscript.SigHashAll)),
			pubKeyBytes,
		}
	}
	return nil
}

func (s *KMSSigner) signDigestDER(ctx context.Context, digest []byte) ([]byte, error) {
	signResp, err := s.kms.Sign(ctx, &awskms.SignInput{
		KeyId:            &s.keyID,
		Message:          digest,
		MessageType:      awskmstypes.MessageTypeDigest,
		SigningAlgorithm: awskmstypes.SigningAlgorithmSpecEcdsaSha256,
	})
	if err != nil {
		return nil, err
	}
	return normalizeKMSSignature(signResp.Signature)
}

func normalizeKMSSignature(der []byte) ([]byte, error) {
	var sig ecdsaKMSSignature
	if _, err := asn1.Unmarshal(der, &sig); err != nil {
		return nil, err
	}
	if sig.R == nil || sig.S == nil {
		return nil, errors.New("invalid kms signature")
	}
	curveN := btcec.S256().Params().N
	halfN := new(big.Int).Rsh(new(big.Int).Set(curveN), 1)
	if sig.S.Cmp(halfN) > 0 {
		sig.S = new(big.Int).Sub(curveN, sig.S)
	}
	var r btcec.ModNScalar
	var s btcec.ModNScalar
	if overflow := r.SetByteSlice(sig.R.Bytes()); overflow {
		return nil, errors.New("invalid kms signature R")
	}
	if overflow := s.SetByteSlice(sig.S.Bytes()); overflow {
		return nil, errors.New("invalid kms signature S")
	}
	return btcec_ecdsa.NewSignature(&r, &s).Serialize(), nil
}

func chainParams(chain string) (*chaincfg.Params, error) {
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return nil, err
	}
	if spec.Family != helpers.FamilyBTC {
		return nil, errors.New("BTC KMS signer requires a btc chain")
	}
	if spec.IsTestnet {
		return &chaincfg.TestNet3Params, nil
	}
	return &chaincfg.MainNetParams, nil
}
