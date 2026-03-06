package provider

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	signpb "wallet-system/proto/signer"

	"github.com/gagliardetto/solana-go"
)

type SOLSigner struct {
	privKey solana.PrivateKey
	pubKey  solana.PublicKey
}

type solUnsignedWithdrawTx struct {
	From     string `json:"from"`
	TxBase64 string `json:"tx_base64"`
}

func NewSOLSigner(rawPriv string) (*SOLSigner, error) {
	priv, err := decodeSOLPrivateKey(rawPriv)
	if err != nil {
		return nil, err
	}
	return &SOLSigner{
		privKey: priv,
		pubKey:  priv.PublicKey(),
	}, nil
}

func (s *SOLSigner) Sign(ctx context.Context, req *signpb.SignRequest) ([]byte, error) {
	_ = ctx
	if s == nil || len(s.privKey) != ed25519.PrivateKeySize {
		return nil, errors.New("solana private key is required")
	}
	if req == nil {
		return nil, errors.New("sign request is nil")
	}
	unsignedTx := req.GetUnsignedTx()
	unsignedReq, tx, err := parseSOLUnsignedWithdrawTx(unsignedTx)
	if err != nil {
		return nil, err
	}
	if s.pubKey.String() != unsignedReq.From {
		return nil, errors.New("solana private key does not match from address")
	}
	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key.Equals(s.pubKey) {
			keyCopy := s.privKey
			return &keyCopy
		}
		return nil
	})
	if err != nil {
		return nil, errors.New("solana sign failed")
	}
	return tx.MarshalBinary()
}

func parseSOLUnsignedWithdrawTx(unsignedTx []byte) (solUnsignedWithdrawTx, *solana.Transaction, error) {
	var req solUnsignedWithdrawTx
	if err := json.Unmarshal(unsignedTx, &req); err != nil {
		return solUnsignedWithdrawTx{}, nil, errors.New("invalid solana unsigned tx")
	}
	from, err := parseSOLAddress(req.From)
	if err != nil {
		return solUnsignedWithdrawTx{}, nil, errors.New("invalid solana from address")
	}
	rawTx, err := base64.StdEncoding.DecodeString(strings.TrimSpace(req.TxBase64))
	if err != nil || len(rawTx) == 0 {
		return solUnsignedWithdrawTx{}, nil, errors.New("invalid solana tx payload")
	}
	tx, err := solana.TransactionFromBytes(rawTx)
	if err != nil {
		return solUnsignedWithdrawTx{}, nil, errors.New("invalid solana tx payload")
	}
	if len(tx.Message.AccountKeys) == 0 || !tx.Message.AccountKeys[0].Equals(from) {
		return solUnsignedWithdrawTx{}, nil, errors.New("invalid solana tx payer")
	}
	req.From = from.String()
	return req, tx, nil
}

func decodeSOLPrivateKey(rawPriv string) (solana.PrivateKey, error) {
	v := strings.TrimSpace(rawPriv)
	if v == "" {
		return nil, errors.New("empty SOL private key")
	}

	if strings.HasPrefix(v, "[") && strings.HasSuffix(v, "]") {
		var arr []byte
		if err := json.Unmarshal([]byte(v), &arr); err == nil {
			return normalizeSOLPrivateKeyBytes(arr)
		}
	}

	if b, err := hex.DecodeString(trim0x(v)); err == nil {
		if priv, err := normalizeSOLPrivateKeyBytes(b); err == nil {
			return priv, nil
		}
	}

	if priv, err := solana.PrivateKeyFromBase58(v); err == nil {
		return priv, nil
	}

	return nil, errors.New("invalid SOL private key encoding")
}

func normalizeSOLPrivateKeyBytes(raw []byte) (solana.PrivateKey, error) {
	switch len(raw) {
	case ed25519.SeedSize:
		return solana.PrivateKey(ed25519.NewKeyFromSeed(raw)), nil
	case ed25519.PrivateKeySize:
		if _, err := solana.ValidatePrivateKey(raw); err != nil {
			return nil, errors.New("invalid SOL private key")
		}
		return solana.PrivateKey(append([]byte(nil), raw...)), nil
	default:
		return nil, errors.New("invalid SOL private key length")
	}
}

func parseSOLAddress(addr string) (solana.PublicKey, error) {
	v := strings.TrimSpace(addr)
	if v == "" {
		return solana.PublicKey{}, errors.New("invalid solana address")
	}
	pk, err := solana.PublicKeyFromBase58(v)
	if err != nil {
		return solana.PublicKey{}, errors.New("invalid solana address")
	}
	return pk, nil
}
