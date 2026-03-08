package provider

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"

	authtoken "wallet-system/internal/auth/token"
)

type LocalProvider struct {
	secret []byte
}

func NewLocalProvider(secret []byte) (*LocalProvider, error) {
	if len(secret) == 0 {
		return nil, errors.New("auth secret is required")
	}
	return &LocalProvider{secret: append([]byte(nil), secret...)}, nil
}

func (p *LocalProvider) MakeToken(ctx context.Context, payload authtoken.Payload) (string, error) {
	_ = ctx
	mac := hmac.New(sha256.New, p.secret)
	mac.Write([]byte(authtoken.CanonicalMessage(payload)))
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func (p *LocalProvider) VerifyToken(ctx context.Context, payload authtoken.Payload, token string) (bool, error) {
	_ = ctx
	got, err := hex.DecodeString(strings.TrimSpace(token))
	if err != nil {
		return false, nil
	}
	mac := hmac.New(sha256.New, p.secret)
	mac.Write([]byte(authtoken.CanonicalMessage(payload)))
	return hmac.Equal(mac.Sum(nil), got), nil
}
