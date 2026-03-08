package auth

import (
	"context"

	authprovider "wallet-system/internal/auth/provider"
	authtoken "wallet-system/internal/auth/token"
)

type Provider = authprovider.Provider
type TxPayload = authtoken.Payload

func NewLocalProvider(secret []byte) (Provider, error) {
	return authprovider.NewLocalProvider(secret)
}

func NewKMSProvider(ctx context.Context, keyID string) (Provider, error) {
	return authprovider.NewKMSProvider(ctx, keyID)
}

func MakeTokenWithProvider(ctx context.Context, provider Provider, p TxPayload) (string, error) {
	return provider.MakeToken(ctx, p)
}

func VerifyTokenWithProvider(ctx context.Context, provider Provider, p TxPayload, token string) (bool, error) {
	return provider.VerifyToken(ctx, p, token)
}
