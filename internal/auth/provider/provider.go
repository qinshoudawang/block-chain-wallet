package provider

import (
	"context"

	authtoken "wallet-system/internal/auth/token"
)

type Provider interface {
	MakeToken(ctx context.Context, p authtoken.Payload) (string, error)
	VerifyToken(ctx context.Context, p authtoken.Payload, token string) (bool, error)
}
