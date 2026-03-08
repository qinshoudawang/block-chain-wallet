package provider

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"

	authtoken "wallet-system/internal/auth/token"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	awskmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
)

type KMSProvider struct {
	keyID string
	kms   *awskms.Client
	algo  awskmstypes.MacAlgorithmSpec
}

func NewKMSProvider(ctx context.Context, keyID string) (*KMSProvider, error) {
	keyID = strings.TrimSpace(keyID)
	if keyID == "" {
		return nil, errors.New("auth kms key id is required")
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	return &KMSProvider{
		keyID: keyID,
		kms:   awskms.NewFromConfig(cfg),
		algo:  awskmstypes.MacAlgorithmSpecHmacSha256,
	}, nil
}

func (p *KMSProvider) MakeToken(ctx context.Context, payload authtoken.Payload) (string, error) {
	msg := []byte(authtoken.CanonicalMessage(payload))
	out, err := p.kms.GenerateMac(ctx, &awskms.GenerateMacInput{
		KeyId:        &p.keyID,
		Message:      msg,
		MacAlgorithm: p.algo,
	})
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(out.Mac), nil
}

func (p *KMSProvider) VerifyToken(ctx context.Context, payload authtoken.Payload, token string) (bool, error) {
	mac, err := hex.DecodeString(strings.TrimSpace(token))
	if err != nil {
		return false, err
	}
	msg := []byte(authtoken.CanonicalMessage(payload))
	out, err := p.kms.VerifyMac(ctx, &awskms.VerifyMacInput{
		KeyId:        &p.keyID,
		Message:      msg,
		Mac:          mac,
		MacAlgorithm: p.algo,
	})
	if err != nil {
		return false, err
	}
	return out.MacValid, nil
}
