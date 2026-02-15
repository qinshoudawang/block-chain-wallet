package signer

import (
	"context"
	"errors"
	"log"
	"math/big"
	"time"

	"wallet-system/internal/auth/hmacauth"
	"wallet-system/internal/signer/idempotency"
	"wallet-system/internal/signer/policy"
	"wallet-system/internal/signer/provider"
	signpb "wallet-system/proto/signer"

	"github.com/redis/go-redis/v9"
)

var (
	ErrAuthFailed = errors.New("auth_token verification failed")
)

type Service struct {
	rdb        *redis.Client
	guard      *idempotency.Guard
	policy     *policy.PolicyEngine
	provider   provider.SignerProvider
	authSecret []byte
}

func NewService(rdb *redis.Client, p provider.SignerProvider, authSecret []byte) *Service {
	return &Service{
		rdb:        rdb,
		guard:      idempotency.New(rdb, 30*time.Minute),
		policy:     policy.NewPolicyEngine(),
		provider:   p,
		authSecret: authSecret,
	}
}

func (s *Service) SignTransaction(ctx context.Context, req *signpb.SignRequest) (*signpb.SignResponse, error) {
	log.Printf("[signer] handle request %s", req.RequestId)
	// 1) 幂等/防重放（Redis）
	ok, cached, err := s.guard.Begin(ctx, req.RequestId)
	if err != nil && !errors.Is(err, idempotency.ErrDuplicateProcessing) {
		return nil, err // fail-closed
	}
	if !ok {
		if cached != nil {
			return &signpb.SignResponse{SignedTx: cached}, nil
		}
		return nil, err
	}

	// 2) policy（白名单/限额）
	amt := new(big.Int)
	if _, ok := amt.SetString(req.Amount, 10); !ok {
		return nil, errors.New("invalid amount")
	}
	if err := s.policy.Validate(req.ToAddress, amt); err != nil {
		return nil, err
	}

	// 3) auth_token 验真（HMAC）
	txHash := hmacauth.SHA256Hex(req.UnsignedTx)
	msg := hmacauth.CanonicalMessage(hmacauth.Payload{
		WithdrawID:     req.WithdrawId,
		RequestID:      req.RequestId,
		Chain:          req.Chain,
		From:           req.FromAddress,
		To:             req.ToAddress,
		Amount:         req.Amount,
		UnsignedTxHash: txHash,
	})
	if !hmacauth.VerifyHex(s.authSecret, msg, req.AuthToken) {
		return nil, ErrAuthFailed
	}

	// 4) 签名（Provider：Local / MockKMS / 后续可换 HSM/MPC）
	signed, err := s.provider.Sign(req.UnsignedTx)
	if err != nil {
		return nil, err
	}

	// 5) 幂等结果落地（Redis）
	if err := s.guard.Finish(ctx, req.RequestId, signed); err != nil {
		return nil, err
	}

	return &signpb.SignResponse{SignedTx: signed}, nil
}
