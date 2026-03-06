package signer

import (
	"context"
	"errors"
	"log"
	"math/big"
	"time"

	auth "wallet-system/internal/auth"
	"wallet-system/internal/signer/derivation"
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
	providers  *provider.Registry
	authSecret []byte
	deriver    *derivation.Deriver
}

func NewService(rdb *redis.Client, providers *provider.Registry, authSecret []byte, mnemonic string, policyEngine *policy.PolicyEngine) *Service {
	if providers == nil {
		log.Fatal("signer provider registry is required")
		return nil
	}
	deriver, err := derivation.NewDeriver(mnemonic)
	if err != nil {
		log.Fatal("init address deriver failed")
		return nil
	}
	return &Service{
		rdb:        rdb,
		guard:      idempotency.New(rdb, 30*time.Minute),
		policy:     policyEngine,
		providers:  providers,
		authSecret: authSecret,
		deriver:    deriver,
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
	if err := s.policy.Validate(ctx, req.GetChain(), req.GetToAddress(), amt); err != nil {
		return nil, err
	}

	// 3) auth_token 验真（HMAC）
	if !auth.VerifyToken(s.authSecret, auth.TxPayload{
		WithdrawID: req.WithdrawId,
		RequestID:  req.RequestId,
		Chain:      req.Chain,
		From:       req.FromAddress,
		To:         req.ToAddress,
		Amount:     req.Amount,
		UnsignedTx: req.UnsignedTx,
	}, req.AuthToken) {
		return nil, ErrAuthFailed
	}

	// 4) 签名（Provider：Local / MockKMS / 后续可换 HSM/MPC）
	p, err := s.providers.Resolve(req.Chain)
	if err != nil {
		return nil, err
	}
	signed, err := p.Sign(ctx, req)
	if err != nil {
		return nil, err
	}

	// 5) 幂等结果落地（Redis）
	if err := s.guard.Finish(ctx, req.RequestId, signed); err != nil {
		return nil, err
	}

	return &signpb.SignResponse{SignedTx: signed}, nil
}

func (s *Service) DeriveAddress(ctx context.Context, chain string, index uint32) (string, error) {
	_ = ctx
	if s.deriver == nil {
		return "", errors.New("deriver not configured")
	}
	return s.deriver.DeriveAddress(chain, index)
}
