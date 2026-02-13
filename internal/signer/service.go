package signer

import (
	"context"
	"errors"
	"math/big"
	"time"

	pb "wallet-system/internal/signer/pb"

	"wallet-system/internal/signer/idempotency"
	"wallet-system/internal/signer/policy"
	"wallet-system/internal/signer/provider"
	"wallet-system/pkg/redisx"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gRPC server
type GRPCServer struct {
	pb.UnimplementedSignerServiceServer

	rdb      *redis.Client
	guard    *idempotency.Guard
	provider provider.SignerProvider
	policy   *policy.PolicyEngine
}

func NewGRPCServer(rdb *redis.Client, p provider.SignerProvider) *GRPCServer {
	return &GRPCServer{
		rdb:      rdb,
		guard:    idempotency.New(rdb, 30*time.Minute),
		provider: p,
		policy:   policy.NewPolicyEngine(),
	}
}

func nonceLockKey(chain, from string) string {
	return "lock:nonce:" + chain + ":" + from
}

func (s *GRPCServer) SignTransaction(ctx context.Context, req *pb.SignRequest) (*pb.SignResponse, error) {
	amount, err := validateSignRequest(req)
	if err != nil {
		return nil, err
	}

	resp, err := s.precheckIdempotency(ctx, req.GetRequestId())
	if resp != nil || err != nil {
		return resp, err
	}

	lock, err := s.acquireNonceLock(ctx, req.GetChain(), req.GetFromAddress())
	if err != nil {
		return nil, err
	}
	defer func() { _ = lock.Unlock(context.Background()) }()

	resp, err = s.beginIdempotency(ctx, req.GetRequestId())
	if resp != nil || err != nil {
		return resp, err
	}

	signed, err := s.signWithPolicy(req, amount)
	if err != nil {
		return nil, err
	}

	if err := s.finishIdempotency(ctx, req.GetRequestId(), signed); err != nil {
		return nil, err
	}

	return &pb.SignResponse{
		SignedTx: signed,
		// TxHash: TODO
	}, nil
}

func validateSignRequest(req *pb.SignRequest) (*big.Int, error) {
	if req.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id required")
	}
	if req.GetChain() == "" || req.GetFromAddress() == "" || req.GetToAddress() == "" {
		return nil, status.Error(codes.InvalidArgument, "chain/from/to required")
	}
	if len(req.GetUnsignedTx()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "unsigned_tx required")
	}

	amount := new(big.Int)
	if _, ok := amount.SetString(req.GetAmount(), 10); !ok {
		return nil, status.Error(codes.InvalidArgument, "invalid amount")
	}
	return amount, nil
}

func (s *GRPCServer) precheckIdempotency(ctx context.Context, requestID string) (*pb.SignResponse, error) {
	okToProceed, cached, err := s.guard.Check(ctx, requestID)
	if err != nil {
		if errors.Is(err, idempotency.ErrDuplicateProcessing) {
			return nil, status.Error(codes.Aborted, "request is processing; retry later")
		}
		return nil, status.Error(codes.Unavailable, "redis unavailable (guard check)")
	}
	if okToProceed {
		return nil, nil
	}
	return responseFromCached(cached)
}

func (s *GRPCServer) beginIdempotency(ctx context.Context, requestID string) (*pb.SignResponse, error) {
	okToProceed, cached, err := s.guard.Begin(ctx, requestID)
	if err != nil {
		if errors.Is(err, idempotency.ErrDuplicateProcessing) {
			return nil, status.Error(codes.Aborted, "request is processing; retry later")
		}
		return nil, status.Error(codes.Unavailable, "redis unavailable (guard begin)")
	}
	if okToProceed {
		return nil, nil
	}
	return responseFromCached(cached)
}

func responseFromCached(cached []byte) (*pb.SignResponse, error) {
	if cached != nil {
		return &pb.SignResponse{
			SignedTx: cached,
			// TxHash: TODO
		}, nil
	}
	return nil, status.Error(codes.Internal, "idempotency state inconsistent")
}

func (s *GRPCServer) acquireNonceLock(ctx context.Context, chain, from string) (*redisx.Lock, error) {
	lock := redisx.NewLock(s.rdb, nonceLockKey(chain, from), 10*time.Second)
	if err := lock.TryLock(ctx); err != nil {
		if errors.Is(err, redisx.ErrLockNotAcquired) {
			return nil, status.Error(codes.Aborted, "nonce lock busy; retry with backoff")
		}
		return nil, status.Error(codes.Unavailable, "redis unavailable (try lock)")
	}
	return lock, nil
}

func (s *GRPCServer) signWithPolicy(req *pb.SignRequest, amount *big.Int) ([]byte, error) {
	if err := s.policy.Validate(req.GetToAddress(), amount); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	signed, err := s.provider.Sign(req.GetUnsignedTx())
	if err != nil {
		return nil, status.Error(codes.Unavailable, "sign provider unavailable")
	}
	return signed, nil
}

func (s *GRPCServer) finishIdempotency(ctx context.Context, requestID string, signed []byte) error {
	if err := s.guard.Finish(ctx, requestID, signed); err != nil {
		return status.Error(codes.Unavailable, "redis unavailable (guard finish)")
	}
	return nil
}
