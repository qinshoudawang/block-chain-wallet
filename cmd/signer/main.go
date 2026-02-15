package main

import (
	"context"
	"log"
	"net"
	"time"

	"wallet-system/internal/helpers"
	"wallet-system/internal/infra/redisx"
	"wallet-system/internal/signer"
	"wallet-system/internal/signer/provider"
	signpb "wallet-system/proto/signer"

	"google.golang.org/grpc"
)

type grpcServer struct {
	signpb.UnimplementedSignerServiceServer
	svc *signer.Service
}

func (s *grpcServer) SignTransaction(ctx context.Context, req *signpb.SignRequest) (*signpb.SignResponse, error) {
	// 适配 proto -> internal req
	ir := &signpb.SignRequest{
		RequestId:   req.GetRequestId(),
		WithdrawId:  req.GetWithdrawId(),
		Chain:       req.GetChain(),
		FromAddress: req.GetFromAddress(),
		ToAddress:   req.GetToAddress(),
		Amount:      req.GetAmount(),
		UnsignedTx:  req.GetUnsignedTx(),
		AuthToken:   req.GetAuthToken(),
		Caller:      req.GetCaller(),
	}

	resp, err := s.svc.SignTransaction(ctx, ir)
	if err != nil {
		return nil, err
	}

	return &signpb.SignResponse{
		SignedTx: resp.SignedTx,
		TxHash:   resp.TxHash,
	}, nil
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	addr := helpers.Getenv("SIGNER_GRPC_ADDR", "127.0.0.1:9001")
	authSecret := []byte(helpers.MustEnv("SIGNER_AUTH_SECRET"))
	if len(authSecret) == 0 {
		log.Fatal("SIGNER_AUTH_SECRET is required")
	}

	// Redis
	redisAddr := helpers.MustEnv("REDIS_ADDR")
	redisPass := helpers.MustEnv("REDIS_PASS")
	redisDB := helpers.MustEnv("REDIS_DB")

	rdc := redisx.New(redisAddr, redisPass, redisDB)
	if err := rdc.Ping(ctx); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}

	// Provider: EVM local signer
	chainID := helpers.MustBig(helpers.MustEnv("ETH_CHAIN_ID"))
	evmSigner, err := provider.NewEVMLocalSigner(helpers.MustEnv("HOT_WALLET_PRIV"), chainID)
	if err != nil {
		log.Fatalf("init evm local signer failed: %v", err)
	}

	svc := signer.NewService(rdc.RDB, evmSigner, authSecret)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}

	gs := grpc.NewServer(
		grpc.ConnectionTimeout(3 * time.Second),
	)

	signpb.RegisterSignerServiceServer(gs, &grpcServer{svc: svc})

	log.Printf("[signer] grpc listening on %s", addr)
	if err := gs.Serve(lis); err != nil {
		log.Fatalf("grpc serve failed: %v", err)
	}
}
