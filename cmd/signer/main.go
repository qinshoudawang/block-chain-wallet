package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
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
	resp, err := s.svc.SignTransaction(ctx, req)
	if err != nil {
		return nil, err
	}
	return &signpb.SignResponse{
		SignedTx: resp.SignedTx,
		TxHash:   resp.TxHash,
	}, nil
}

func (s *grpcServer) DeriveAddress(ctx context.Context, req *signpb.DeriveAddressRequest) (*signpb.DeriveAddressResponse, error) {
	addr, err := s.svc.DeriveAddress(ctx, req.GetChain(), req.GetIndex())
	if err != nil {
		return nil, err
	}
	return &signpb.DeriveAddressResponse{Address: addr}, nil
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go handleShutdown(cancel)

	rdc := initRedis(ctx)
	defer rdc.RDB.Close()
	svc := initSignerService(rdc)
	lis := initListener()
	defer lis.Close()
	gs := initGRPCServer(svc)

	if err := runGRPCServer(ctx, gs, lis); err != nil {
		log.Fatalf("grpc serve failed: %v", err)
	}
}

func handleShutdown(cancel context.CancelFunc) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(ch)
	<-ch
	cancel()
}

func initRedis(ctx context.Context) *redisx.Client {
	rdc := redisx.New(
		helpers.MustEnv("REDIS_ADDR"),
		helpers.MustEnv("REDIS_PASS"),
		helpers.MustEnv("REDIS_DB"),
	)
	pingCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := rdc.Ping(pingCtx); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}
	return rdc
}

func initSignerService(rdc *redisx.Client) *signer.Service {
	authSecret := []byte(helpers.MustEnv("SIGNER_AUTH_SECRET"))
	if len(authSecret) == 0 {
		log.Fatal("SIGNER_AUTH_SECRET is required")
	}
	mnemonic := helpers.MustEnv("SIGNER_MNEMONIC")

	chainID := helpers.MustBig(helpers.MustEnv("ETH_CHAIN_ID"))
	evmSigner, err := provider.NewEVMLocalSigner(helpers.MustEnv("HOT_WALLET_PRIV"), chainID)
	if err != nil {
		log.Fatalf("init evm local signer failed: %v", err)
	}

	return signer.NewService(rdc.RDB, evmSigner, authSecret, mnemonic)
}

func initListener() net.Listener {
	addr := helpers.Getenv("SIGNER_GRPC_ADDR", "127.0.0.1:9001")
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}
	return lis
}

func initGRPCServer(svc *signer.Service) *grpc.Server {
	gs := grpc.NewServer(
		grpc.ConnectionTimeout(3 * time.Second),
	)
	signpb.RegisterSignerServiceServer(gs, &grpcServer{svc: svc})
	return gs
}

func runGRPCServer(ctx context.Context, gs *grpc.Server, lis net.Listener) error {
	errCh := make(chan error, 1)

	go func() {
		log.Printf("[signer] grpc listening on %s", lis.Addr().String())
		errCh <- gs.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		done := make(chan struct{})
		go func() {
			gs.GracefulStop()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			gs.Stop()
			<-done
		}
		err := <-errCh
		if err != nil && !errors.Is(err, net.ErrClosed) {
			return err
		}
		return nil
	case err := <-errCh:
		if err != nil && !errors.Is(err, net.ErrClosed) {
			return err
		}
		return nil
	}
}
