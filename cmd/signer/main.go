package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"wallet-system/internal/config"
	"wallet-system/internal/helpers"
	"wallet-system/internal/infra/redisx"
	"wallet-system/internal/signer"
	"wallet-system/internal/signer/derivation"
	"wallet-system/internal/signer/policy"
	"wallet-system/internal/signer/provider"
	btcprovider "wallet-system/internal/signer/provider/btc"
	evmprovider "wallet-system/internal/signer/provider/evm"
	solprovider "wallet-system/internal/signer/provider/sol"
	"wallet-system/internal/storage/repo"
	signpb "wallet-system/proto/signer"

	"google.golang.org/grpc"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
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
	helpers.InitServiceLogger("signer")
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go handleShutdown(cancel)

	rdc := initRedis(ctx)
	defer rdc.RDB.Close()
	db := initGorm()
	profiles := buildChainProfiles()
	svc := initSignerService(rdc, profiles, repo.NewAddressRepo(db))
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

func buildChainProfiles() map[string]config.ChainProfile {
	profiles, err := config.LoadChainProfilesFromEnv()
	if err != nil {
		log.Fatalf("invalid chain profile config: %v", err)
	}
	return profiles
}

func initSignerService(rdc *redisx.Client, profiles map[string]config.ChainProfile, addressRepo *repo.AddressRepo) *signer.Service {
	authSecret := []byte(helpers.MustEnv("SIGNER_AUTH_SECRET"))
	if len(authSecret) == 0 {
		log.Fatal("SIGNER_AUTH_SECRET is required")
	}
	mnemonic := helpers.MustEnv("SIGNER_MNEMONIC")
	deriver, err := derivation.NewDeriver(mnemonic)
	if err != nil {
		log.Fatalf("init address deriver failed: %v", err)
	}
	registry := provider.NewRegistry()
	registerEVMSignerProviders(registry, profiles, deriver, addressRepo)
	registerBTCSignerProvider(registry, profiles)
	registerSOLSignerProvider(registry, profiles)

	return signer.NewService(rdc.RDB, registry, authSecret, mnemonic, policy.NewPolicyEngine(addressRepo))
}

func initGorm() *gorm.DB {
	dsn := "host=" + helpers.Getenv("DB_HOST", "127.0.0.1") +
		" port=" + helpers.Getenv("DB_PORT", "5432") +
		" user=" + helpers.MustEnv("DB_USER") +
		" password=" + helpers.MustEnv("DB_PASS") +
		" dbname=" + helpers.MustEnv("DB_NAME") +
		" sslmode=" + helpers.Getenv("DB_SSLMODE", "disable") +
		" TimeZone=" + helpers.Getenv("DB_TZ", "Asia/Shanghai")
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("init postgres failed: %v", err)
	}
	return db
}

func registerEVMSignerProviders(registry *provider.Registry, profiles map[string]config.ChainProfile, deriver *derivation.Deriver, addressRepo *repo.AddressRepo) {
	kmsKeyID := helpers.Getenv("ETH_HOT_WALLET_KMS_KEY_ID", "")
	priv := helpers.Getenv("ETH_HOT_WALLET_PRIV", "")
	for chain, p := range profiles {
		spec, err := helpers.ResolveChainSpec(chain)
		if err != nil {
			log.Fatalf("resolve chain spec failed chain=%s err=%v", chain, err)
		}
		if spec.Family != helpers.FamilyEVM {
			continue
		}
		if p.ChainID == nil {
			log.Fatalf("evm chain id is required for chain=%s", chain)
		}
		var evmSigner provider.SignerProvider
		if strings.TrimSpace(kmsKeyID) != "" {
			kmsSigner, kmsErr := evmprovider.NewKMSSigner(context.Background(), kmsKeyID, p.ChainID, deriver, addressRepo)
			if kmsErr != nil {
				err = kmsErr
			} else {
				log.Printf("[signer] evm kms signer ready chain=%s key_id=%s address=%s", chain, kmsSigner.KeyID(), kmsSigner.HotAddress())
				evmSigner = kmsSigner
			}
		} else {
			if strings.TrimSpace(priv) == "" {
				log.Fatalf("ETH_HOT_WALLET_PRIV or ETH_HOT_WALLET_KMS_KEY_ID is required for chain=%s", chain)
			}
			evmSigner, err = evmprovider.NewSigner(priv, p.ChainID, deriver, addressRepo)
		}
		if err != nil {
			log.Fatalf("init evm signer failed chain=%s err=%v", chain, err)
		}
		if err := registry.Register(chain, evmSigner); err != nil {
			log.Fatalf("register evm signer provider failed chain=%s err=%v", chain, err)
		}
	}
}

func registerBTCSignerProvider(registry *provider.Registry, profiles map[string]config.ChainProfile) {
	kmsKeyID := helpers.Getenv("BTC_HOT_WALLET_KMS_KEY_ID", "")
	priv := helpers.Getenv("BTC_HOT_WALLET_PRIV", "")
	if strings.TrimSpace(kmsKeyID) == "" && priv == "" {
		return
	}

	for chain := range profiles {
		spec, err := helpers.ResolveChainSpec(chain)
		if err != nil {
			log.Fatalf("resolve chain spec failed chain=%s err=%v", chain, err)
		}
		if spec.Family != helpers.FamilyBTC {
			continue
		}
		var btcSigner provider.SignerProvider
		if strings.TrimSpace(kmsKeyID) != "" {
			kmsSigner, kmsErr := btcprovider.NewKMSSigner(context.Background(), kmsKeyID, chain)
			if kmsErr != nil {
				err = kmsErr
			} else {
				log.Printf("[signer] btc kms signer ready chain=%s key_id=%s address=%s", chain, kmsSigner.KeyID(), kmsSigner.HotAddress())
				btcSigner = kmsSigner
			}
		} else {
			btcSigner, err = btcprovider.NewSigner(priv)
		}
		if err != nil {
			log.Fatalf("init btc signer failed chain=%s err=%v", chain, err)
		}
		if err := registry.Register(chain, btcSigner); err != nil {
			log.Fatalf("register btc signer provider failed chain=%s err=%v", chain, err)
		}
	}
}

func registerSOLSignerProvider(registry *provider.Registry, profiles map[string]config.ChainProfile) {
	priv := helpers.Getenv("SOL_HOT_WALLET_PRIV", "")
	if priv == "" {
		return
	}

	for chain := range profiles {
		spec, err := helpers.ResolveChainSpec(chain)
		if err != nil {
			log.Fatalf("resolve chain spec failed chain=%s err=%v", chain, err)
		}
		if spec.Family != helpers.FamilySOL {
			continue
		}
		solSigner, err := solprovider.NewSigner(priv)
		if err != nil {
			log.Fatalf("init sol signer failed chain=%s err=%v", chain, err)
		}
		if err := registry.Register(chain, solSigner); err != nil {
			log.Fatalf("register sol signer provider failed chain=%s err=%v", chain, err)
		}
	}
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
