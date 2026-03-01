package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"wallet-system/internal/address"
	"wallet-system/internal/api"
	btcchain "wallet-system/internal/chain/btc"
	evmchain "wallet-system/internal/chain/evm"
	"wallet-system/internal/clients"
	"wallet-system/internal/config"
	"wallet-system/internal/config/env"
	"wallet-system/internal/helpers"
	"wallet-system/internal/infra/kafka"
	"wallet-system/internal/infra/redisx"
	"wallet-system/internal/risk"
	storagemigrate "wallet-system/internal/storage/migrate"
	"wallet-system/internal/storage/repo"
	"wallet-system/internal/withdraw"
	"wallet-system/internal/withdraw/chainclient"
	signpb "wallet-system/proto/signer"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go handleShutdown(cancel)

	rdc := initRedis(ctx)
	defer rdc.RDB.Close()
	db := initGorm()
	producer := initKafkaProducer()
	defer producer.Close()
	chainProfiles := buildChainProfiles()
	signerCli := initSignerClient()
	defer signerCli.Close()
	wsvc, closeChainClients := initWithdrawService(rdc, chainProfiles, producer, db, signerCli.Client)
	defer closeChainClients()
	asvc := address.NewAddressService(db, signerCli.Client)
	server := initHTTPServer(wsvc, asvc)

	if err := runHTTPServer(ctx, server); err != nil {
		log.Fatal(err)
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

func initKafkaProducer() *kafka.Producer {
	brokers := strings.Split(helpers.Getenv("KAFKA_BROKERS", "127.0.0.1:9092"), ",")
	topic := helpers.Getenv("KAFKA_TOPIC_BROADCAST", "tx.broadcast.v1")
	return kafka.NewProducer(brokers, topic)
}

func initEVMClient() *evmchain.Client {
	evmProf, err := env.LoadEVMProfileFromEnv()
	if err != nil {
		log.Fatalf("invalid evm network config: %v", err)
	}
	eth, err := evmchain.NewClient(evmProf.RPC)
	if err != nil {
		log.Fatalf("init eth client failed chain=%s rpc=%s err=%v", evmProf.Chain, evmProf.RPC, err)
	}
	return eth
}

func initBTCClient() *btcchain.Client {
	btcProf, ok, err := env.LoadBTCProfileFromEnv()
	if err != nil {
		log.Fatalf("invalid btc network config chain=%s err=%v", btcProf.Chain, err)
	}
	if !ok {
		log.Fatalf("btc profile is not configured for chain=%s", btcProf.Chain)
	}
	cli, err := btcchain.NewClient(btcchain.Config{
		Host:       btcProf.Host,
		User:       btcProf.User,
		Pass:       btcProf.Pass,
		DisableTLS: btcProf.DisableTLS,
		Params:     btcProf.Params,
	})
	if err != nil {
		log.Fatalf("init btc rpc client failed chain=%s err=%v", btcProf.Chain, err)
	}
	return cli
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
	if err := storagemigrate.All(db); err != nil {
		log.Fatalf("migrate storage tables failed: %v", err)
	}
	return db
}

func initSignerClient() *clients.SignerClient {
	signerAddr := helpers.Getenv("SIGNER_GRPC_ADDR", "127.0.0.1:9001")
	signerCli, err := clients.NewSignerClient(signerAddr)
	if err != nil {
		log.Fatalf("init signer client failed: %v", err)
	}
	return signerCli
}

func initWithdrawService(
	rdc *redisx.Client,
	chainProfiles map[string]config.ChainProfile,
	producer *kafka.Producer,
	db *gorm.DB,
	signerCli signpb.SignerServiceClient,
) (*withdraw.Service, func()) {
	chainRegistry, closeChainClients := buildWithdrawChainClientRegistry(chainProfiles)
	return withdraw.NewService(
		chainProfiles,
		[]byte(helpers.MustEnv("WITHDRAW_AUTH_SECRET")),
		withdraw.Deps{
			Redis:       rdc.RDB,
			ChainClient: chainRegistry,
			Signer:      signerCli,
			Ledger:      repo.NewLedgerRepo(db),
			Withdraw:    repo.NewWithdrawRepo(db),
			Risk:        risk.NewNoopApprover(),
		},
		producer,
	), closeChainClients
}

func buildChainProfiles() map[string]config.ChainProfile {
	profiles, err := config.LoadChainProfilesFromEnv()
	if err != nil {
		log.Fatalf("invalid withdraw profile config: %v", err)
	}
	return profiles
}

func buildWithdrawChainClientRegistry(profiles map[string]config.ChainProfile) (*chainclient.Registry, func()) {
	registry := chainclient.NewRegistry()
	var evmClient *evmchain.Client
	var btcClient *btcchain.Client
	for chain := range profiles {
		spec, err := helpers.ResolveChainSpec(chain)
		if err != nil {
			log.Fatalf("resolve chain spec failed chain=%s err=%v", chain, err)
		}
		if spec.Family == "evm" {
			if evmClient == nil {
				evmClient = initEVMClient()
			}
			registry.RegisterEVM(chainclient.EVMRegistration{Client: evmClient})
		}
		if spec.Family == "btc" {
			if btcClient == nil {
				btcClient = initBTCClient()
			}
			registry.RegisterBTC(chainclient.BTCRegistration{
				Client: btcClient,
			})
		}
	}
	return registry, func() {
		if evmClient != nil {
			evmClient.Close()
		}
		if btcClient != nil {
			btcClient.Close()
		}
	}
}

func initHTTPServer(wsvc *withdraw.Service, asvc *address.AddressService) *http.Server {
	addr := helpers.Getenv("API_ADDR", "127.0.0.1:8080")
	return &http.Server{
		Addr:    addr,
		Handler: api.NewRouter(wsvc, asvc),
	}
}

func runHTTPServer(ctx context.Context, server *http.Server) error {
	errCh := make(chan error, 1)

	go func() {
		log.Printf("[withdraw-api] listening on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		return <-errCh
	case err := <-errCh:
		return err
	}
}
