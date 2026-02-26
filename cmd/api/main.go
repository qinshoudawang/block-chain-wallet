package main

import (
	"context"
	"errors"
	"log"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"wallet-system/internal/address"
	"wallet-system/internal/api"
	"wallet-system/internal/clients"
	"wallet-system/internal/helpers"
	"wallet-system/internal/infra/kafka"
	"wallet-system/internal/infra/redisx"
	"wallet-system/internal/risk"
	storagemigrate "wallet-system/internal/storage/migrate"
	"wallet-system/internal/storage/repo"
	"wallet-system/internal/withdraw"
	"wallet-system/internal/withdraw/chainclient"
	signpb "wallet-system/proto/signer"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
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
	eth := initEthClient()
	defer eth.Close()
	signerCli := initSignerClient()
	wsvc := initWithdrawService(rdc, eth, producer, db, signerCli)
	asvc := address.NewAddressService(db, signerCli)
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

func initEthClient() *ethclient.Client {
	eth, err := ethclient.Dial(helpers.MustEnv("ETH_RPC"))
	if err != nil {
		log.Fatalf("init eth client failed: %v", err)
	}
	return eth
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

func initSignerClient() signpb.SignerServiceClient {
	signerAddr := helpers.Getenv("SIGNER_GRPC_ADDR", "127.0.0.1:9001")
	signerCli, err := clients.NewSignerClient(signerAddr)
	if err != nil {
		log.Fatalf("init signer client failed: %v", err)
	}
	return signerCli
}

func initWithdrawService(rdc *redisx.Client, eth *ethclient.Client, producer *kafka.Producer, db *gorm.DB, signerCli signpb.SignerServiceClient) *withdraw.Service {
	chainIDStr := helpers.MustEnv("ETH_CHAIN_ID")
	chainID, ok := new(big.Int).SetString(chainIDStr, 10)
	if !ok {
		log.Fatalf("invalid ETH_CHAIN_ID: %s", chainIDStr)
	}
	gasReserveWeiStr := helpers.Getenv("WITHDRAW_GAS_RESERVE_WEI", "0")
	gasReserveWei, ok := new(big.Int).SetString(gasReserveWeiStr, 10)
	if !ok || gasReserveWei.Sign() < 0 {
		log.Fatalf("invalid WITHDRAW_GAS_RESERVE_WEI: %s", gasReserveWeiStr)
	}
	chain := helpers.MustEnv("ETH_CHAIN")

	return withdraw.NewService(
		map[string]withdraw.ChainProfile{
			chain: {
				From:          common.HexToAddress(helpers.MustEnv("FROM_ADDRESS")),
				ChainID:       chainID,
				FreezeReserve: gasReserveWei,
			},
		},
		[]byte(helpers.MustEnv("WITHDRAW_AUTH_SECRET")),
		withdraw.Deps{
			Redis:       rdc.RDB,
			ChainClient: buildWithdrawChainClientRegistry(eth),
			Signer:      signerCli,
			Ledger:      repo.NewLedgerRepo(db),
			Withdraw:    repo.NewWithdrawRepo(db),
			Risk:        risk.NewNoopApprover(),
		},
		producer,
	)
}

func buildWithdrawChainClientRegistry(eth *ethclient.Client) *chainclient.Registry {
	registry := chainclient.NewRegistry()
	registry.RegisterEVM(eth)
	return registry
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
