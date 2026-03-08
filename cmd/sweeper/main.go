package main

import (
	"context"
	"log"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	auth "wallet-system/internal/auth"
	evmchain "wallet-system/internal/chain/evm"
	"wallet-system/internal/clients"
	"wallet-system/internal/config/env"
	"wallet-system/internal/helpers"
	"wallet-system/internal/infra/kafka"
	"wallet-system/internal/infra/redisx"
	storagemigrate "wallet-system/internal/storage/migrate"
	"wallet-system/internal/storage/repo"
	"wallet-system/internal/sweep"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func init() {
	helpers.InitServiceLogger("sweeper")
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go handleShutdown(cancel)

	db := initGorm()
	rdc := initRedis(ctx)
	defer rdc.RDB.Close()
	evmProf, err := env.LoadEVMProfileFromEnv()
	if err != nil {
		log.Fatalf("invalid evm profile: %v", err)
	}
	cli, err := evmchain.NewClient(evmProf.RPC)
	if err != nil {
		log.Fatalf("init evm client failed: %v", err)
	}
	defer cli.Close()

	signerCli, err := clients.NewSignerClient(helpers.Getenv("SIGNER_GRPC_ADDR", "127.0.0.1:9001"))
	if err != nil {
		log.Fatalf("init signer client failed: %v", err)
	}
	defer signerCli.Close()
	withdrawCli, err := clients.NewWithdrawClient(helpers.Getenv("WITHDRAW_GRPC_ADDR", "127.0.0.1:9002"))
	if err != nil {
		log.Fatalf("init withdraw client failed: %v", err)
	}
	defer withdrawCli.Close()
	producer := initKafkaProducer()
	defer producer.Close()

	assets := loadSweepAssets()
	minNativeWei := parseBigIntEnv("SWEEP_EVM_MIN_NATIVE_WEI", "1000000000000000")
	minTokenAmount := parseBigIntEnv("SWEEP_EVM_MIN_TOKEN_ATOMIC", "1000000")
	gasReserveWei := parseBigIntEnv("SWEEP_EVM_GAS_RESERVE_WEI", "0")
	tokenTopUpWei := parseBigIntEnv("SWEEP_EVM_TOKEN_TOPUP_WEI", "0")

	baseCfg := sweep.EVMSweeperConfig{
		Chain:          evmProf.Chain,
		ChainID:        evmProf.ChainID,
		HotAddress:     evmProf.From.Hex(),
		MinNativeWei:   minNativeWei,
		MinTokenAmount: minTokenAmount,
		GasReserveWei:  gasReserveWei,
		TokenTopUpWei:  tokenTopUpWei,
		PollInterval:   time.Duration(helpers.ParseIntEnv("SWEEP_EVM_POLL_SEC", 30)) * time.Second,
		LockTTL:        time.Duration(helpers.ParseIntEnv("SWEEP_EVM_LOCK_TTL_SEC", 90)) * time.Second,
		CandidateLimit: helpers.ParseIntEnv("SWEEP_EVM_BATCH", 200),
	}
	log.Printf("[sweeper] start chain=%s hot=%s assets=%d min_native=%s min_token=%s gas_reserve=%s token_topup=%s lock_ttl=%s",
		baseCfg.Chain, baseCfg.HotAddress, len(assets), baseCfg.MinNativeWei.String(), baseCfg.MinTokenAmount.String(),
		baseCfg.GasReserveWei.String(), baseCfg.TokenTopUpWei.String(), baseCfg.LockTTL.String())

	authProvider := buildSignerAuthProvider()
	sweepRepo := repo.NewSweepRepo(db)
	chainRepo := repo.NewChainRepo(db)
	go sweep.NewEVMTracker(
		evmProf.Chain,
		uint64(helpers.ParseIntEnv("DEPOSIT_EVM_CONFIRMATIONS", 6)),
		chainRepo,
		sweepRepo,
		cli,
		time.Duration(helpers.ParseIntEnv("BROADCAST_CONFIRMER_POLL_SEC", 60))*time.Second,
	).Run(ctx)
	for _, asset := range assets {
		cfg := baseCfg
		cfg.TokenContract = asset
		log.Printf("[sweeper] worker chain=%s token=%s", cfg.Chain, cfg.TokenContract)
		go sweep.NewEVMSweeper(cfg, cli, signerCli, authProvider, producer, rdc.RDB, sweepRepo, withdrawCli).Run(ctx)
	}
	<-ctx.Done()
}

func buildSignerAuthProvider() auth.Provider {
	if keyID := strings.TrimSpace(helpers.Getenv("SIGNER_AUTH_KMS_KEY_ID", "")); keyID != "" {
		p, err := auth.NewKMSProvider(context.Background(), keyID)
		if err != nil {
			log.Fatalf("init signer auth kms provider failed: %v", err)
		}
		return p
	}
	secret := []byte(helpers.Getenv("SWEEP_AUTH_SECRET", helpers.MustEnv("SIGNER_AUTH_SECRET")))
	p, err := auth.NewLocalProvider(secret)
	if err != nil {
		log.Fatalf("init signer auth provider failed: %v", err)
	}
	return p
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

func handleShutdown(cancel context.CancelFunc) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(ch)
	<-ch
	cancel()
}

func parseBigIntEnv(key string, def string) *big.Int {
	raw := strings.TrimSpace(helpers.Getenv(key, def))
	v, ok := new(big.Int).SetString(raw, 10)
	if !ok || v.Sign() < 0 {
		log.Fatalf("invalid %s: %s", key, raw)
	}
	return v
}

func initKafkaProducer() *kafka.Producer {
	brokers := strings.Split(helpers.Getenv("KAFKA_BROKERS", "127.0.0.1:9092"), ",")
	topic := helpers.Getenv("KAFKA_TOPIC_BROADCAST", "tx.broadcast.v1")
	return kafka.NewProducer(brokers, topic)
}

func loadSweepAssets() []string {
	contracts := parseCSVEnv("EVM_TOKEN_CONTRACTS")
	if len(contracts) == 0 {
		return []string{""}
	}
	seen := make(map[string]struct{}, len(contracts))
	out := make([]string, 0, len(contracts))
	for _, c := range contracts {
		key := strings.ToLower(strings.TrimSpace(c))
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, c)
	}
	if len(out) == 0 {
		return []string{""}
	}
	return out
}

func parseCSVEnv(key string) []string {
	raw := strings.TrimSpace(helpers.Getenv(key, ""))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		v := strings.TrimSpace(p)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}
