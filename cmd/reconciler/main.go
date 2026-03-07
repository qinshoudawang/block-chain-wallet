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

	evmchain "wallet-system/internal/chain/evm"
	"wallet-system/internal/config/env"
	"wallet-system/internal/helpers"
	"wallet-system/internal/infra/redisx"
	"wallet-system/internal/reconcile"
	storagemigrate "wallet-system/internal/storage/migrate"
	"wallet-system/internal/storage/repo"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func init() {
	helpers.InitServiceLogger("reconciler")
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
	evmCli, err := evmchain.NewClient(evmProf.RPC)
	if err != nil {
		log.Fatalf("init evm client failed: %v", err)
	}
	defer evmCli.Close()

	tokenContracts := parseCSVEnv("EVM_TOKEN_CONTRACTS")
	tolerance := parseBigIntEnv("RECON_EVM_TOLERANCE_WEI", "0")
	cfg := reconcile.EVMReconcilerConfig{
		Chain:          evmProf.Chain,
		HotAddress:     evmProf.From.Hex(),
		TokenContracts: tokenContracts,
		HotPoll:        time.Duration(helpers.ParseIntEnv("RECON_EVM_HOT_POLL_SEC", 30)) * time.Second,
		UserPoll:       time.Duration(helpers.ParseIntEnv("RECON_EVM_USER_POLL_SEC", 300)) * time.Second,
		HotLockTTL:     time.Duration(helpers.ParseIntEnv("RECON_EVM_HOT_LOCK_TTL_SEC", 60)) * time.Second,
		UserLockTTL:    time.Duration(helpers.ParseIntEnv("RECON_EVM_USER_LOCK_TTL_SEC", 180)) * time.Second,
		Tolerance:      tolerance,
	}
	log.Printf("[reconciler] start chain=%s hot=%s assets=%d hot_poll=%s user_poll=%s hot_lock_ttl=%s user_lock_ttl=%s tolerance=%s",
		cfg.Chain, cfg.HotAddress, len(cfg.TokenContracts)+1, cfg.HotPoll.String(), cfg.UserPoll.String(),
		cfg.HotLockTTL.String(), cfg.UserLockTTL.String(), cfg.Tolerance.String())

	r := reconcile.NewEVMReconciler(
		cfg,
		evmCli,
		rdc.RDB,
		repo.NewAddressRepo(db),
		repo.NewLedgerRepo(db),
		repo.NewReconcileRepo(db),
	)
	flow := reconcile.NewFlowReconciler(
		reconcile.FlowReconcilerConfig{
			Chain:        evmProf.Chain,
			PollInterval: time.Duration(helpers.ParseIntEnv("RECON_FLOW_POLL_SEC", 60)) * time.Second,
			LockTTL:      time.Duration(helpers.ParseIntEnv("RECON_FLOW_LOCK_TTL_SEC", 60)) * time.Second,
			BatchSize:    helpers.ParseIntEnv("RECON_FLOW_BATCH", 500),
		},
		rdc.RDB,
		repo.NewDepositRepo(db),
		repo.NewWithdrawRepo(db),
		repo.NewSweepRepo(db),
		repo.NewLedgerRepo(db),
		repo.NewUserLedgerRepo(db),
		repo.NewReconcileRepo(db),
	)

	go r.Run(ctx)
	go flow.Run(ctx)
	<-ctx.Done()
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

func parseBigIntEnv(key string, def string) *big.Int {
	raw := strings.TrimSpace(helpers.Getenv(key, def))
	v, ok := new(big.Int).SetString(raw, 10)
	if !ok || v.Sign() < 0 {
		log.Fatalf("invalid %s: %s", key, raw)
	}
	return v
}

func handleShutdown(cancel context.CancelFunc) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(ch)
	<-ch
	cancel()
}
