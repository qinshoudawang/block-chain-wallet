package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	evmchain "wallet-system/internal/chain/evm"
	"wallet-system/internal/chainindex"
	"wallet-system/internal/config/env"
	"wallet-system/internal/helpers"
	storagemigrate "wallet-system/internal/storage/migrate"
	"wallet-system/internal/storage/repo"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func init() {
	helpers.InitServiceLogger("indexer")
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go handleShutdown(cancel)

	db := initGorm()
	evmProf, err := env.LoadEVMProfileFromEnv()
	if err != nil {
		log.Fatalf("invalid evm profile: %v", err)
	}
	evmCli, err := evmchain.NewClient(evmProf.RPC)
	if err != nil {
		log.Fatalf("init evm client failed chain=%s err=%v", evmProf.Chain, err)
	}
	defer evmCli.Close()

	cfg := chainindex.EVMIndexerConfig{
		Chain:          evmProf.Chain,
		TokenContracts: parseCSVEnv("EVM_TOKEN_CONTRACTS"),
		Confirmations:  uint64(helpers.ParseIntEnv("DEPOSIT_EVM_CONFIRMATIONS", 6)),
		PollInterval:   time.Duration(helpers.ParseIntEnv("DEPOSIT_EVM_POLL_SEC", 8)) * time.Second,
		BatchBlocks:    uint64(helpers.ParseIntEnv("DEPOSIT_EVM_BATCH_BLOCKS", 200)),
		StartBlock:     uint64(helpers.ParseIntEnv("DEPOSIT_EVM_START_BLOCK", 0)),
	}

	chainRepo := repo.NewChainRepo(db)
	depositRepo := repo.NewDepositRepo(db)
	addressRepo := repo.NewAddressRepo(db)
	withdrawRepo := repo.NewWithdrawRepo(db)
	sweepRepo := repo.NewSweepRepo(db)
	ledgerRepo := repo.NewLedgerRepo(db)

	log.Printf("[indexer] starting evm chain index chain=%s contracts=%d confirmations=%d", cfg.Chain, len(cfg.TokenContracts), cfg.Confirmations)
	go chainindex.NewEVMIndexer(chainRepo, addressRepo, withdrawRepo, sweepRepo, evmCli, cfg).Run(ctx)
	go chainindex.NewDepositProjector(cfg.Chain, chainRepo, depositRepo, addressRepo, 3*time.Second).Run(ctx)
	go chainindex.NewWithdrawProjector(cfg.Chain, chainRepo, withdrawRepo, ledgerRepo, 3*time.Second).Run(ctx)
	chainindex.NewSweepProjector(cfg.Chain, chainRepo, sweepRepo, 3*time.Second).Run(ctx)
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

func handleShutdown(cancel context.CancelFunc) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(ch)
	<-ch
	cancel()
}
