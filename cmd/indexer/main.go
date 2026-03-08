package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	btcchain "wallet-system/internal/chain/btc"
	evmchain "wallet-system/internal/chain/evm"
	btcindex "wallet-system/internal/chainindex/btc"
	evmindex "wallet-system/internal/chainindex/evm"
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
	repos := initRepos(db)
	btcCleanup := startBTCIndexer(ctx, repos)
	defer btcCleanup()
	evmCleanup := startEVMIndexer(ctx, repos)
	defer evmCleanup()
	<-ctx.Done()
}

type repos struct {
	chain    *repo.ChainRepo
	deposit  *repo.DepositRepo
	address  *repo.AddressRepo
	withdraw *repo.WithdrawRepo
	sweep    *repo.SweepRepo
	ledger   *repo.LedgerRepo
}

func initRepos(db *gorm.DB) repos {
	return repos{
		chain:    repo.NewChainRepo(db),
		deposit:  repo.NewDepositRepo(db),
		address:  repo.NewAddressRepo(db),
		withdraw: repo.NewWithdrawRepo(db),
		sweep:    repo.NewSweepRepo(db),
		ledger:   repo.NewLedgerRepo(db),
	}
}

func startEVMIndexer(ctx context.Context, repos repos) func() {
	prof, err := env.LoadEVMProfileFromEnv()
	if err != nil {
		log.Fatalf("invalid evm profile: %v", err)
	}
	client, err := evmchain.NewClient(prof.RPC)
	if err != nil {
		log.Fatalf("init evm client failed chain=%s err=%v", prof.Chain, err)
	}

	cfg := evmindex.EVMIndexerConfig{
		Chain:          prof.Chain,
		TokenContracts: parseCSVEnv("EVM_TOKEN_CONTRACTS"),
		Confirmations:  uint64(helpers.ParseIntEnv("DEPOSIT_EVM_CONFIRMATIONS", 6)),
		PollInterval:   time.Duration(helpers.ParseIntEnv("DEPOSIT_EVM_POLL_SEC", 8)) * time.Second,
		BatchBlocks:    uint64(helpers.ParseIntEnv("DEPOSIT_EVM_BATCH_BLOCKS", 200)),
		StartBlock:     uint64(helpers.ParseIntEnv("DEPOSIT_EVM_START_BLOCK", 0)),
	}

	log.Printf("[indexer] starting evm chain index chain=%s contracts=%d confirmations=%d", cfg.Chain, len(cfg.TokenContracts), cfg.Confirmations)
	go evmindex.NewEVMDepositProjector(cfg.Chain, cfg.Confirmations, repos.chain, repos.deposit, client, 3*time.Second).Run(ctx)
	go evmindex.NewEVMWithdrawProjector(cfg.Chain, cfg.Confirmations, repos.chain, repos.withdraw, repos.ledger, client, 3*time.Second).Run(ctx)
	go evmindex.NewEVMSweepProjector(cfg.Chain, cfg.Confirmations, repos.chain, repos.sweep, client, 3*time.Second).Run(ctx)
	go evmindex.NewEVMIndexer(repos.chain, repos.deposit, repos.address, client, cfg).Run(ctx)
	return func() { client.Close() }
}

func startBTCIndexer(ctx context.Context, repos repos) func() {
	prof, ok, err := env.LoadBTCProfileFromEnv()
	if err != nil {
		log.Fatalf("invalid btc profile: %v", err)
	}
	if !ok {
		return func() {}
	}
	client, err := btcchain.NewClient(btcchain.Config{
		Host:       prof.Host,
		DisableTLS: prof.DisableTLS,
		Params:     prof.Params,
	})
	if err != nil {
		log.Fatalf("init btc client failed chain=%s err=%v", prof.Chain, err)
	}

	cfg := btcindex.IndexerConfig{
		Chain:         prof.Chain,
		Confirmations: uint64(helpers.ParseIntEnv("DEPOSIT_BTC_CONFIRMATIONS", 2)),
		PollInterval:  time.Duration(helpers.ParseIntEnv("DEPOSIT_BTC_POLL_SEC", 15)) * time.Second,
		BatchBlocks:   uint64(helpers.ParseIntEnv("DEPOSIT_BTC_BATCH_BLOCKS", 20)),
		StartBlock:    uint64(helpers.ParseIntEnv("DEPOSIT_BTC_START_BLOCK", 0)),
	}

	log.Printf("[indexer] starting btc chain index chain=%s confirmations=%d", cfg.Chain, cfg.Confirmations)
	go btcindex.NewDepositProjector(cfg.Chain, cfg.Confirmations, repos.chain, repos.deposit, client, 3*time.Second).Run(ctx)
	go btcindex.NewIndexer(repos.chain, repos.deposit, repos.address, client, cfg).Run(ctx)
	return func() { client.Close() }
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
