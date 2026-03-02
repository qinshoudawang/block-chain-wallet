package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"wallet-system/internal/broadcaster"
	broadcasterchain "wallet-system/internal/broadcaster/chainclient"
	btcchain "wallet-system/internal/chain/btc"
	"wallet-system/internal/chain/evm"
	solchain "wallet-system/internal/chain/sol"
	"wallet-system/internal/config"
	"wallet-system/internal/config/env"
	"wallet-system/internal/helpers"
	"wallet-system/internal/infra/kafka"
	"wallet-system/internal/infra/redisx"
	"wallet-system/internal/sequence/utxoreserve"
	storagemigrate "wallet-system/internal/storage/migrate"
	"wallet-system/internal/storage/repo"

	kafkago "github.com/segmentio/kafka-go"
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

	db := initGorm()
	rdc := initRedis(ctx)
	defer rdc.RDB.Close()
	utxoReserveTTL := time.Duration(helpers.ParseIntEnv("BTC_UTXO_RESERVE_TTL_SEC", 7200)) * time.Second
	utxoReserveRepo := repo.NewUTXOReservationRepo(db)
	utxoReserve := utxoreserve.NewManager(rdc.RDB, utxoReserveRepo, utxoReserveTTL)
	withdrawRepo := repo.NewWithdrawRepo(db)
	ledgerRepo := repo.NewLedgerRepo(db)
	producer := initKafkaProducer()
	defer producer.Close()
	consumer := initKafkaConsumer()
	defer consumer.Close()
	chainProfiles := buildChainProfiles()
	clients, closeChainClients := buildBroadcasterChainClientRegistry(chainProfiles)
	defer closeChainClients()

	go broadcaster.RunConsumer(ctx, withdrawRepo, clients, utxoReserve, consumer)
	go broadcaster.RunReplayer(ctx, withdrawRepo, producer)
	go broadcaster.RunConfirmer(ctx, withdrawRepo, ledgerRepo, clients, utxoReserve)

	<-ctx.Done()
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

func initGorm() *gorm.DB {
	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s TimeZone=%s",
		helpers.Getenv("DB_HOST", "127.0.0.1"),
		helpers.Getenv("DB_PORT", "5432"),
		helpers.MustEnv("DB_USER"),
		helpers.MustEnv("DB_PASS"),
		helpers.MustEnv("DB_NAME"),
		helpers.Getenv("DB_SSLMODE", "disable"),
		helpers.Getenv("DB_TZ", "Asia/Shanghai"),
	)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("init postgres failed: %v", err)
	}
	if err := storagemigrate.All(db); err != nil {
		log.Fatalf("migrate storage tables failed: %v", err)
	}
	return db
}

func initEVMClient() *evm.Client {
	evmProf, err := env.LoadEVMProfileFromEnv()
	if err != nil {
		log.Fatalf("invalid evm network config: %v", err)
	}
	client, err := evm.NewClient(evmProf.RPC)
	if err != nil {
		log.Fatalf("init evm client failed: %v", err)
	}
	return client
}

func initBTCClient() *btcchain.Client {
	prof, ok, err := env.LoadBTCProfileFromEnv()
	if err != nil {
		log.Fatalf("invalid btc network config chain=%s err=%v", prof.Chain, err)
	}
	if !ok {
		log.Fatalf("btc profile is not configured for chain=%s", prof.Chain)
	}
	cli, err := btcchain.NewClient(btcchain.Config{
		Host:       prof.Host,
		DisableTLS: prof.DisableTLS,
		Params:     prof.Params,
	})
	if err != nil {
		log.Fatalf("init btc rpc client failed chain=%s err=%v", prof.Chain, err)
	}
	return cli
}

func initSOLClient() *solchain.Client {
	prof, ok, err := env.LoadSOLProfileFromEnv()
	if err != nil {
		log.Fatalf("invalid sol network config chain=%s err=%v", prof.Chain, err)
	}
	if !ok {
		log.Fatalf("sol profile is not configured for chain=%s", prof.Chain)
	}
	return solchain.NewClient(prof.RPC)
}

func buildChainProfiles() map[string]config.ChainProfile {
	profiles, err := config.LoadChainProfilesFromEnv()
	if err != nil {
		log.Fatalf("invalid withdraw profile config: %v", err)
	}
	return profiles
}

func buildBroadcasterChainClientRegistry(profiles map[string]config.ChainProfile) (*broadcasterchain.Registry, func()) {
	registry := broadcasterchain.NewRegistry()
	var evmClient *evm.Client
	var btcClient *btcchain.Client
	var solClient *solchain.Client

	for chain := range profiles {
		spec, err := helpers.ResolveChainSpec(chain)
		if err != nil {
			log.Fatalf("resolve chain spec failed chain=%s err=%v", chain, err)
		}
		if spec.Family == helpers.FamilyEVM {
			if evmClient == nil {
				evmClient = initEVMClient()
			}
			if err := registry.Register(chain, broadcasterchain.NewEVMClient(evmClient)); err != nil {
				log.Fatalf("register broadcaster chain client failed: %v", err)
			}
		}
		if spec.Family == helpers.FamilyBTC {
			if btcClient == nil {
				btcClient = initBTCClient()
			}
			if err := registry.Register(chain, broadcasterchain.NewBTCClient(btcClient)); err != nil {
				log.Fatalf("register broadcaster chain client failed: %v", err)
			}
		}
		if spec.Family == helpers.FamilySOL {
			if solClient == nil {
				solClient = initSOLClient()
			}
			if err := registry.Register(chain, broadcasterchain.NewSOLClient(solClient)); err != nil {
				log.Fatalf("register broadcaster chain client failed: %v", err)
			}
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

func initKafkaProducer() *kafka.Producer {
	brokers := strings.Split(helpers.Getenv("KAFKA_BROKERS", "127.0.0.1:9092"), ",")
	topic := helpers.Getenv("KAFKA_TOPIC_BROADCAST", "tx.broadcast.v1")
	return kafka.NewProducer(brokers, topic)
}

func initKafkaConsumer() *broadcaster.ConsumerRuntime {
	brokers := strings.Split(helpers.Getenv("KAFKA_BROKERS", "127.0.0.1:9092"), ",")
	topic := helpers.Getenv("KAFKA_TOPIC_BROADCAST", "tx.broadcast.v1")
	group := helpers.Getenv("KAFKA_GROUP", "broadcaster-v1")
	dlqTopic := helpers.Getenv("KAFKA_TOPIC_BROADCAST_DLQ", "tx.broadcast.dlq.v1")

	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  group,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	log.Printf("[broadcaster] consuming %s group=%s", topic, group)

	return &broadcaster.ConsumerRuntime{
		Reader: r,
		Dlq:    kafka.NewProducer(brokers, dlqTopic),
		Topic:  topic,
		Group:  group,
	}
}
