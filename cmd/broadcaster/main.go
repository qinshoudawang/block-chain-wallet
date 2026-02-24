package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"wallet-system/internal/chain/evm"
	"wallet-system/internal/helpers"
	"wallet-system/internal/infra/kafka"
	storagemigrate "wallet-system/internal/storage/migrate"
	"wallet-system/internal/storage/repo"

	kafkago "github.com/segmentio/kafka-go"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const (
	maxRetry  = 4
	threshold = 5
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go handleShutdown(cancel)

	db := initGorm()
	withdrawRepo := repo.NewWithdrawRepo(db)
	ledgerRepo := repo.NewLedgerRepo(db)
	producer := initKafkaProducer()
	defer producer.Close()
	consumer := initKafkaConsumer()
	defer consumer.Close()
	sender := initEVMSender()

	go RunConsumer(ctx, withdrawRepo, sender, consumer)
	go RunReplayer(ctx, withdrawRepo, producer)
	go RunConfirmer(ctx, withdrawRepo, ledgerRepo, sender.EthClient(), threshold)

	<-ctx.Done()
}

type consumerRuntime struct {
	reader *kafkago.Reader
	dlq    *kafka.Producer
	topic  string
	group  string
}

func (c *consumerRuntime) Close() error {
	if c == nil {
		return nil
	}
	if c.reader != nil {
		_ = c.reader.Close()
	}
	if c.dlq != nil {
		_ = c.dlq.Close()
	}
	return nil
}

func handleShutdown(cancel context.CancelFunc) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(ch)
	<-ch
	cancel()
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

func initEVMSender() *evm.EVMSender {
	rpc := helpers.MustEnv("ETH_RPC")
	sender, err := evm.NewEVMSender(rpc)
	if err != nil {
		log.Fatalf("init sender failed: %v", err)
	}
	return sender
}

func initKafkaProducer() *kafka.Producer {
	brokers := strings.Split(helpers.Getenv("KAFKA_BROKERS", "127.0.0.1:9092"), ",")
	topic := helpers.Getenv("KAFKA_TOPIC_BROADCAST", "tx.broadcast.v1")
	return kafka.NewProducer(brokers, topic)
}

func initKafkaConsumer() *consumerRuntime {
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

	return &consumerRuntime{
		reader: r,
		dlq:    kafka.NewProducer(brokers, dlqTopic),
		topic:  topic,
		group:  group,
	}
}
