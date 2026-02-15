package main

import (
	"context"
	"log"
	"math/big"
	"os"
	"strings"
	"time"

	"wallet-system/internal/api"
	"wallet-system/internal/chain/evm"
	"wallet-system/internal/clients"
	"wallet-system/internal/helpers"
	"wallet-system/internal/infra/kafka"
	"wallet-system/internal/infra/redisx"
	"wallet-system/internal/withdraw"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/joho/godotenv"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	if err := godotenv.Load(".env"); err != nil && !os.IsNotExist(err) {
		log.Fatalf("load .env failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	ethRPC := helpers.MustEnv("ETH_RPC")
	chain := helpers.MustEnv("ETH_CHAIN")
	chainIDStr := helpers.MustEnv("ETH_CHAIN_ID")
	redisAddr := helpers.MustEnv("REDIS_ADDR")
	redisPass := helpers.MustEnv("REDIS_PASS")
	redisDB := helpers.MustEnv("REDIS_DB")

	chainID, ok := new(big.Int).SetString(chainIDStr, 10)
	if !ok {
		log.Fatalf("invalid ETH_CHAIN_ID: %s", chainIDStr)
	}
	log.Printf("[withdraw-api] config loaded chain=%s chain_id=%s", chain, chainID.String())

	// redis
	rdc := redisx.New(redisAddr, redisPass, redisDB)
	if err := rdc.Ping(ctx); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}
	log.Printf("[withdraw-api] redis ready addr=%s db=%s", redisAddr, redisDB)

	// kafka
	brokers := strings.Split(helpers.Getenv("KAFKA_BROKERS", "127.0.0.1:9092"), ",")
	topic := helpers.Getenv("KAFKA_TOPIC_BROADCAST", "tx.broadcast.v1")
	producer := kafka.NewProducer(brokers, topic)
	log.Printf("[withdraw-api] kafka producer ready topic=%s brokers=%s", topic, strings.Join(brokers, ","))

	// Signer gRPC client
	signerAddr := helpers.Getenv("SIGNER_GRPC_ADDR", "127.0.0.1:9001")
	signerCli, err := clients.NewSignerClient(signerAddr)
	if err != nil {
		log.Fatalf("init signer client failed: %v", err)
	}
	log.Printf("[withdraw-api] signer client ready addr=%s", signerAddr)

	// Auth secret（demo 同值；生产会按调用方分配/或 mTLS）
	authSecret := []byte(helpers.MustEnv("WITHDRAW_AUTH_SECRET"))

	// Withdraw service（编排层）
	eth, err := ethclient.Dial(ethRPC)
	if err != nil {
		log.Fatalf("init eth client failed: %v", err)
	}
	log.Printf("[withdraw-api] eth client ready rpc=%s", ethRPC)
	wsvc := withdraw.NewService(withdraw.Config{
		Chain:      chain,
		ChainID:    chainID,
		From:       common.HexToAddress(helpers.MustEnv("FROM_ADDRESS")),
		AuthSecret: authSecret,
	}, withdraw.Deps{
		Redis:   rdc.RDB,
		Eth:     eth,
		Builder: evm.NewEVMBuilder(eth),
		Signer:  signerCli,
	}, producer)

	// Gin router
	r := api.NewRouter(wsvc)

	addr := helpers.Getenv("API_ADDR", "127.0.0.1:8080")
	log.Printf("[withdraw-api] listening on %s", addr)
	if err := r.Run(addr); err != nil {
		log.Fatal(err)
	}
}
