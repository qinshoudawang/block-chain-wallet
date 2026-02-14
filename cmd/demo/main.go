package main

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"os"
	"time"

	"wallet-system/internal/signer"
	pb "wallet-system/internal/signer/pb"
	"wallet-system/internal/signer/provider"
	"wallet-system/internal/txbuilder"
	"wallet-system/internal/txsender"
	"wallet-system/pkg/redisx"

	"github.com/ethereum/go-ethereum/common"
	"github.com/joho/godotenv"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("missing env: %s", k)
	}
	return v
}

func main() {
	if err := godotenv.Load(".env"); err != nil && !os.IsNotExist(err) {
		log.Fatalf("load .env failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	ethRPC := mustEnv("ETH_RPC")
	priv := mustEnv("HOT_WALLET_PRIV")
	chainIDStr := mustEnv("ETH_CHAIN_ID")
	redisAddr := mustEnv("REDIS_ADDR")
	redisPass := mustEnv("REDIS_PASS")
	redisDB := mustEnv("REDIS_DB")

	chainID, ok := new(big.Int).SetString(chainIDStr, 10)
	if !ok {
		log.Fatalf("invalid ETH_CHAIN_ID: %s", chainIDStr)
	}

	// 1) Redis
	rdc := redisx.New(redisAddr, redisPass, redisDB)
	if err := rdc.Ping(ctx); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}

	// 2) Builder + Sender
	builder, err := txbuilder.NewEVMBuilder(ethRPC)
	if err != nil {
		log.Fatalf("builder init: %v", err)
	}
	sender, err := txsender.NewEVMSender(ethRPC)
	if err != nil {
		log.Fatalf("sender init: %v", err)
	}

	// 3) Signer Provider
	evmSigner, err := provider.NewEVMLocalSigner(priv, chainID)
	if err != nil {
		log.Fatalf("evm signer init: %v", err)
	}

	// 4) Signer Service
	svc := signer.NewGRPCServer(rdc.RDB, evmSigner)

	from := common.HexToAddress(mustEnv("FROM_ADDRESS")) // 和 priv 匹配
	to := common.HexToAddress(mustEnv("TO_ADDRESS"))

	// 发送 0.001 ETH（单位 wei）
	value := new(big.Int).Mul(big.NewInt(1e15), big.NewInt(1)) // 0.001 ETH = 1e15 wei
	// ==============================================

	// 5) 构造 unsigned tx（EIP-1559）
	unsignedTx, err := builder.BuildUnsignedTx(ctx, from, to, value, nil, chainID)
	if err != nil {
		log.Fatalf("build unsigned tx: %v", err)
	}

	// 6) 调 signer（这里会触发：幂等 + nonce 锁 + policy + 签名）
	req := &pb.SignRequest{
		RequestId:   fmt.Sprintf("demo-%d", time.Now().UnixNano()),
		Chain:       "evm:sepolia",
		FromAddress: from.Hex(),
		ToAddress:   to.Hex(),
		Amount:      value.String(), // wei
		UnsignedTx:  unsignedTx,
	}

	resp, err := svc.SignTransaction(ctx, req)
	if err != nil {
		log.Fatalf("sign tx: %v", err)
	}

	// 7) 广播
	txHash, err := sender.Broadcast(ctx, resp.SignedTx)
	if err != nil {
		log.Fatalf("broadcast: %v", err)
	}

	log.Println("✅ sent tx:", txHash)
}
