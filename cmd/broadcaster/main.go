package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"log"
	"strings"
	"time"

	"wallet-system/internal/chain/evm"
	"wallet-system/internal/helpers"
	"wallet-system/internal/infra/kafka"
	"wallet-system/internal/withdraw"

	kafkago "github.com/segmentio/kafka-go"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	// tx sender
	rpc := helpers.MustEnv("ETH_RPC")
	sender, err := evm.NewEVMSender(rpc)
	if err != nil {
		log.Fatalf("init sender failed: %v", err)
	}

	brokers := strings.Split(helpers.Getenv("KAFKA_BROKERS", "127.0.0.1:9092"), ",")

	// DLQ producer
	dlq := kafka.NewProducer(brokers, "tx.broadcast.dlq.v1")
	defer dlq.Close()

	// consumer
	topic := helpers.Getenv("KAFKA_TOPIC_BROADCAST", "tx.broadcast.v1")
	group := helpers.Getenv("KAFKA_GROUP", "broadcaster-v1")

	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: group,
		// 单笔提现消息较小，避免因 MinBytes 过大导致拉取延迟
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer r.Close()

	log.Printf("[broadcaster] consuming %s group=%s", topic, group)

	for {
		msg, err := r.FetchMessage(context.Background())
		if err != nil {
			log.Printf("fetch error: %v", err)
			time.Sleep(300 * time.Millisecond)
			continue
		}

		var task withdraw.BroadcastTask
		if err := json.Unmarshal(msg.Value, &task); err != nil {
			log.Printf("bad message, send to dlq: %v", err)
			_ = dlq.Publish(context.Background(), string(msg.Key), msg.Value)
			_ = r.CommitMessages(context.Background(), msg)
			continue
		}
		log.Printf("message received topic=%s partition=%d offset=%d withdraw=%s req=%s nonce=%d", topic, msg.Partition, msg.Offset, task.WithdrawID, task.RequestID, task.Nonce)

		// 广播（最小重试：本地退避 2 次）
		txHash, err := broadcastWithRetry(context.Background(), sender, task.SignedTxHex)
		if err != nil {
			task.Attempt++
			// 超过阈值 → DLQ
			if task.Attempt >= 3 {
				b, _ := json.Marshal(task)
				_ = dlq.Publish(context.Background(), string(msg.Key), b)
				log.Printf("DLQ withdraw=%s req=%s err=%v", task.WithdrawID, task.RequestID, err)
				_ = r.CommitMessages(context.Background(), msg)
				continue
			}

			// TODO：写 retry topic（延迟）或写 DB 让定时器重试
			b, _ := json.Marshal(task)
			_ = dlq.Publish(context.Background(), string(msg.Key), b) // 先放 DLQ 便于观察
			log.Printf("broadcast failed, sent to dlq withdraw=%s req=%s attempt=%d err=%v", task.WithdrawID, task.RequestID, task.Attempt, err)
			_ = r.CommitMessages(context.Background(), msg)
			continue
		}

		log.Printf("broadcast ok withdraw=%s req=%s tx=%s nonce=%d", task.WithdrawID, task.RequestID, txHash, task.Nonce)

		// TODO: 更新 DB 状态 BROADCASTED + txHash（下一步做）
		_ = r.CommitMessages(context.Background(), msg)
	}
}

func broadcastWithRetry(ctx context.Context, sender *evm.EVMSender, signedHex string) (string, error) {
	raw, err := hex.DecodeString(strings.TrimPrefix(signedHex, "0x"))
	if err != nil {
		return "", err
	}
	backoffs := []time.Duration{0, 300 * time.Millisecond, 800 * time.Millisecond}
	var last error
	for _, b := range backoffs {
		if b > 0 {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(b):
			}
		}
		h, err := sender.Broadcast(ctx, raw)
		if err == nil {
			return h, nil
		}
		last = err
	}
	return "", last
}
