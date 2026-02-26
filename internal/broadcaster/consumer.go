package broadcaster

import (
	"context"
	"encoding/json"
	"log"
	"time"
	"wallet-system/internal/broadcaster/chainclient"
	"wallet-system/internal/helpers"
	"wallet-system/internal/infra/kafka"
	"wallet-system/internal/storage/repo"
	"wallet-system/internal/withdraw"

	kafkago "github.com/segmentio/kafka-go"
)

type ConsumerRuntime struct {
	Reader *kafkago.Reader
	Dlq    *kafka.Producer
	Topic  string
	Group  string
}

func (c *ConsumerRuntime) Close() error {
	if c == nil {
		return nil
	}
	if c.Reader != nil {
		_ = c.Reader.Close()
	}
	if c.Dlq != nil {
		_ = c.Dlq.Close()
	}
	return nil
}

func RunConsumer(ctx context.Context, withdrawRepo *repo.WithdrawRepo, clients *chainclient.Registry, consumer *ConsumerRuntime) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := consumer.Reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("fetch error: %v", err)
			time.Sleep(300 * time.Millisecond)
			continue
		}

		var task withdraw.BroadcastTask
		if err := json.Unmarshal(msg.Value, &task); err != nil {
			log.Printf("bad message, send to dlq: %v", err)
			_ = consumer.Dlq.Publish(ctx, string(msg.Key), msg.Value)
			_ = consumer.Reader.CommitMessages(ctx, msg)
			continue
		}
		log.Printf("message received topic=%s partition=%d offset=%d withdraw=%s req=%s nonce=%d", consumer.Topic, msg.Partition, msg.Offset, task.WithdrawID, task.RequestID, task.Nonce)

		chain, cli, err := clients.Resolve(task.Chain)
		if err != nil {
			task.Attempt++
			b, _ := json.Marshal(task)
			_ = consumer.Dlq.Publish(ctx, string(msg.Key), b)
			log.Printf("DLQ withdraw=%s req=%s chain=%s err=%v", task.WithdrawID, task.RequestID, task.Chain, err)
			_ = consumer.Reader.CommitMessages(ctx, msg)
			continue
		}
		txHash, err := broadcastWithRetry(ctx, cli, task.SignedTxHex)
		if err != nil {
			task.Attempt++
			if task.Attempt > maxRetry {
				b, _ := json.Marshal(task)
				_ = consumer.Dlq.Publish(ctx, string(msg.Key), b)
				log.Printf("DLQ withdraw=%s req=%s err=%v", task.WithdrawID, task.RequestID, err)
				_ = consumer.Reader.CommitMessages(ctx, msg)
				continue
			}

			next := time.Now().Add(helpers.NextBackoff(task.Attempt))
			if _, dbErr := withdrawRepo.MarkRetry(ctx, task.WithdrawID, next, err.Error()); dbErr != nil {
				log.Printf("mark retry failed withdraw=%s err=%v", task.WithdrawID, dbErr)
			}
			_ = consumer.Reader.CommitMessages(ctx, msg)
			continue
		}

		log.Printf("broadcast ok chain=%s withdraw=%s req=%s tx=%s nonce=%d", chain, task.WithdrawID, task.RequestID, txHash, task.Nonce)
		if _, dbErr := withdrawRepo.MarkBroadcasted(ctx, task.WithdrawID, txHash); dbErr != nil {
			log.Printf("mark broadcasted failed withdraw=%s tx=%s err=%v", task.WithdrawID, txHash, dbErr)
		}
		_ = consumer.Reader.CommitMessages(ctx, msg)
	}
}

func broadcastWithRetry(ctx context.Context, cli chainclient.Client, signedHex string) (string, error) {
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
		h, err := cli.BroadcastSignedTxHex(ctx, signedHex)
		if err == nil {
			return h, nil
		}
		last = err
	}
	return "", last
}
