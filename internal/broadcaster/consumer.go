package broadcaster

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"
	"wallet-system/internal/broadcaster/chainclient"
	"wallet-system/internal/helpers"
	"wallet-system/internal/infra/kafka"
	"wallet-system/internal/sequence/utxoreserve"
	"wallet-system/internal/storage/model"
	"wallet-system/internal/storage/repo"

	kafkago "github.com/segmentio/kafka-go"
)

type ConsumerRuntime struct {
	Reader *kafkago.Reader
	Dlq    *kafka.Producer
	Topic  string
	Group  string
}

type Consumer struct {
	withdrawRepo *repo.WithdrawRepo
	clients      *chainclient.Registry
	utxoReserve  *utxoreserve.Manager
	runtime      *ConsumerRuntime
}

func NewConsumer(withdrawRepo *repo.WithdrawRepo, clients *chainclient.Registry, utxoReserve *utxoreserve.Manager, runtime *ConsumerRuntime) *Consumer {
	return &Consumer{
		withdrawRepo: withdrawRepo,
		clients:      clients,
		utxoReserve:  utxoReserve,
		runtime:      runtime,
	}
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

func RunConsumer(ctx context.Context, withdrawRepo *repo.WithdrawRepo, clients *chainclient.Registry, utxoReserve *utxoreserve.Manager, runtime *ConsumerRuntime) {
	NewConsumer(withdrawRepo, clients, utxoReserve, runtime).Run(ctx)
}

func (c *Consumer) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := c.runtime.Reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("fetch error: %v", err)
			time.Sleep(300 * time.Millisecond)
			continue
		}
		c.handleMessage(ctx, msg)
	}
}

func (c *Consumer) handleMessage(ctx context.Context, msg kafkago.Message) {
	task, ok := c.decodeTask(msg)
	if !ok {
		c.commit(ctx, msg)
		return
	}

	log.Printf(
		"message received topic=%s partition=%d offset=%d withdraw=%s req=%s sequence=%s",
		c.runtime.Topic, msg.Partition, msg.Offset, task.WithdrawID, task.RequestID, sequenceForLog(task.Sequence),
	)

	chain, cli, err := c.clients.Resolve(task.Chain)
	if err != nil {
		task.Attempt++
		c.publishDLQTask(ctx, task, string(msg.Key))
		log.Printf("DLQ withdraw=%s req=%s chain=%s err=%v", task.WithdrawID, task.RequestID, task.Chain, err)
		c.commit(ctx, msg)
		return
	}

	txHash, err := broadcastWithRetry(ctx, cli, task.SignedPayload, task.SignedPayloadEncoding)
	if err != nil {
		c.handleBroadcastError(ctx, task, err, msg)
		return
	}

	log.Printf("broadcast ok chain=%s withdraw=%s req=%s tx=%s sequence=%s", chain, task.WithdrawID, task.RequestID, txHash, sequenceForLog(task.Sequence))
	if _, dbErr := c.withdrawRepo.MarkBroadcasted(ctx, task.WithdrawID, txHash); dbErr != nil {
		log.Printf("mark broadcasted failed withdraw=%s tx=%s err=%v", task.WithdrawID, txHash, dbErr)
	}
	c.commit(ctx, msg)
}

func (c *Consumer) decodeTask(msg kafkago.Message) (BroadcastTask, bool) {
	var task BroadcastTask
	if err := json.Unmarshal(msg.Value, &task); err != nil {
		log.Printf("bad message, send to dlq: %v", err)
		_ = c.runtime.Dlq.Publish(context.Background(), string(msg.Key), msg.Value)
		return BroadcastTask{}, false
	}
	return task, true
}

func (c *Consumer) handleBroadcastError(ctx context.Context, task BroadcastTask, broadcastErr error, msg kafkago.Message) {
	task.Attempt++
	if task.Attempt > maxRetry {
		if _, dbErr := c.withdrawRepo.MarkFailed(ctx, task.WithdrawID, broadcastErr.Error()); dbErr != nil {
			log.Printf("mark failed failed withdraw=%s err=%v", task.WithdrawID, dbErr)
		}
		c.releaseReservation(ctx, task.WithdrawID, task.Chain, task.ReservationType)
		c.publishDLQTask(ctx, task, string(msg.Key))
		log.Printf("DLQ withdraw=%s req=%s err=%v", task.WithdrawID, task.RequestID, broadcastErr)
		c.commit(ctx, msg)
		return
	}

	next := time.Now().Add(helpers.NextBackoff(task.Attempt))
	if _, dbErr := c.withdrawRepo.MarkRetry(ctx, task.WithdrawID, next, broadcastErr.Error()); dbErr != nil {
		log.Printf("mark retry failed withdraw=%s err=%v", task.WithdrawID, dbErr)
	}
	c.commit(ctx, msg)
}

func (c *Consumer) releaseReservation(ctx context.Context, withdrawID string, chain string, reservationType string) {
	if c.utxoReserve == nil || reservationType != string(model.ReservationTypeUTXO) {
		return
	}
	if err := c.utxoReserve.ReleaseByWithdrawID(ctx, withdrawID); err != nil {
		log.Printf("release utxo reservation failed withdraw=%s chain=%s err=%v", withdrawID, chain, err)
	}
}

func (c *Consumer) publishDLQTask(ctx context.Context, task BroadcastTask, key string) {
	b, _ := json.Marshal(task)
	_ = c.runtime.Dlq.Publish(ctx, key, b)
}

func (c *Consumer) commit(ctx context.Context, msg kafkago.Message) {
	_ = c.runtime.Reader.CommitMessages(ctx, msg)
}

func broadcastWithRetry(ctx context.Context, cli chainclient.Client, signedPayload string, encoding string) (string, error) {
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
		signedHex, err := normalizeSignedPayloadHex(signedPayload, encoding)
		if err != nil {
			return "", err
		}
		h, err := cli.BroadcastSignedTxHex(ctx, signedHex)
		if err == nil {
			return h, nil
		}
		last = err
	}
	return "", last
}

func sequenceForLog(v uint64) string {
	return fmt.Sprintf("%d", v)
}

func normalizeSignedPayloadHex(payload string, encoding string) (string, error) {
	switch encoding {
	case "", SignedPayloadEncodingHex:
		return payload, nil
	case SignedPayloadEncodingBase64:
		raw, err := base64.StdEncoding.DecodeString(payload)
		if err != nil {
			return "", err
		}
		return hex.EncodeToString(raw), nil
	default:
		return "", errors.New("unsupported signed payload encoding")
	}
}
