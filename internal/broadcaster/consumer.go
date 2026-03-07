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
	sweepRepo    *repo.SweepRepo
	clients      *chainclient.Registry
	utxoReserve  *utxoreserve.Manager
	runtime      *ConsumerRuntime
}

type ConsumerDeps struct {
	WithdrawRepo *repo.WithdrawRepo
	SweepRepo    *repo.SweepRepo
	Clients      *chainclient.Registry
	UTXOReserve  *utxoreserve.Manager
	Runtime      *ConsumerRuntime
}

func NewConsumer(deps ConsumerDeps) *Consumer {
	return &Consumer{
		withdrawRepo: deps.WithdrawRepo,
		sweepRepo:    deps.SweepRepo,
		clients:      deps.Clients,
		utxoReserve:  deps.UTXOReserve,
		runtime:      deps.Runtime,
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

func RunConsumer(ctx context.Context, deps ConsumerDeps) {
	NewConsumer(deps).Run(ctx)
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
		"message received topic=%s partition=%d offset=%d type=%s withdraw=%s sweep=%s req=%s sequence=%s",
		c.runtime.Topic, msg.Partition, msg.Offset, normalizeTaskType(task.TaskType), task.WithdrawID, task.SweepID, task.RequestID, sequenceForLog(task.Sequence),
	)

	switch normalizeTaskType(task.TaskType) {
	case TaskTypeSweep:
		c.handleSweepTask(ctx, task, msg)
	case TaskTypeTopUp:
		c.handleTopUpTask(ctx, task, msg)
	default:
		c.handleWithdrawTask(ctx, task, msg)
	}
}

func (c *Consumer) handleWithdrawTask(ctx context.Context, task BroadcastTask, msg kafkago.Message) {
	if c.withdrawRepo == nil {
		log.Printf("withdraw repo not configured for withdraw task withdraw=%s", task.WithdrawID)
		c.publishDLQTask(ctx, task, string(msg.Key))
		c.commit(ctx, msg)
		return
	}

	chain, cli, err := c.clients.Resolve(task.Chain)
	if err != nil {
		c.handleBroadcastError(ctx, task, err, msg)
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

func (c *Consumer) handleSweepTask(ctx context.Context, task BroadcastTask, msg kafkago.Message) {
	if c.sweepRepo == nil {
		log.Printf("sweep repo not configured for sweep task sweep=%s", task.SweepID)
		c.publishDLQTask(ctx, task, string(msg.Key))
		c.commit(ctx, msg)
		return
	}
	chain, cli, err := c.clients.Resolve(task.Chain)
	if err != nil {
		_ = c.sweepRepo.MarkFailed(ctx, task.SweepID, err.Error())
		c.publishDLQTask(ctx, task, string(msg.Key))
		log.Printf("DLQ sweep=%s chain=%s err=%v", task.SweepID, task.Chain, err)
		c.commit(ctx, msg)
		return
	}
	txHash, err := broadcastWithRetry(ctx, cli, task.SignedPayload, task.SignedPayloadEncoding)
	if err != nil {
		_ = c.sweepRepo.MarkFailed(ctx, task.SweepID, err.Error())
		c.publishDLQTask(ctx, task, string(msg.Key))
		log.Printf("DLQ sweep=%s req=%s err=%v", task.SweepID, task.RequestID, err)
		c.commit(ctx, msg)
		return
	}
	log.Printf("broadcast sweep ok chain=%s sweep=%s req=%s tx=%s", chain, task.SweepID, task.RequestID, txHash)
	if err := c.sweepRepo.MarkBroadcasted(ctx, task.SweepID, txHash); err != nil {
		log.Printf("mark sweep broadcasted failed sweep=%s tx=%s err=%v", task.SweepID, txHash, err)
	}
	c.commit(ctx, msg)
}

func (c *Consumer) handleTopUpTask(ctx context.Context, task BroadcastTask, msg kafkago.Message) {
	chain, cli, err := c.clients.Resolve(task.Chain)
	if err != nil {
		c.publishDLQTask(ctx, task, string(msg.Key))
		log.Printf("DLQ topup req=%s chain=%s err=%v", task.RequestID, task.Chain, err)
		c.commit(ctx, msg)
		return
	}
	txHash, err := broadcastWithRetry(ctx, cli, task.SignedPayload, task.SignedPayloadEncoding)
	if err != nil {
		c.publishDLQTask(ctx, task, string(msg.Key))
		log.Printf("DLQ topup req=%s err=%v", task.RequestID, err)
		c.commit(ctx, msg)
		return
	}
	log.Printf("broadcast topup ok chain=%s req=%s from=%s to=%s amount=%s tx=%s", chain, task.RequestID, task.From, task.To, task.Amount, txHash)
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
		c.releaseReservation(ctx, task.WithdrawID, task.Chain)
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

func (c *Consumer) releaseReservation(ctx context.Context, withdrawID string, chain string) {
	if c.utxoReserve == nil || !helpers.NeedReservation(chain) {
		return
	}
	if err := c.utxoReserve.ReleaseByWithdrawID(ctx, withdrawID); err != nil {
		log.Printf("release utxo reservation failed withdraw=%s chain=%s err=%v", withdrawID, chain, err)
	}
}

func (c *Consumer) publishDLQTask(ctx context.Context, task BroadcastTask, key string) {
	b, _ := json.Marshal(task)
	if c == nil || c.runtime == nil || c.runtime.Dlq == nil {
		log.Printf("dlq producer not configured key=%s withdraw=%s sweep=%s req=%s", key, task.WithdrawID, task.SweepID, task.RequestID)
		return
	}
	if err := c.runtime.Dlq.Publish(ctx, key, b); err != nil {
		log.Printf("publish dlq failed key=%s withdraw=%s sweep=%s req=%s err=%v", key, task.WithdrawID, task.SweepID, task.RequestID, err)
	}
}

func (c *Consumer) commit(ctx context.Context, msg kafkago.Message) {
	if c == nil || c.runtime == nil || c.runtime.Reader == nil {
		return
	}
	if err := c.runtime.Reader.CommitMessages(ctx, msg); err != nil {
		log.Printf("commit failed topic=%s partition=%d offset=%d err=%v", msg.Topic, msg.Partition, msg.Offset, err)
	}
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

func normalizeTaskType(v string) string {
	switch v {
	case TaskTypeSweep:
		return TaskTypeSweep
	case TaskTypeTopUp:
		return TaskTypeTopUp
	default:
		return TaskTypeWithdraw
	}
}
