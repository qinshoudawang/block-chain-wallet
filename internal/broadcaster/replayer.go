package broadcaster

import (
	"context"
	"encoding/json"
	"time"

	"wallet-system/internal/helpers"
	"wallet-system/internal/infra/kafka"
	withdrawmodel "wallet-system/internal/storage/model/withdraw"
	"wallet-system/internal/storage/repo"
)

type Replayer struct {
	wr   *repo.WithdrawRepo
	prod *kafka.Producer
}

func NewReplayer(wr *repo.WithdrawRepo, prod *kafka.Producer) *Replayer {
	return &Replayer{wr: wr, prod: prod}
}

func RunReplayer(ctx context.Context, wr *repo.WithdrawRepo, prod *kafka.Producer) {
	NewReplayer(wr, prod).Run(ctx)
}

func (r *Replayer) Run(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.tick(ctx)
		}
	}
}

func (r *Replayer) tick(ctx context.Context) {
	items, err := r.wr.ListDueRetries(ctx, 200)
	if err != nil {
		return
	}
	for _, o := range items {
		r.tryReplay(ctx, o)
	}
}

func (r *Replayer) tryReplay(ctx context.Context, o withdrawmodel.WithdrawOrder) {
	// Occupy first to reduce duplicate delivery across replayers.
	ok, _ := r.wr.MarkReplayScheduled(ctx, o.WithdrawID, 30*time.Second)
	if !ok {
		return
	}

	task := BroadcastTask{
		Version:               1,
		WithdrawID:            o.WithdrawID,
		RequestID:             o.RequestID,
		Chain:                 o.Chain,
		From:                  o.FromAddr,
		To:                    o.ToAddr,
		Amount:                o.Amount,
		Sequence:              o.Sequence,
		SignedPayload:         o.SignedPayload,
		SignedPayloadEncoding: normalizeSignedPayloadEncoding(o.SignedPayloadEncoding),
		TokenContractAddress:  o.TokenContractAddress,
		CreatedAt:             time.Now().Unix(),
		Attempt:               o.RetryCount,
	}

	b, _ := json.Marshal(task)
	_ = r.prod.Publish(ctx, o.WithdrawID, b)
}

func normalizeSignedPayloadEncoding(v string) string {
	if v == "" {
		return SignedPayloadEncodingHex
	}
	return v
}

func nextRetryTime(attempt int) time.Time {
	return time.Now().Add(helpers.NextBackoff(attempt))
}
