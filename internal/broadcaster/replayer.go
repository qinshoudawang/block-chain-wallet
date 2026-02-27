package broadcaster

import (
	"context"
	"encoding/json"
	"time"

	"wallet-system/internal/helpers"
	"wallet-system/internal/infra/kafka"
	"wallet-system/internal/storage/repo"
)

func RunReplayer(ctx context.Context, wr *repo.WithdrawRepo, prod *kafka.Producer) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			items, err := wr.ListDueRetries(ctx, 200)
			if err != nil {
				continue
			}
			for _, o := range items {
				// 占位，减少重复投递
				ok, _ := wr.MarkReplayScheduled(ctx, o.WithdrawID, 30*time.Second)
				if !ok {
					continue
				}

				task := BroadcastTask{
					Version:     1,
					WithdrawID:  o.WithdrawID,
					RequestID:   o.RequestID,
					Chain:       o.Chain,
					From:        o.FromAddr,
					To:          o.ToAddr,
					Amount:      o.Amount,
					Nonce:       o.Nonce,
					SignedTxHex: o.SignedTxHex,
					CreatedAt:   time.Now().Unix(),
					Attempt:     o.RetryCount,
				}
				b, _ := json.Marshal(task)
				_ = prod.Publish(ctx, o.WithdrawID, b)
			}
		}
	}
}

func nextRetryTime(attempt int) time.Time {
	return time.Now().Add(helpers.NextBackoff(attempt))
}
