package chainindex

import (
	"context"
	"log"
	"time"

	chainmodel "wallet-system/internal/storage/model/chain"
	"wallet-system/internal/storage/repo"
)

type projectorHandler func(context.Context, chainmodel.ChainEvent) error

type projectorConfig struct {
	name      string
	chain     string
	eventType chainmodel.EventType
	poll      time.Duration
	repo      *repo.ChainRepo
	handle    projectorHandler
}

func runProjector(ctx context.Context, cfg projectorConfig) {
	if cfg.repo == nil || cfg.handle == nil {
		return
	}
	poll := cfg.poll
	if poll <= 0 {
		poll = 3 * time.Second
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()

	projectOnce(ctx, cfg)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			projectOnce(ctx, cfg)
		}
	}
}

func projectOnce(ctx context.Context, cfg projectorConfig) {
	cur, err := cfg.repo.GetOrCreateProjectorCursor(ctx, cfg.name)
	if err != nil {
		log.Printf("[projector] load cursor failed name=%s chain=%s err=%v", cfg.name, cfg.chain, err)
		return
	}
	items, err := cfg.repo.ListEventsAfterID(ctx, cfg.chain, cfg.eventType, cur.LastEventID, 500)
	if err != nil {
		log.Printf("[projector] list events failed name=%s chain=%s last_id=%d err=%v", cfg.name, cfg.chain, cur.LastEventID, err)
		return
	}
	if len(items) == 0 {
		return
	}
	var maxID uint64
	for i := range items {
		ev := items[i]
		if err := cfg.handle(ctx, ev); err != nil {
			log.Printf("[projector] handle failed name=%s chain=%s event_id=%d tx=%s err=%v", cfg.name, cfg.chain, ev.ID, ev.TxHash, err)
			return
		}
		maxID = ev.ID
	}
	if maxID > 0 {
		if err := cfg.repo.SaveProjectorCursor(ctx, cfg.name, maxID); err != nil {
			log.Printf("[projector] save cursor failed name=%s chain=%s last_id=%d err=%v", cfg.name, cfg.chain, maxID, err)
		}
	}
}
