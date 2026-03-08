package withdraw

import (
	"context"
	"log"
	"strings"
	"time"

	"wallet-system/internal/broadcaster/chainclient"
	btcchain "wallet-system/internal/chain/btc"
	"wallet-system/internal/storage/model/withdraw"
	"wallet-system/internal/storage/repo"
)

type BTCTracker struct {
	chainRepo    *repo.ChainRepo
	withdrawRepo *repo.WithdrawRepo
	ledgerRepo   *repo.LedgerRepo
	client       chainclient.Client
	chain        string
	threshold    int
	poll         time.Duration
	reorgCursor  string
}

func NewBTCTracker(chain string, threshold int, chainRepo *repo.ChainRepo, withdrawRepo *repo.WithdrawRepo, ledgerRepo *repo.LedgerRepo, btc *btcchain.Client, poll time.Duration) *BTCTracker {
	return &BTCTracker{
		chainRepo:    chainRepo,
		withdrawRepo: withdrawRepo,
		ledgerRepo:   ledgerRepo,
		client:       chainclient.NewBTCClient(btc),
		chain:        strings.ToLower(strings.TrimSpace(chain)),
		threshold:    threshold,
		poll:         poll,
		reorgCursor:  "withdraw-reorg-tracker:" + strings.ToLower(strings.TrimSpace(chain)),
	}
}

func (w *BTCTracker) Run(ctx context.Context) {
	if w == nil || w.chainRepo == nil || w.withdrawRepo == nil || w.client == nil {
		return
	}
	poll := w.poll
	if poll <= 0 {
		poll = 30 * time.Second
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()

	w.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.tick(ctx)
		}
	}
}

func (w *BTCTracker) tick(ctx context.Context) {
	latest, err := w.client.GetLatestHeight(ctx)
	if err != nil {
		log.Printf("[btc-withdraw-tracker] latest height failed chain=%s err=%v", w.chain, err)
		return
	}
	if err := w.handleReorgs(ctx, latest); err != nil {
		log.Printf("[btc-withdraw-tracker] handle reorgs failed chain=%s err=%v", w.chain, err)
		return
	}
	items, err := w.withdrawRepo.ListBroadcastedToConfirm(ctx, 200)
	if err != nil {
		log.Printf("[btc-withdraw-tracker] list broadcasted failed chain=%s err=%v", w.chain, err)
		return
	}
	for i := range items {
		item := items[i]
		if !strings.EqualFold(item.Chain, w.chain) {
			continue
		}
		if err := w.reconcileOrder(ctx, &item, latest, false); err != nil {
			log.Printf("[btc-withdraw-tracker] reconcile failed withdraw_id=%s tx=%s err=%v", item.WithdrawID, item.TxHash, err)
		}
	}
}

func (w *BTCTracker) handleReorgs(ctx context.Context, latest uint64) error {
	cur, err := w.chainRepo.GetOrCreateTrackerCursor(ctx, w.reorgCursor)
	if err != nil {
		return err
	}
	notices, err := w.chainRepo.ListReorgNoticesAfterID(ctx, w.chain, cur.LastEventID, 100)
	if err != nil {
		return err
	}
	if len(notices) == 0 {
		return nil
	}
	maxID := cur.LastEventID
	for _, notice := range notices {
		if err := w.reconcileFromBlock(ctx, notice.FromBlock, latest); err != nil {
			return err
		}
		if notice.ID > maxID {
			maxID = notice.ID
		}
	}
	return w.chainRepo.SaveTrackerCursor(ctx, w.reorgCursor, maxID)
}

func (w *BTCTracker) reconcileFromBlock(ctx context.Context, fromBlock uint64, latest uint64) error {
	var lastID uint64
	for {
		items, err := w.withdrawRepo.ListReorgAffectedAfterID(ctx, w.chain, fromBlock, lastID, 200)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}
		for i := range items {
			item := items[i]
			lastID = item.ID
			if err := w.reconcileOrder(ctx, &item, latest, true); err != nil {
				return err
			}
		}
	}
}

func (w *BTCTracker) reconcileOrder(ctx context.Context, rec *withdrawmodel.WithdrawOrder, latest uint64, allowRevert bool) error {
	if rec == nil || strings.TrimSpace(rec.TxHash) == "" {
		return nil
	}
	cf, err := w.client.GetConfirmation(ctx, rec.TxHash, rec.Amount, latest)
	if err != nil {
		return err
	}
	if cf == nil {
		if allowRevert {
			_, err = w.withdrawRepo.RevertConfirmationWithSettlement(ctx, w.ledgerRepo, rec.WithdrawID)
		}
		return err
	}
	if cf.Confirmations < w.threshold {
		if allowRevert {
			_, err = w.withdrawRepo.RevertConfirmationWithSettlement(ctx, w.ledgerRepo, rec.WithdrawID)
			return err
		}
		_, err = w.withdrawRepo.UpdateConfirmations(ctx, rec.WithdrawID, cf.BlockNumber, cf.Confirmations, w.threshold)
		return err
	}
	if allowRevert {
		if _, err := w.withdrawRepo.RevertConfirmationWithSettlement(ctx, w.ledgerRepo, rec.WithdrawID); err != nil {
			return err
		}
	}
	_, err = w.withdrawRepo.ConfirmWithSettlement(
		ctx,
		w.ledgerRepo,
		rec.WithdrawID,
		cf.BlockNumber,
		cf.Confirmations,
		w.threshold,
		cf.Settlement.TransferAssetContractAddress,
		cf.Settlement.TransferSpentAmount,
		cf.Settlement.NetworkFeeAssetContractAddress,
		cf.Settlement.NetworkFeeAmount,
	)
	return err
}
