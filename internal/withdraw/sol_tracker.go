package withdraw

import (
	"context"
	"log"
	"math/big"
	"strings"
	"time"

	solchain "wallet-system/internal/chain/sol"
	"wallet-system/internal/storage/model/withdraw"
	"wallet-system/internal/storage/repo"
)

type SOLTracker struct {
	chainRepo    *repo.ChainRepo
	withdrawRepo *repo.WithdrawRepo
	ledgerRepo   *repo.LedgerRepo
	sol          *solchain.Client
	chain        string
	threshold    int
	poll         time.Duration
	reorgCursor  string
}

func NewSOLTracker(chain string, threshold int, chainRepo *repo.ChainRepo, withdrawRepo *repo.WithdrawRepo, ledgerRepo *repo.LedgerRepo, sol *solchain.Client, poll time.Duration) *SOLTracker {
	return &SOLTracker{
		chainRepo:    chainRepo,
		withdrawRepo: withdrawRepo,
		ledgerRepo:   ledgerRepo,
		sol:          sol,
		chain:        strings.ToLower(strings.TrimSpace(chain)),
		threshold:    threshold,
		poll:         poll,
		reorgCursor:  "withdraw-reorg-tracker:" + strings.ToLower(strings.TrimSpace(chain)),
	}
}

func (w *SOLTracker) Run(ctx context.Context) {
	if w == nil || w.chainRepo == nil || w.withdrawRepo == nil || w.sol == nil {
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

func (w *SOLTracker) tick(ctx context.Context) {
	latest, err := w.sol.LatestHeight(ctx)
	if err != nil {
		log.Printf("[sol-withdraw-tracker] latest slot failed chain=%s err=%v", w.chain, err)
		return
	}
	if err := w.handleReorgs(ctx, latest); err != nil {
		log.Printf("[sol-withdraw-tracker] handle reorgs failed chain=%s err=%v", w.chain, err)
		return
	}
	items, err := w.withdrawRepo.ListBroadcastedToConfirm(ctx, 200)
	if err != nil {
		log.Printf("[sol-withdraw-tracker] list broadcasted failed chain=%s err=%v", w.chain, err)
		return
	}
	for i := range items {
		item := items[i]
		if !strings.EqualFold(item.Chain, w.chain) {
			continue
		}
		if err := w.reconcileOrder(ctx, &item, latest, false); err != nil {
			log.Printf("[sol-withdraw-tracker] reconcile failed withdraw_id=%s tx=%s err=%v", item.WithdrawID, item.TxHash, err)
		}
	}
}

func (w *SOLTracker) handleReorgs(ctx context.Context, latest uint64) error {
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

func (w *SOLTracker) reconcileFromBlock(ctx context.Context, fromBlock uint64, latest uint64) error {
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

func (w *SOLTracker) reconcileOrder(ctx context.Context, rec *withdrawmodel.WithdrawOrder, latest uint64, allowRevert bool) error {
	if rec == nil || strings.TrimSpace(rec.TxHash) == "" {
		return nil
	}
	status, err := w.sol.GetSignatureStatus(ctx, rec.TxHash)
	if err != nil {
		return err
	}
	if status == nil {
		if allowRevert {
			_, err = w.withdrawRepo.RevertConfirmationWithSettlement(ctx, w.ledgerRepo, rec.WithdrawID)
		}
		return err
	}
	fee, err := w.sol.GetTransactionFee(ctx, rec.TxHash)
	if err != nil {
		return err
	}
	if fee == nil {
		fee = big.NewInt(0)
	}
	amount := parseAmount(rec.Amount)
	if status.Err != nil {
		if allowRevert {
			if _, err := w.withdrawRepo.RevertConfirmationWithSettlement(ctx, w.ledgerRepo, rec.WithdrawID); err != nil {
				return err
			}
		}
		_, err = w.withdrawRepo.FailWithSettlement(ctx, w.ledgerRepo, rec.WithdrawID, "", amount, "", fee)
		return err
	}
	conf := solConfirmations(status, latest)
	if conf < w.threshold {
		if allowRevert {
			_, err = w.withdrawRepo.RevertConfirmationWithSettlement(ctx, w.ledgerRepo, rec.WithdrawID)
			return err
		}
		_, err = w.withdrawRepo.UpdateConfirmations(ctx, rec.WithdrawID, status.Slot, conf, w.threshold)
		return err
	}
	if allowRevert {
		if _, err := w.withdrawRepo.RevertConfirmationWithSettlement(ctx, w.ledgerRepo, rec.WithdrawID); err != nil {
			return err
		}
	}
	_, err = w.withdrawRepo.ConfirmWithSettlement(ctx, w.ledgerRepo, rec.WithdrawID, status.Slot, conf, w.threshold, "", amount, "", fee)
	return err
}

func parseAmount(v string) *big.Int {
	out, ok := new(big.Int).SetString(strings.TrimSpace(v), 10)
	if !ok || out.Sign() < 0 {
		return big.NewInt(0)
	}
	return out
}

func solConfirmations(status *solchain.SignatureStatus, latest uint64) int {
	if status == nil {
		return 0
	}
	if status.Confirmations != nil {
		return int(*status.Confirmations)
	}
	if status.Slot > 0 && latest >= status.Slot {
		return int(latest-status.Slot) + 1
	}
	switch status.ConfirmationStatus {
	case "confirmed", "finalized":
		return 1
	default:
		return 0
	}
}
