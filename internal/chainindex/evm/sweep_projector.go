package evmindex

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	evmchain "wallet-system/internal/chain/evm"
	sweepmodel "wallet-system/internal/storage/model/sweep"
	"wallet-system/internal/storage/repo"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
)

type EVMSweepProjector struct {
	chainRepo    *repo.ChainRepo
	sweepRepo    *repo.SweepRepo
	evm          *evmchain.Client
	chain        string
	confirmDepth uint64
	poll         time.Duration
	reorgCursor  string
}

func NewEVMSweepProjector(chain string, confirmDepth uint64, chainRepo *repo.ChainRepo, sweepRepo *repo.SweepRepo, evm *evmchain.Client, poll time.Duration) *EVMSweepProjector {
	return &EVMSweepProjector{
		chainRepo:    chainRepo,
		sweepRepo:    sweepRepo,
		evm:          evm,
		chain:        strings.ToLower(strings.TrimSpace(chain)),
		confirmDepth: confirmDepth,
		poll:         poll,
		reorgCursor:  "sweep-reorg-projector:" + strings.ToLower(strings.TrimSpace(chain)),
	}
}

func (w *EVMSweepProjector) Run(ctx context.Context) {
	if w == nil || w.chainRepo == nil || w.sweepRepo == nil || w.evm == nil {
		return
	}
	poll := w.poll
	if poll <= 0 {
		poll = 8 * time.Second
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

func (w *EVMSweepProjector) tick(ctx context.Context) {
	finalized, ok := finalizedHeight(ctx, w.evm, w.confirmDepth)
	if !ok {
		return
	}
	if err := w.handleReorgs(ctx, finalized); err != nil {
		log.Printf("[evm-sweep-projector] handle reorgs failed chain=%s err=%v", w.chain, err)
		return
	}
	items, err := w.sweepRepo.ListBroadcastedToConfirm(ctx, 200)
	if err != nil {
		log.Printf("[evm-sweep-projector] list broadcasted failed chain=%s err=%v", w.chain, err)
		return
	}
	for _, item := range items {
		if !strings.EqualFold(item.Chain, w.chain) {
			continue
		}
		if err := w.reconcileOrder(ctx, &item, finalized, false); err != nil {
			log.Printf("[evm-sweep-projector] reconcile failed sweep_id=%s tx=%s err=%v", item.SweepID, item.TxHash, err)
		}
	}
}

func (w *EVMSweepProjector) handleReorgs(ctx context.Context, finalized uint64) error {
	cur, err := w.chainRepo.GetOrCreateProjectorCursor(ctx, w.reorgCursor)
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
	var maxID uint64 = cur.LastEventID
	for _, notice := range notices {
		if err := w.reconcileFromBlock(ctx, notice.FromBlock, finalized); err != nil {
			return err
		}
		if notice.ID > maxID {
			maxID = notice.ID
		}
	}
	return w.chainRepo.SaveProjectorCursor(ctx, w.reorgCursor, maxID)
}

func (w *EVMSweepProjector) reconcileFromBlock(ctx context.Context, fromBlock uint64, finalized uint64) error {
	var lastID uint64
	for {
		items, err := w.sweepRepo.ListReorgAffectedAfterID(ctx, w.chain, fromBlock, lastID, 200)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}
		for i := range items {
			item := items[i]
			lastID = item.ID
			if err := w.reconcileOrder(ctx, &item, finalized, true); err != nil {
				return err
			}
		}
	}
}

func (w *EVMSweepProjector) reconcileOrder(ctx context.Context, rec *sweepmodel.SweepOrder, finalized uint64, allowRevert bool) error {
	if rec == nil || strings.TrimSpace(rec.TxHash) == "" {
		return nil
	}
	txHash := common.HexToHash(strings.TrimSpace(rec.TxHash))
	tx, _, err := w.evm.TransactionByHash(ctx, txHash)
	if err != nil {
		if errors.Is(err, ethereum.NotFound) {
			if allowRevert {
				_, err = w.sweepRepo.RevertConfirmationWithSettlement(ctx, rec.SweepID)
			}
			return err
		}
		return err
	}
	receipt, err := w.evm.TransactionReceipt(ctx, txHash)
	if err != nil {
		if errors.Is(err, ethereum.NotFound) {
			if allowRevert {
				_, err = w.sweepRepo.RevertConfirmationWithSettlement(ctx, rec.SweepID)
			}
			return err
		}
		return err
	}
	if receipt == nil || receipt.BlockNumber == nil || receipt.BlockNumber.Uint64() > finalized {
		if allowRevert {
			_, err = w.sweepRepo.RevertConfirmationWithSettlement(ctx, rec.SweepID)
		}
		return err
	}
	fromAddr, err := senderOf(tx)
	if err != nil {
		return err
	}
	txEvent, ok, err := buildTransactionEvent(w.chain, tx, receipt, fromAddr)
	if err != nil || !ok {
		return err
	}
	if !matchesExecutionTarget(sweepExecutionTarget{rec: *rec}, chainEventToModel(txEvent)) {
		if allowRevert {
			_, err = w.sweepRepo.RevertConfirmationWithSettlement(ctx, rec.SweepID)
		}
		return err
	}
	if allowRevert {
		if _, err := w.sweepRepo.RevertConfirmationWithSettlement(ctx, rec.SweepID); err != nil {
			return err
		}
	}
	if txEvent.Success {
		_, err = w.sweepRepo.ConfirmWithSettlement(
			ctx,
			rec.SweepID,
			txEvent.BlockNumber,
			1,
			1,
			txEvent.AssetContractAddress,
			txEvent.Amount,
			txEvent.FeeAssetContractAddress,
			txEvent.FeeAmount,
		)
		return err
	}
	_, err = w.sweepRepo.FailWithSettlement(
		ctx,
		rec.SweepID,
		txEvent.AssetContractAddress,
		txEvent.Amount,
		txEvent.FeeAssetContractAddress,
		txEvent.FeeAmount,
	)
	return err
}
