package evmindex

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	evmchain "wallet-system/internal/chain/evm"
	chainmodel "wallet-system/internal/storage/model/chain"
	withdrawmodel "wallet-system/internal/storage/model/withdraw"
	"wallet-system/internal/storage/repo"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
)

type EVMWithdrawProjector struct {
	chainRepo    *repo.ChainRepo
	withdrawRepo *repo.WithdrawRepo
	ledgerRepo   *repo.LedgerRepo
	evm          *evmchain.Client
	chain        string
	confirmDepth uint64
	poll         time.Duration
	reorgCursor  string
}

func NewEVMWithdrawProjector(chain string, confirmDepth uint64, chainRepo *repo.ChainRepo, withdrawRepo *repo.WithdrawRepo, ledgerRepo *repo.LedgerRepo, evm *evmchain.Client, poll time.Duration) *EVMWithdrawProjector {
	return &EVMWithdrawProjector{
		chainRepo:    chainRepo,
		withdrawRepo: withdrawRepo,
		ledgerRepo:   ledgerRepo,
		evm:          evm,
		chain:        strings.ToLower(strings.TrimSpace(chain)),
		confirmDepth: confirmDepth,
		poll:         poll,
		reorgCursor:  "withdraw-reorg-projector:" + strings.ToLower(strings.TrimSpace(chain)),
	}
}

func (w *EVMWithdrawProjector) Run(ctx context.Context) {
	if w == nil || w.chainRepo == nil || w.withdrawRepo == nil || w.evm == nil {
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

func (w *EVMWithdrawProjector) tick(ctx context.Context) {
	finalized, ok := finalizedHeight(ctx, w.evm, w.confirmDepth)
	if !ok {
		return
	}
	if err := w.handleReorgs(ctx, finalized); err != nil {
		log.Printf("[evm-withdraw-projector] handle reorgs failed chain=%s err=%v", w.chain, err)
		return
	}
	items, err := w.withdrawRepo.ListBroadcastedToConfirm(ctx, 200)
	if err != nil {
		log.Printf("[evm-withdraw-projector] list broadcasted failed chain=%s err=%v", w.chain, err)
		return
	}
	for _, item := range items {
		if !strings.EqualFold(item.Chain, w.chain) {
			continue
		}
		if err := w.reconcileOrder(ctx, &item, finalized, false); err != nil {
			log.Printf("[evm-withdraw-projector] reconcile failed withdraw_id=%s tx=%s err=%v", item.WithdrawID, item.TxHash, err)
		}
	}
}

func (w *EVMWithdrawProjector) handleReorgs(ctx context.Context, finalized uint64) error {
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

func (w *EVMWithdrawProjector) reconcileFromBlock(ctx context.Context, fromBlock uint64, finalized uint64) error {
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
			if err := w.reconcileOrder(ctx, &item, finalized, true); err != nil {
				return err
			}
		}
	}
}

func (w *EVMWithdrawProjector) reconcileOrder(ctx context.Context, rec *withdrawmodel.WithdrawOrder, finalized uint64, allowRevert bool) error {
	if rec == nil || strings.TrimSpace(rec.TxHash) == "" {
		return nil
	}
	txHash := common.HexToHash(strings.TrimSpace(rec.TxHash))
	tx, _, err := w.evm.TransactionByHash(ctx, txHash)
	if err != nil {
		if errors.Is(err, ethereum.NotFound) {
			if allowRevert {
				_, err = w.withdrawRepo.RevertConfirmationWithSettlement(ctx, w.ledgerRepo, rec.WithdrawID)
			}
			return err
		}
		return err
	}
	receipt, err := w.evm.TransactionReceipt(ctx, txHash)
	if err != nil {
		if errors.Is(err, ethereum.NotFound) {
			if allowRevert {
				_, err = w.withdrawRepo.RevertConfirmationWithSettlement(ctx, w.ledgerRepo, rec.WithdrawID)
			}
			return err
		}
		return err
	}
	if receipt == nil || receipt.BlockNumber == nil || receipt.BlockNumber.Uint64() > finalized {
		if allowRevert {
			_, err = w.withdrawRepo.RevertConfirmationWithSettlement(ctx, w.ledgerRepo, rec.WithdrawID)
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
	if !matchesExecutionTarget(withdrawExecutionTarget{rec: *rec}, chainEventToModel(txEvent)) {
		if allowRevert {
			_, err = w.withdrawRepo.RevertConfirmationWithSettlement(ctx, w.ledgerRepo, rec.WithdrawID)
		}
		return err
	}
	if allowRevert {
		if _, err := w.withdrawRepo.RevertConfirmationWithSettlement(ctx, w.ledgerRepo, rec.WithdrawID); err != nil {
			return err
		}
	}
	if txEvent.Success {
		_, err = w.withdrawRepo.ConfirmWithSettlement(
			ctx,
			w.ledgerRepo,
			rec.WithdrawID,
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
	_, err = w.withdrawRepo.FailWithSettlement(
		ctx,
		w.ledgerRepo,
		rec.WithdrawID,
		txEvent.AssetContractAddress,
		txEvent.Amount,
		txEvent.FeeAssetContractAddress,
		txEvent.FeeAmount,
	)
	return err
}

func finalizedHeight(ctx context.Context, evm *evmchain.Client, confirmations uint64) (uint64, bool) {
	if evm == nil {
		return 0, false
	}
	latest, err := evm.LatestHeight(ctx)
	if err != nil {
		log.Printf("[evm-tx-projector] latest height failed err=%v", err)
		return 0, false
	}
	if latest < confirmations {
		return 0, false
	}
	return latest - confirmations, true
}

func chainEventToModel(in repo.ChainEventInput) chainmodel.ChainEvent {
	amount := "0"
	if in.Amount != nil {
		amount = in.Amount.String()
	}
	fee := "0"
	if in.FeeAmount != nil {
		fee = in.FeeAmount.String()
	}
	return chainmodel.ChainEvent{
		Chain:                   in.Chain,
		EventType:               in.EventType,
		Action:                  chainmodel.EventActionApply,
		BlockNumber:             in.BlockNumber,
		BlockHash:               in.BlockHash,
		TxHash:                  in.TxHash,
		TxIndex:                 in.TxIndex,
		LogIndex:                in.LogIndex,
		AssetContractAddress:    in.AssetContractAddress,
		FromAddress:             in.FromAddress,
		ToAddress:               in.ToAddress,
		Amount:                  amount,
		FeeAssetContractAddress: in.FeeAssetContractAddress,
		FeeAmount:               fee,
		Success:                 in.Success,
	}
}

type executionMatchTarget interface {
	GetSourceAddress() string
	GetTargetAddress() string
	GetAssetContractAddress() string
	GetAmount() string
}

func matchesExecutionTarget(target executionMatchTarget, ev chainmodel.ChainEvent) bool {
	if !strings.EqualFold(strings.TrimSpace(target.GetSourceAddress()), strings.TrimSpace(ev.FromAddress)) {
		return false
	}
	if to := strings.TrimSpace(target.GetTargetAddress()); to != "" && !strings.EqualFold(to, strings.TrimSpace(ev.ToAddress)) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(target.GetAssetContractAddress()), strings.TrimSpace(ev.AssetContractAddress)) {
		return false
	}
	if ev.Success && strings.TrimSpace(target.GetAmount()) != strings.TrimSpace(ev.Amount) {
		return false
	}
	return true
}

type withdrawExecutionTarget struct {
	rec withdrawmodel.WithdrawOrder
}

func (t withdrawExecutionTarget) GetSourceAddress() string        { return t.rec.FromAddr }
func (t withdrawExecutionTarget) GetTargetAddress() string        { return t.rec.ToAddr }
func (t withdrawExecutionTarget) GetAssetContractAddress() string { return t.rec.TokenContractAddress }
func (t withdrawExecutionTarget) GetAmount() string               { return t.rec.Amount }
