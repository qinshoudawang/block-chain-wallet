package sweep

import (
	"context"
	"errors"
	"log"
	"math/big"
	"strings"
	"time"

	evmchain "wallet-system/internal/chain/evm"
	chainmodel "wallet-system/internal/storage/model/chain"
	sweepmodel "wallet-system/internal/storage/model/sweep"
	"wallet-system/internal/storage/repo"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
)

var erc20TransferTopic = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

type EVMTracker struct {
	chainRepo    *repo.ChainRepo
	sweepRepo    *repo.SweepRepo
	evm          *evmchain.Client
	chain        string
	confirmDepth uint64
	poll         time.Duration
	reorgCursor  string
}

func NewEVMTracker(chain string, confirmDepth uint64, chainRepo *repo.ChainRepo, sweepRepo *repo.SweepRepo, evm *evmchain.Client, poll time.Duration) *EVMTracker {
	return &EVMTracker{
		chainRepo:    chainRepo,
		sweepRepo:    sweepRepo,
		evm:          evm,
		chain:        strings.ToLower(strings.TrimSpace(chain)),
		confirmDepth: confirmDepth,
		poll:         poll,
		reorgCursor:  "sweep-reorg-tracker:" + strings.ToLower(strings.TrimSpace(chain)),
	}
}

func (w *EVMTracker) Run(ctx context.Context) {
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

func (w *EVMTracker) tick(ctx context.Context) {
	finalized, ok := finalizedHeight(ctx, w.evm, w.confirmDepth)
	if !ok {
		return
	}
	if err := w.handleReorgs(ctx, finalized); err != nil {
		log.Printf("[evm-sweep-tracker] handle reorgs failed chain=%s err=%v", w.chain, err)
		return
	}
	items, err := w.sweepRepo.ListBroadcastedToConfirm(ctx, 200)
	if err != nil {
		log.Printf("[evm-sweep-tracker] list broadcasted failed chain=%s err=%v", w.chain, err)
		return
	}
	for _, item := range items {
		if !strings.EqualFold(item.Chain, w.chain) {
			continue
		}
		if err := w.reconcileOrder(ctx, &item, finalized, false); err != nil {
			log.Printf("[evm-sweep-tracker] reconcile failed sweep_id=%s tx=%s err=%v", item.SweepID, item.TxHash, err)
		}
	}
}

func (w *EVMTracker) handleReorgs(ctx context.Context, finalized uint64) error {
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
	var maxID uint64 = cur.LastEventID
	for _, notice := range notices {
		if err := w.reconcileFromBlock(ctx, notice.FromBlock, finalized); err != nil {
			return err
		}
		if notice.ID > maxID {
			maxID = notice.ID
		}
	}
	return w.chainRepo.SaveTrackerCursor(ctx, w.reorgCursor, maxID)
}

func (w *EVMTracker) reconcileFromBlock(ctx context.Context, fromBlock uint64, finalized uint64) error {
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

func (w *EVMTracker) reconcileOrder(ctx context.Context, rec *sweepmodel.SweepOrder, finalized uint64, allowRevert bool) error {
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

type sweepExecutionTarget struct {
	rec sweepmodel.SweepOrder
}

func (t sweepExecutionTarget) GetSourceAddress() string        { return t.rec.FromAddress }
func (t sweepExecutionTarget) GetTargetAddress() string        { return t.rec.ToAddress }
func (t sweepExecutionTarget) GetAssetContractAddress() string { return t.rec.AssetContractAddress }
func (t sweepExecutionTarget) GetAmount() string               { return t.rec.Amount }

func finalizedHeight(ctx context.Context, evm *evmchain.Client, confirmations uint64) (uint64, bool) {
	if evm == nil {
		return 0, false
	}
	latest, err := evm.LatestHeight(ctx)
	if err != nil {
		log.Printf("[evm-sweep-tracker] latest height failed err=%v", err)
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

func buildTransactionEvent(chain string, tx *ethtypes.Transaction, receipt *ethtypes.Receipt, fromAddr string) (repo.ChainEventInput, bool, error) {
	if tx == nil || receipt == nil {
		return repo.ChainEventInput{}, false, nil
	}
	fee := big.NewInt(0)
	if receipt.EffectiveGasPrice != nil {
		fee = new(big.Int).Mul(new(big.Int).SetUint64(receipt.GasUsed), receipt.EffectiveGasPrice)
	}
	in := repo.ChainEventInput{
		Chain:                   chain,
		EventType:               chainmodel.EventTypeTransaction,
		BlockNumber:             receipt.BlockNumber.Uint64(),
		BlockHash:               receipt.BlockHash.Hex(),
		TxHash:                  receipt.TxHash.Hex(),
		TxIndex:                 uint(receipt.TransactionIndex),
		LogIndex:                0,
		FromAddress:             fromAddr,
		FeeAssetContractAddress: "",
		FeeAmount:               fee,
		Success:                 receipt.Status == ethtypes.ReceiptStatusSuccessful,
	}
	if tx.To() != nil && tx.Value() != nil && tx.Value().Sign() > 0 {
		in.ToAddress = tx.To().Hex()
		in.AssetContractAddress = ""
		in.Amount = new(big.Int).Set(tx.Value())
		return in, true, nil
	}
	tokenContract, toAddr, amount, ok, err := extractERC20Transfer(tx, receipt, fromAddr)
	if err != nil {
		return repo.ChainEventInput{}, false, err
	}
	if !ok {
		if !in.Success {
			tokenContract, toAddr, amount, ok = inferFailedERC20Transfer(tx)
			if !ok {
				return in, true, nil
			}
		} else {
			return repo.ChainEventInput{}, false, nil
		}
	}
	in.AssetContractAddress = tokenContract
	in.ToAddress = toAddr
	in.Amount = amount
	return in, true, nil
}

func senderOf(tx *ethtypes.Transaction) (string, error) {
	if tx == nil {
		return "", errors.New("transaction is nil")
	}
	signer := ethtypes.LatestSignerForChainID(tx.ChainId())
	from, err := ethtypes.Sender(signer, tx)
	if err != nil {
		return "", err
	}
	return from.Hex(), nil
}

func extractERC20Transfer(tx *ethtypes.Transaction, receipt *ethtypes.Receipt, fromAddr string) (string, string, *big.Int, bool, error) {
	for _, lg := range receipt.Logs {
		if lg == nil || len(lg.Topics) < 3 || len(lg.Data) != 32 {
			continue
		}
		if lg.Topics[0] != erc20TransferTopic {
			continue
		}
		logFrom := common.BytesToAddress(lg.Topics[1].Bytes()[12:]).Hex()
		if !strings.EqualFold(logFrom, fromAddr) {
			continue
		}
		amount := new(big.Int).SetBytes(lg.Data)
		if amount.Sign() <= 0 {
			continue
		}
		return lg.Address.Hex(), common.BytesToAddress(lg.Topics[2].Bytes()[12:]).Hex(), amount, true, nil
	}
	return "", "", nil, false, nil
}

func inferFailedERC20Transfer(tx *ethtypes.Transaction) (string, string, *big.Int, bool) {
	if tx == nil || tx.To() == nil {
		return "", "", nil, false
	}
	data := tx.Data()
	if len(data) != 4+32+32 {
		return "", "", nil, false
	}
	if data[0] != 0xa9 || data[1] != 0x05 || data[2] != 0x9c || data[3] != 0xbb {
		return "", "", nil, false
	}
	toAddr := common.BytesToAddress(data[4+12 : 4+32]).Hex()
	amount := new(big.Int).SetBytes(data[4+32:])
	if amount.Sign() < 0 {
		return "", "", nil, false
	}
	return tx.To().Hex(), toAddr, amount, true
}
