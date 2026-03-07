package chainindex

import (
	"context"
	"time"

	chainmodel "wallet-system/internal/storage/model/chain"
	withdrawmodel "wallet-system/internal/storage/model/withdraw"
	"wallet-system/internal/storage/repo"
)

type WithdrawProjector struct {
	chainRepo    *repo.ChainRepo
	withdrawRepo *repo.WithdrawRepo
	ledgerRepo   *repo.LedgerRepo
	chain        string
	cursorName   string
	poll         time.Duration
}

func NewWithdrawProjector(chain string, chainRepo *repo.ChainRepo, withdrawRepo *repo.WithdrawRepo, ledgerRepo *repo.LedgerRepo, poll time.Duration) *WithdrawProjector {
	return &WithdrawProjector{
		chainRepo:    chainRepo,
		withdrawRepo: withdrawRepo,
		ledgerRepo:   ledgerRepo,
		chain:        chain,
		cursorName:   "withdraw-projector:" + chain,
		poll:         poll,
	}
}

func (p *WithdrawProjector) Run(ctx context.Context) {
	if p == nil || p.chainRepo == nil || p.withdrawRepo == nil {
		return
	}
	runProjector(ctx, projectorConfig{
		name:      p.cursorName,
		chain:     p.chain,
		eventType: chainmodel.EventTypeTransaction,
		poll:      p.poll,
		repo:      p.chainRepo,
		handle:    p.applyEvent,
	})
}

func (p *WithdrawProjector) applyEvent(ctx context.Context, ev chainmodel.ChainEvent) error {
	rec, ok, err := p.withdrawRepo.GetTrackedByTxHash(ctx, ev.Chain, ev.TxHash)
	if err != nil || !ok {
		return err
	}
	if !matchesExecutionTarget(withdrawExecutionTarget{rec: *rec}, ev) {
		return nil
	}
	switch ev.Action {
	case chainmodel.EventActionApply:
		if ev.Success {
			_, err = p.withdrawRepo.ConfirmWithSettlement(
				ctx,
				p.ledgerRepo,
				rec.WithdrawID,
				ev.BlockNumber,
				1,
				1,
				ev.AssetContractAddress,
				amountFromString(ev.Amount),
				ev.FeeAssetContractAddress,
				amountFromString(ev.FeeAmount),
			)
			return err
		}
		_, err = p.withdrawRepo.FailWithSettlement(
			ctx,
			p.ledgerRepo,
			rec.WithdrawID,
			ev.AssetContractAddress,
			amountFromString(ev.Amount),
			ev.FeeAssetContractAddress,
			amountFromString(ev.FeeAmount),
		)
		return err
	case chainmodel.EventActionRevert:
		_, err = p.withdrawRepo.RevertConfirmationWithSettlement(ctx, p.ledgerRepo, rec.WithdrawID)
		return err
	default:
		return nil
	}
}

type withdrawExecutionTarget struct {
	rec withdrawmodel.WithdrawOrder
}

func (t withdrawExecutionTarget) GetSourceAddress() string        { return t.rec.FromAddr }
func (t withdrawExecutionTarget) GetTargetAddress() string        { return t.rec.ToAddr }
func (t withdrawExecutionTarget) GetAssetContractAddress() string { return t.rec.TokenContractAddress }
func (t withdrawExecutionTarget) GetAmount() string               { return t.rec.Amount }
