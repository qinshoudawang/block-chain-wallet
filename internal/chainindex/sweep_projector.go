package chainindex

import (
	"context"
	"time"

	chainmodel "wallet-system/internal/storage/model/chain"
	sweepmodel "wallet-system/internal/storage/model/sweep"
	"wallet-system/internal/storage/repo"
)

type SweepProjector struct {
	chainRepo  *repo.ChainRepo
	sweepRepo  *repo.SweepRepo
	chain      string
	cursorName string
	poll       time.Duration
}

func NewSweepProjector(chain string, chainRepo *repo.ChainRepo, sweepRepo *repo.SweepRepo, poll time.Duration) *SweepProjector {
	return &SweepProjector{
		chainRepo:  chainRepo,
		sweepRepo:  sweepRepo,
		chain:      chain,
		cursorName: "sweep-projector:" + chain,
		poll:       poll,
	}
}

func (p *SweepProjector) Run(ctx context.Context) {
	if p == nil || p.chainRepo == nil || p.sweepRepo == nil {
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

func (p *SweepProjector) applyEvent(ctx context.Context, ev chainmodel.ChainEvent) error {
	rec, ok, err := p.sweepRepo.GetTrackedByTxHash(ctx, ev.Chain, ev.TxHash)
	if err != nil || !ok {
		return err
	}
	if !matchesExecutionTarget(sweepExecutionTarget{rec: *rec}, ev) {
		return nil
	}
	switch ev.Action {
	case chainmodel.EventActionApply:
		if ev.Success {
			_, err = p.sweepRepo.ConfirmWithSettlement(
				ctx,
				rec.SweepID,
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
		_, err = p.sweepRepo.FailWithSettlement(
			ctx,
			rec.SweepID,
			ev.AssetContractAddress,
			amountFromString(ev.Amount),
			ev.FeeAssetContractAddress,
			amountFromString(ev.FeeAmount),
		)
		return err
	case chainmodel.EventActionRevert:
		_, err = p.sweepRepo.RevertConfirmationWithSettlement(ctx, rec.SweepID)
		return err
	default:
		return nil
	}
}

type sweepExecutionTarget struct {
	rec sweepmodel.SweepOrder
}

func (t sweepExecutionTarget) GetSourceAddress() string        { return t.rec.FromAddress }
func (t sweepExecutionTarget) GetTargetAddress() string        { return t.rec.ToAddress }
func (t sweepExecutionTarget) GetAssetContractAddress() string { return t.rec.AssetContractAddress }
func (t sweepExecutionTarget) GetAmount() string               { return t.rec.Amount }
