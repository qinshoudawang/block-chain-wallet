package evmindex

import (
	"context"
	"errors"
	"math/big"
	"time"

	chainmodel "wallet-system/internal/storage/model/chain"
	"wallet-system/internal/storage/repo"

	"gorm.io/gorm"
)

type EVMDepositProjector struct {
	chainRepo   *repo.ChainRepo
	depositRepo *repo.DepositRepo
	addressRepo *repo.AddressRepo
	chain       string
	cursorName  string
	poll        time.Duration
}

func NewEVMDepositProjector(chain string, chainRepo *repo.ChainRepo, depositRepo *repo.DepositRepo, addressRepo *repo.AddressRepo, poll time.Duration) *EVMDepositProjector {
	return &EVMDepositProjector{
		chainRepo:   chainRepo,
		depositRepo: depositRepo,
		addressRepo: addressRepo,
		chain:       chain,
		cursorName:  "evm-deposit-projector:" + chain,
		poll:        poll,
	}
}

func (p *EVMDepositProjector) Run(ctx context.Context) {
	if p == nil || p.chainRepo == nil || p.depositRepo == nil || p.addressRepo == nil {
		return
	}
	runProjector(ctx, projectorConfig{
		name:      p.cursorName,
		chain:     p.chain,
		eventType: chainmodel.EventTypeTransfer,
		poll:      p.poll,
		repo:      p.chainRepo,
		handle:    p.applyEvent,
	})
}

func (p *EVMDepositProjector) applyEvent(ctx context.Context, ev chainmodel.ChainEvent) error {
	userID, err := p.addressRepo.FindUserIDByChainAddress(ctx, ev.Chain, ev.ToAddress)
	if err != nil {
		if ev.Action == chainmodel.EventActionRevert {
			return p.depositRepo.RevertByChainRef(ctx, ev.Chain, ev.TxHash, ev.LogIndex)
		}
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}
	switch ev.Action {
	case chainmodel.EventActionApply:
		return p.depositRepo.CreateConfirmedAndCredit(ctx, repo.DepositUpsertInput{
			Chain:                ev.Chain,
			TxHash:               ev.TxHash,
			LogIndex:             ev.LogIndex,
			BlockNumber:          ev.BlockNumber,
			TokenContractAddress: ev.AssetContractAddress,
			UserID:               userID,
			FromAddress:          ev.FromAddress,
			ToAddress:            ev.ToAddress,
			Amount:               amountFromString(ev.Amount),
		})
	case chainmodel.EventActionRevert:
		return p.depositRepo.RevertByChainRef(ctx, ev.Chain, ev.TxHash, ev.LogIndex)
	default:
		return nil
	}
}

func amountFromString(v string) *big.Int {
	out, ok := new(big.Int).SetString(v, 10)
	if !ok {
		return big.NewInt(0)
	}
	return out
}
