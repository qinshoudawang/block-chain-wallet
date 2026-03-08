package solindex

import (
	"context"
	"log"
	"math/big"
	"strings"
	"time"

	solchain "wallet-system/internal/chain/sol"
	"wallet-system/internal/storage/repo"
)

type DepositProjector struct {
	chainRepo     *repo.ChainRepo
	depositRepo   *repo.DepositRepo
	sol           *solchain.Client
	chain         string
	poll          time.Duration
	confirmations uint64
	reorgCursor   string
}

func NewDepositProjector(chain string, confirmations uint64, chainRepo *repo.ChainRepo, depositRepo *repo.DepositRepo, sol *solchain.Client, poll time.Duration) *DepositProjector {
	chain = strings.ToLower(strings.TrimSpace(chain))
	return &DepositProjector{
		chainRepo:     chainRepo,
		depositRepo:   depositRepo,
		sol:           sol,
		chain:         chain,
		poll:          poll,
		confirmations: confirmations,
		reorgCursor:   "deposit-reorg-projector:" + chain,
	}
}

func (p *DepositProjector) Run(ctx context.Context) {
	if p == nil || p.chainRepo == nil || p.depositRepo == nil || p.sol == nil {
		return
	}
	poll := p.poll
	if poll <= 0 {
		poll = 3 * time.Second
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()

	p.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.tick(ctx)
		}
	}
}

func (p *DepositProjector) tick(ctx context.Context) {
	latest, err := p.sol.LatestHeight(ctx)
	if err != nil {
		return
	}
	if err := p.handleReorgs(ctx); err != nil {
		log.Printf("[deposit-projector-sol] handle reorgs failed chain=%s err=%v", p.chain, err)
		return
	}
	maxBlock, ok := confirmableDepositBlock(latest, p.confirmations)
	if !ok {
		return
	}
	items, err := p.depositRepo.ListPendingConfirmable(ctx, p.chain, maxBlock, 500)
	if err != nil {
		return
	}
	for i := range items {
		rec := items[i]
		amount := amountFromString(rec.Amount)
		if amount.Sign() <= 0 {
			continue
		}
		err := p.depositRepo.ConfirmPendingAndCredit(ctx, repo.DepositUpsertInput{
			Chain:                rec.Chain,
			TxHash:               rec.TxHash,
			LogIndex:             rec.LogIndex,
			BlockNumber:          rec.BlockNumber,
			TokenContractAddress: rec.TokenContractAddress,
			UserID:               rec.UserID,
			FromAddress:          rec.FromAddress,
			ToAddress:            rec.ToAddress,
			Amount:               amount,
		})
		if err == nil {
			log.Printf("[deposit-projector-sol] confirmed deposit chain=%s tx=%s account_index=%d to=%s amount=%s",
				rec.Chain, rec.TxHash, rec.LogIndex, rec.ToAddress, rec.Amount)
		}
	}
}

func (p *DepositProjector) handleReorgs(ctx context.Context) error {
	cur, err := p.chainRepo.GetOrCreateProjectorCursor(ctx, p.reorgCursor)
	if err != nil {
		return err
	}
	notices, err := p.chainRepo.ListReorgNoticesAfterID(ctx, p.chain, cur.LastEventID, 100)
	if err != nil {
		return err
	}
	if len(notices) == 0 {
		return nil
	}
	maxID := cur.LastEventID
	for _, notice := range notices {
		if err := p.revertFromBlock(ctx, notice.FromBlock); err != nil {
			return err
		}
		if notice.ID > maxID {
			maxID = notice.ID
		}
	}
	return p.chainRepo.SaveProjectorCursor(ctx, p.reorgCursor, maxID)
}

func (p *DepositProjector) revertFromBlock(ctx context.Context, fromBlock uint64) error {
	var lastID uint64
	for {
		items, err := p.depositRepo.ListReorgAffectedAfterID(ctx, p.chain, fromBlock, lastID, 500)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}
		for i := range items {
			rec := items[i]
			lastID = rec.ID
			if err := p.depositRepo.RevertByChainRef(ctx, rec.Chain, rec.TxHash, rec.LogIndex); err != nil {
				return err
			}
			log.Printf("[deposit-projector-sol] reverted deposit chain=%s tx=%s account_index=%d",
				rec.Chain, rec.TxHash, rec.LogIndex)
		}
	}
}

func amountFromString(v string) *big.Int {
	out, ok := new(big.Int).SetString(v, 10)
	if !ok {
		return big.NewInt(0)
	}
	return out
}

func confirmableDepositBlock(latest uint64, confirmations uint64) (uint64, bool) {
	if confirmations <= 1 {
		return latest, true
	}
	if latest+1 < confirmations {
		return 0, false
	}
	return latest - confirmations + 1, true
}
