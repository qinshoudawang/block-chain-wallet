package sol

import (
	"context"
	"log"
	"math/big"
	"strings"
	"time"

	chainmodel "wallet-system/internal/storage/model/chain"
	"wallet-system/internal/storage/repo"
)

const solIngressStream = "sol_ingress"

type IndexerConfig struct {
	Chain        string
	PollInterval time.Duration
	BatchBlocks  uint64
	StartBlock   uint64
}

type Indexer struct {
	cfg         IndexerConfig
	sol         *Client
	repo        *repo.ChainRepo
	depositRepo *repo.DepositRepo
	addressRepo *repo.AddressRepo
}

func NewIndexer(chainRepo *repo.ChainRepo, depositRepo *repo.DepositRepo, addressRepo *repo.AddressRepo, client *Client, cfg IndexerConfig) *Indexer {
	return &Indexer{
		cfg:         cfg,
		sol:         client,
		repo:        chainRepo,
		depositRepo: depositRepo,
		addressRepo: addressRepo,
	}
}

func (s *Indexer) Run(ctx context.Context) {
	if s == nil || s.sol == nil || s.repo == nil || s.depositRepo == nil || s.addressRepo == nil {
		return
	}
	poll := s.cfg.PollInterval
	if poll <= 0 {
		poll = 15 * time.Second
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()

	s.scanOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.scanOnce(ctx)
		}
	}
}

func (s *Indexer) scanOnce(ctx context.Context) {
	cur, latest, ok := s.prepareState(ctx)
	if !ok {
		return
	}
	if !s.handleReorgIfNeeded(ctx, cur.NextBlockNumber) {
		return
	}
	if cur.NextBlockNumber > latest {
		return
	}
	batch := s.cfg.BatchBlocks
	if batch == 0 {
		batch = 100
	}
	from := cur.NextBlockNumber
	to := latest
	if to-from+1 > batch {
		to = from + batch - 1
	}
	addressMap, err := s.loadDepositAddressMap(ctx)
	if err != nil {
		log.Printf("[chain-indexer-sol] load deposit addresses failed chain=%s err=%v", s.cfg.Chain, err)
		return
	}
	if err := s.indexRange(ctx, from, to, addressMap); err != nil {
		log.Printf("[chain-indexer-sol] index range failed chain=%s from=%d to=%d err=%v", s.cfg.Chain, from, to, err)
		return
	}
	if err := s.repo.SaveCursor(ctx, s.cfg.Chain, solIngressStream, to+1); err != nil {
		log.Printf("[chain-indexer-sol] save cursor failed chain=%s next=%d err=%v", s.cfg.Chain, to+1, err)
		return
	}
	log.Printf("[chain-indexer-sol] indexed chain=%s from=%d to=%d", s.cfg.Chain, from, to)
}

func (s *Indexer) prepareState(ctx context.Context) (*chainmodel.IndexCursor, uint64, bool) {
	cur, err := s.repo.GetOrCreateCursor(ctx, s.cfg.Chain, solIngressStream, s.cfg.StartBlock)
	if err != nil {
		log.Printf("[chain-indexer-sol] load cursor failed chain=%s err=%v", s.cfg.Chain, err)
		return nil, 0, false
	}
	latest, err := s.sol.LatestHeight(ctx)
	if err != nil {
		log.Printf("[chain-indexer-sol] latest slot failed chain=%s err=%v", s.cfg.Chain, err)
		return nil, 0, false
	}
	return cur, latest, true
}

func (s *Indexer) handleReorgIfNeeded(ctx context.Context, cursor uint64) bool {
	reorgFrom, err := s.detectReorgFromCursor(ctx, cursor)
	if err != nil {
		log.Printf("[chain-indexer-sol] detect reorg failed chain=%s err=%v", s.cfg.Chain, err)
		return false
	}
	if reorgFrom == 0 {
		return true
	}
	if err := s.repo.RevertFromBlock(ctx, s.cfg.Chain, reorgFrom); err != nil {
		log.Printf("[chain-indexer-sol] revert from slot failed chain=%s from=%d err=%v", s.cfg.Chain, reorgFrom, err)
		return false
	}
	if err := s.repo.DeleteSOLBlocksFrom(ctx, s.cfg.Chain, reorgFrom); err != nil {
		log.Printf("[chain-indexer-sol] delete reverted sol blocks failed chain=%s from=%d err=%v", s.cfg.Chain, reorgFrom, err)
		return false
	}
	if err := s.repo.SaveCursor(ctx, s.cfg.Chain, solIngressStream, reorgFrom); err != nil {
		log.Printf("[chain-indexer-sol] rewind cursor failed chain=%s from=%d err=%v", s.cfg.Chain, reorgFrom, err)
		return false
	}
	log.Printf("[chain-indexer-sol] matched reorg chain=%s rewind_to=%d", s.cfg.Chain, reorgFrom)
	return true
}

func (s *Indexer) detectReorgFromCursor(ctx context.Context, cursor uint64) (uint64, error) {
	if cursor == 0 {
		return 0, nil
	}
	check := cursor - 1
	localHash, ok, err := s.repo.GetSOLBlockHash(ctx, s.cfg.Chain, check)
	if err != nil || !ok {
		return 0, err
	}
	block, err := s.sol.GetBlock(ctx, check)
	if err != nil {
		return 0, err
	}
	if strings.EqualFold(localHash, BlockHashString(block.Blockhash)) {
		return 0, nil
	}
	for b := check; ; b-- {
		h, ok, err := s.repo.GetSOLBlockHash(ctx, s.cfg.Chain, b)
		if err != nil {
			return 0, err
		}
		if !ok {
			return b + 1, nil
		}
		block, err := s.sol.GetBlock(ctx, b)
		if err != nil {
			return 0, err
		}
		if strings.EqualFold(h, BlockHashString(block.Blockhash)) {
			return b + 1, nil
		}
		if b == 0 {
			return 0, nil
		}
	}
}

func (s *Indexer) indexRange(ctx context.Context, from uint64, to uint64, addressMap map[string]string) error {
	for slot := from; slot <= to; slot++ {
		block, err := s.sol.GetBlock(ctx, slot)
		if err != nil {
			return err
		}
		parentHash, err := ParentHashFromBlock(block)
		if err != nil {
			return err
		}
		if err := s.repo.UpsertSOLBlock(ctx, s.cfg.Chain, slot, BlockHashString(block.Blockhash), parentHash); err != nil {
			return err
		}
		for _, item := range block.Transactions {
			if item.Meta == nil || item.Meta.Err != nil {
				continue
			}
			tx, err := item.GetTransaction()
			if err != nil || tx == nil || len(tx.Signatures) == 0 {
				continue
			}
			keys := tx.Message.AccountKeys
			if len(keys) == 0 || len(item.Meta.PreBalances) != len(keys) || len(item.Meta.PostBalances) != len(keys) {
				continue
			}
			fromAddr := keys[0].String()
			txHash := tx.Signatures[0].String()
			for idx := range keys {
				toAddr := keys[idx].String()
				userID, ok := addressMap[strings.ToLower(strings.TrimSpace(toAddr))]
				if !ok {
					continue
				}
				pre := item.Meta.PreBalances[idx]
				post := item.Meta.PostBalances[idx]
				if post <= pre {
					continue
				}
				amount := new(big.Int).SetUint64(post - pre)
				if amount.Sign() <= 0 {
					continue
				}
				if err := s.depositRepo.CreatePending(ctx, repo.DepositUpsertInput{
					Chain:                s.cfg.Chain,
					TxHash:               txHash,
					LogIndex:             uint(idx),
					BlockNumber:          slot,
					TokenContractAddress: "",
					UserID:               userID,
					FromAddress:          fromAddr,
					ToAddress:            toAddr,
					Amount:               amount,
				}); err != nil {
					return err
				}
				log.Printf("[deposit-tracker-sol] matched pending deposit chain=%s slot=%d tx=%s account_index=%d to=%s amount=%s",
					s.cfg.Chain, slot, txHash, idx, toAddr, amount.String())
			}
		}
	}
	return nil
}

func (s *Indexer) loadDepositAddressMap(ctx context.Context) (map[string]string, error) {
	rows, err := s.addressRepo.ListByChain(ctx, s.cfg.Chain)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(rows))
	for i := range rows {
		addr := strings.ToLower(strings.TrimSpace(rows[i].Address))
		if addr == "" {
			continue
		}
		out[addr] = strings.TrimSpace(rows[i].UserID)
	}
	return out, nil
}
