package btc

import (
	"context"
	"log"
	"math/big"
	"strings"
	"time"

	chainmodel "wallet-system/internal/storage/model/chain"
	"wallet-system/internal/storage/repo"

	"github.com/btcsuite/btcd/btcutil"
)

const btcIngressStream = "btc_ingress"

type IndexerConfig struct {
	Chain         string
	Confirmations uint64
	PollInterval  time.Duration
	BatchBlocks   uint64
	StartBlock    uint64
}

type Indexer struct {
	cfg         IndexerConfig
	btc         *Client
	repo        *repo.ChainRepo
	addressRepo *repo.AddressRepo
	depositRepo *repo.DepositRepo
}

func NewIndexer(chainRepo *repo.ChainRepo, depositRepo *repo.DepositRepo, addressRepo *repo.AddressRepo, client *Client, cfg IndexerConfig) *Indexer {
	return &Indexer{
		cfg:         cfg,
		btc:         client,
		repo:        chainRepo,
		addressRepo: addressRepo,
		depositRepo: depositRepo,
	}
}

func (s *Indexer) Run(ctx context.Context) {
	if s == nil || s.btc == nil || s.repo == nil || s.addressRepo == nil {
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
	finalized := latest
	if cur.NextBlockNumber > finalized {
		log.Printf("[chain-indexer-btc] idle chain=%s cursor=%d latest=%d", s.cfg.Chain, cur.NextBlockNumber, latest)
		return
	}
	batch := s.cfg.BatchBlocks
	if batch == 0 {
		batch = 20
	}
	from := cur.NextBlockNumber
	to := finalized
	if to-from+1 > batch {
		to = from + batch - 1
	}
	addressMap, err := s.loadDepositAddressMap(ctx)
	if err != nil {
		log.Printf("[chain-indexer-btc] load deposit addresses failed chain=%s err=%v", s.cfg.Chain, err)
		return
	}
	if err := s.indexRange(ctx, from, to, addressMap); err != nil {
		log.Printf("[chain-indexer-btc] index range failed chain=%s from=%d to=%d err=%v", s.cfg.Chain, from, to, err)
		return
	}
	if err := s.repo.SaveCursor(ctx, s.cfg.Chain, btcIngressStream, to+1); err != nil {
		log.Printf("[chain-indexer-btc] save cursor failed chain=%s next=%d err=%v", s.cfg.Chain, to+1, err)
		return
	}
	log.Printf("[chain-indexer-btc] indexed chain=%s from=%d to=%d", s.cfg.Chain, from, to)
}

func (s *Indexer) prepareState(ctx context.Context) (*chainmodel.IndexCursor, uint64, bool) {
	cur, err := s.repo.GetOrCreateCursor(ctx, s.cfg.Chain, btcIngressStream, s.cfg.StartBlock)
	if err != nil {
		log.Printf("[chain-indexer-btc] load cursor failed chain=%s err=%v", s.cfg.Chain, err)
		return nil, 0, false
	}
	latest, err := s.btc.LatestHeightContext(ctx)
	if err != nil {
		log.Printf("[chain-indexer-btc] latest height failed chain=%s err=%v", s.cfg.Chain, err)
		return nil, 0, false
	}
	return cur, latest, true
}

func (s *Indexer) handleReorgIfNeeded(ctx context.Context, cursor uint64) bool {
	reorgFrom, err := s.detectReorgFromCursor(ctx, cursor)
	if err != nil {
		log.Printf("[chain-indexer-btc] detect reorg failed chain=%s err=%v", s.cfg.Chain, err)
		return false
	}
	if reorgFrom == 0 {
		return true
	}
	if err := s.repo.RevertFromBlock(ctx, s.cfg.Chain, reorgFrom); err != nil {
		log.Printf("[chain-indexer-btc] revert from block failed chain=%s from=%d err=%v", s.cfg.Chain, reorgFrom, err)
		return false
	}
	if err := s.repo.DeleteBTCBlocksFrom(ctx, s.cfg.Chain, reorgFrom); err != nil {
		log.Printf("[chain-indexer-btc] delete reverted btc blocks failed chain=%s from=%d err=%v", s.cfg.Chain, reorgFrom, err)
		return false
	}
	if err := s.repo.SaveCursor(ctx, s.cfg.Chain, btcIngressStream, reorgFrom); err != nil {
		log.Printf("[chain-indexer-btc] rewind cursor failed chain=%s from=%d err=%v", s.cfg.Chain, reorgFrom, err)
		return false
	}
	log.Printf("[chain-indexer-btc] matched reorg chain=%s rewind_to=%d", s.cfg.Chain, reorgFrom)
	return true
}

func (s *Indexer) detectReorgFromCursor(ctx context.Context, cursor uint64) (uint64, error) {
	if cursor == 0 {
		return 0, nil
	}
	check := cursor - 1
	localHash, ok, err := s.repo.GetBTCBlockHash(ctx, s.cfg.Chain, check)
	if err != nil || !ok {
		return 0, err
	}
	block, err := s.btc.GetBlockByHeight(ctx, check)
	if err != nil {
		return 0, err
	}
	if strings.EqualFold(localHash, block.Hash) {
		return 0, nil
	}
	for b := check; ; b-- {
		h, ok, err := s.repo.GetBTCBlockHash(ctx, s.cfg.Chain, b)
		if err != nil {
			return 0, err
		}
		if !ok {
			return b + 1, nil
		}
		block, err := s.btc.GetBlockByHeight(ctx, b)
		if err != nil {
			return 0, err
		}
		if strings.EqualFold(h, block.Hash) {
			return b + 1, nil
		}
		if b == 0 {
			return 0, nil
		}
	}
}

func (s *Indexer) indexRange(ctx context.Context, from uint64, to uint64, depositAddressMap map[string]string) error {
	for n := from; n <= to; n++ {
		block, err := s.btc.GetBlockByHeight(ctx, n)
		if err != nil {
			return err
		}
		log.Printf("[chain-indexer-btc] scanning block chain=%s height=%d txs=%d hash=%s",
			s.cfg.Chain, n, len(block.TxIDs), block.Hash)
		if err := s.repo.UpsertBTCBlock(ctx, s.cfg.Chain, n, block.Hash, block.ParentHash); err != nil {
			return err
		}
		for _, txid := range block.TxIDs {
			tx, err := s.btc.GetTransaction(ctx, txid)
			if err != nil {
				return err
			}
			for voutIndex := range tx.Vout {
				vout := tx.Vout[voutIndex]
				toAddr := strings.TrimSpace(vout.ScriptPubKey.Address)
				if toAddr == "" {
					continue
				}
				userID, ok := depositAddressMap[strings.ToLower(toAddr)]
				if !ok {
					continue
				}
				sat, err := btcFloatToSatString(vout.Value)
				if err != nil || sat.Sign() <= 0 {
					continue
				}
				err = s.depositRepo.CreatePending(ctx, repo.DepositUpsertInput{
					Chain:                s.cfg.Chain,
					TxHash:               tx.Txid,
					LogIndex:             uint(voutIndex),
					BlockNumber:          n,
					TokenContractAddress: "",
					UserID:               userID,
					FromAddress:          "",
					ToAddress:            toAddr,
					Amount:               sat,
				})
				if err != nil {
					return err
				}
				log.Printf("[chain-indexer-btc] matched deposit chain=%s height=%d tx=%s vout=%d to=%s amount=%s",
					s.cfg.Chain, n, tx.Txid, voutIndex, toAddr, sat.String())
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

func btcFloatToSatString(v float64) (*big.Int, error) {
	sat, err := btcutil.NewAmount(v)
	if err != nil {
		return nil, err
	}
	return big.NewInt(int64(sat)), nil
}
