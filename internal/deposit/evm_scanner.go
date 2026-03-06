package deposit

import (
	"context"
	"errors"
	"log"
	"math/big"
	"strings"
	"time"

	evmchain "wallet-system/internal/chain/evm"
	depositmodel "wallet-system/internal/storage/model/deposit"
	"wallet-system/internal/storage/repo"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"gorm.io/gorm"
)

var erc20TransferTopic = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

type EVMScannerConfig struct {
	Chain           string
	TokenContracts  []string
	Confirmations   uint64
	PollInterval    time.Duration
	BatchBlocks     uint64
	StartBlock      uint64
	EnableSubscribe bool
}

type EVMScanner struct {
	cfg         EVMScannerConfig
	client      *evmchain.Client
	addressRepo *repo.AddressRepo
	depositRepo *repo.DepositRepo
}

func NewEVMScanner(db *gorm.DB, client *evmchain.Client, cfg EVMScannerConfig) *EVMScanner {
	return &EVMScanner{
		cfg:         cfg,
		client:      client,
		addressRepo: repo.NewAddressRepo(db),
		depositRepo: repo.NewDepositRepo(db),
	}
}

func (s *EVMScanner) Run(ctx context.Context) {
	if s == nil || s.client == nil {
		return
	}
	if len(s.cfg.TokenContracts) == 0 {
		log.Printf("[deposit-evm] scanner disabled chain=%s reason=no token contracts", s.cfg.Chain)
		return
	}
	poll := s.cfg.PollInterval
	if poll <= 0 {
		poll = 8 * time.Second
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	if s.cfg.EnableSubscribe {
		go s.runSubscribeLoop(ctx)
	}

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

func (s *EVMScanner) runSubscribeLoop(ctx context.Context) {
	query := s.transferFilterQuery(nil, nil)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		logCh := make(chan types.Log, 256)
		sub, err := s.client.SubscribeFilterLogs(ctx, query, logCh)
		if err != nil {
			log.Printf("[deposit-evm] subscribe failed chain=%s err=%v", s.cfg.Chain, err)
			time.Sleep(3 * time.Second)
			continue
		}
		log.Printf("[deposit-evm] subscribe started chain=%s", s.cfg.Chain)
		func() {
			defer sub.Unsubscribe()
			for {
				select {
				case <-ctx.Done():
					return
				case err := <-sub.Err():
					if err != nil {
						log.Printf("[deposit-evm] subscribe dropped chain=%s err=%v", s.cfg.Chain, err)
					}
					return
				case lg := <-logCh:
					s.processRealtimeLog(ctx, lg)
				}
			}
		}()
		time.Sleep(1500 * time.Millisecond)
	}
}

func (s *EVMScanner) processRealtimeLog(ctx context.Context, lg types.Log) {
	if lg.Removed || len(lg.Topics) < 3 || len(lg.Data) != 32 {
		return
	}
	toAddr := common.BytesToAddress(lg.Topics[2].Bytes()[12:]).Hex()
	userID, err := s.addressRepo.FindUserIDByChainAddress(ctx, s.cfg.Chain, toAddr)
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			log.Printf("[deposit-evm] realtime address lookup failed chain=%s to=%s err=%v", s.cfg.Chain, toAddr, err)
		}
		return
	}
	fromAddr := common.BytesToAddress(lg.Topics[1].Bytes()[12:]).Hex()
	amount := new(big.Int).SetBytes(lg.Data)
	if amount.Sign() <= 0 {
		return
	}
	_, err = s.depositRepo.InsertPending(ctx, repo.DepositUpsertInput{
		Chain:                s.cfg.Chain,
		TxHash:               lg.TxHash.Hex(),
		LogIndex:             uint(lg.Index),
		BlockNumber:          lg.BlockNumber,
		TokenContractAddress: lg.Address.Hex(),
		UserID:               userID,
		FromAddress:          fromAddr,
		ToAddress:            toAddr,
		Amount:               amount,
	})
	if err != nil {
		log.Printf("[deposit-evm] realtime insert pending failed chain=%s tx=%s idx=%d err=%v", s.cfg.Chain, lg.TxHash.Hex(), lg.Index, err)
	}
}

func (s *EVMScanner) scanOnce(ctx context.Context) {
	state, ok := s.prepareScanState(ctx)
	if !ok {
		return
	}
	if !s.handleReorgIfNeeded(ctx, state) {
		return
	}
	s.backfillPending(ctx, state)
	s.confirmPending(ctx, state.latest)
}

type scanState struct {
	addressMap map[string]string
	nextBlock  uint64
	latest     uint64
}

func (s *EVMScanner) prepareScanState(ctx context.Context) (*scanState, bool) {
	addressMap, err := s.loadAddressMap(ctx)
	if err != nil {
		log.Printf("[deposit-evm] load address map failed chain=%s err=%v", s.cfg.Chain, err)
		return nil, false
	}
	if len(addressMap) == 0 {
		return nil, false
	}
	cur, err := s.depositRepo.GetOrCreateCursor(ctx, s.cfg.Chain, s.cfg.StartBlock)
	if err != nil {
		log.Printf("[deposit-evm] load cursor failed chain=%s err=%v", s.cfg.Chain, err)
		return nil, false
	}
	latest, err := s.client.LatestHeight(ctx)
	if err != nil {
		log.Printf("[deposit-evm] latest height failed chain=%s err=%v", s.cfg.Chain, err)
		return nil, false
	}
	return &scanState{
		addressMap: addressMap,
		nextBlock:  cur.NextBlockNumber,
		latest:     latest,
	}, true
}

func (s *EVMScanner) handleReorgIfNeeded(ctx context.Context, state *scanState) bool {
	reorgFrom, err := s.detectReorgFromCursor(ctx, state.nextBlock)
	if err != nil {
		log.Printf("[deposit-evm] detect reorg failed chain=%s err=%v", s.cfg.Chain, err)
		return false
	}
	if reorgFrom == 0 {
		return true
	}
	if err := s.depositRepo.RevertAndPruneFromBlock(ctx, s.cfg.Chain, reorgFrom); err != nil {
		log.Printf("[deposit-evm] revert from block failed chain=%s from=%d err=%v", s.cfg.Chain, reorgFrom, err)
		return false
	}
	if err := s.depositRepo.SaveCursor(ctx, s.cfg.Chain, reorgFrom); err != nil {
		log.Printf("[deposit-evm] rewind cursor failed chain=%s from=%d err=%v", s.cfg.Chain, reorgFrom, err)
		return false
	}
	log.Printf("[deposit-evm] reorg handled chain=%s rewind_to=%d", s.cfg.Chain, reorgFrom)
	state.nextBlock = reorgFrom
	return true
}

func (s *EVMScanner) backfillPending(ctx context.Context, state *scanState) {
	batch := s.cfg.BatchBlocks
	if batch == 0 {
		batch = 200
	}
	if state.nextBlock <= state.latest {
		from := state.nextBlock
		to := state.latest
		if to-from+1 > batch {
			to = from + batch - 1
		}
		if err := s.saveBlockHeaders(ctx, from, to); err != nil {
			log.Printf("[deposit-evm] save block headers failed chain=%s from=%d to=%d err=%v", s.cfg.Chain, from, to, err)
			return
		}
		newPending := s.scanRangeToPending(ctx, state.addressMap, from, to)
		if err := s.depositRepo.SaveCursor(ctx, s.cfg.Chain, to+1); err != nil {
			log.Printf("[deposit-evm] save cursor failed chain=%s next=%d err=%v", s.cfg.Chain, to+1, err)
			return
		}
		if newPending > 0 {
			log.Printf("[deposit-evm] scanned pending chain=%s from=%d to=%d new=%d", s.cfg.Chain, from, to, newPending)
		}
	}
}

func (s *EVMScanner) detectReorgFromCursor(ctx context.Context, cursor uint64) (uint64, error) {
	if cursor == 0 {
		return 0, nil
	}
	check := cursor - 1
	localHash, ok, err := s.depositRepo.GetBlockHash(ctx, s.cfg.Chain, check)
	if err != nil || !ok {
		return 0, err
	}
	head, err := s.client.HeaderByNumber(ctx, big.NewInt(int64(check)))
	if err != nil {
		return 0, err
	}
	if strings.EqualFold(localHash, head.Hash().Hex()) {
		return 0, nil
	}

	for b := check; ; b-- {
		h, ok, err := s.depositRepo.GetBlockHash(ctx, s.cfg.Chain, b)
		if err != nil {
			return 0, err
		}
		if !ok {
			return b + 1, nil
		}
		head, err := s.client.HeaderByNumber(ctx, big.NewInt(int64(b)))
		if err != nil {
			return 0, err
		}
		if strings.EqualFold(h, head.Hash().Hex()) {
			return b + 1, nil
		}
		if b == 0 {
			return 0, nil
		}
	}
}

func (s *EVMScanner) saveBlockHeaders(ctx context.Context, from uint64, to uint64) error {
	for n := from; n <= to; n++ {
		head, err := s.client.HeaderByNumber(ctx, big.NewInt(int64(n)))
		if err != nil {
			return err
		}
		if err := s.depositRepo.UpsertBlock(ctx, s.cfg.Chain, n, head.Hash().Hex(), head.ParentHash.Hex()); err != nil {
			return err
		}
	}
	return nil
}

func (s *EVMScanner) scanRangeToPending(ctx context.Context, addressMap map[string]string, from uint64, to uint64) int {
	logs, err := s.filterTransferLogs(ctx, from, to)
	if err != nil {
		log.Printf("[deposit-evm] filter logs failed chain=%s from=%d to=%d err=%v", s.cfg.Chain, from, to, err)
		return 0
	}
	newCount := 0
	for i := range logs {
		lg := logs[i]
		if lg.Removed || len(lg.Topics) < 3 || len(lg.Data) != 32 {
			continue
		}
		toAddr := common.BytesToAddress(lg.Topics[2].Bytes()[12:]).Hex()
		userID, ok := addressMap[strings.ToLower(toAddr)]
		if !ok {
			continue
		}
		fromAddr := common.BytesToAddress(lg.Topics[1].Bytes()[12:]).Hex()
		amount := new(big.Int).SetBytes(lg.Data)
		if amount.Sign() <= 0 {
			continue
		}
		inserted, err := s.depositRepo.InsertPending(ctx, repo.DepositUpsertInput{
			Chain:                s.cfg.Chain,
			TxHash:               lg.TxHash.Hex(),
			LogIndex:             uint(lg.Index),
			BlockNumber:          lg.BlockNumber,
			TokenContractAddress: lg.Address.Hex(),
			UserID:               userID,
			FromAddress:          fromAddr,
			ToAddress:            toAddr,
			Amount:               amount,
		})
		if err != nil {
			log.Printf("[deposit-evm] save pending failed chain=%s tx=%s idx=%d err=%v", s.cfg.Chain, lg.TxHash.Hex(), lg.Index, err)
			continue
		}
		if inserted {
			newCount++
		}
	}
	return newCount
}

func (s *EVMScanner) confirmPending(ctx context.Context, latest uint64) {
	if latest <= s.cfg.Confirmations {
		return
	}
	finalized := latest - s.cfg.Confirmations
	items, err := s.depositRepo.ListPendingConfirmable(ctx, s.cfg.Chain, finalized, 500)
	if err != nil {
		log.Printf("[deposit-evm] list confirmable failed chain=%s finalized=%d err=%v", s.cfg.Chain, finalized, err)
		return
	}
	confirmed := 0
	reverted := 0
	for i := range items {
		rec := items[i]
		ok, shouldRevert, err := s.isCanonicalDepositLog(ctx, rec)
		if err != nil {
			log.Printf("[deposit-evm] verify pending failed id=%d tx=%s err=%v", rec.ID, rec.TxHash, err)
			continue
		}
		if shouldRevert {
			if err := s.depositRepo.MarkReverted(ctx, rec.ID); err != nil {
				log.Printf("[deposit-evm] mark reverted failed id=%d tx=%s err=%v", rec.ID, rec.TxHash, err)
				continue
			}
			reverted++
			continue
		}
		if !ok {
			continue
		}
		updated, err := s.depositRepo.ConfirmAndCredit(ctx, rec.ID)
		if err != nil {
			log.Printf("[deposit-evm] confirm credit failed id=%d tx=%s err=%v", rec.ID, rec.TxHash, err)
			continue
		}
		if updated {
			confirmed++
		}
	}
	if confirmed > 0 || reverted > 0 {
		log.Printf("[deposit-evm] transition chain=%s finalized=%d confirmed=%d reverted=%d", s.cfg.Chain, finalized, confirmed, reverted)
	}
}

func (s *EVMScanner) isCanonicalDepositLog(ctx context.Context, rec depositmodel.DepositRecord) (ok bool, shouldRevert bool, err error) {
	receipt, err := s.client.TransactionReceipt(ctx, common.HexToHash(rec.TxHash))
	if err != nil {
		if errors.Is(err, ethereum.NotFound) {
			return false, true, nil
		}
		return false, false, err
	}
	if receipt == nil || receipt.Status != types.ReceiptStatusSuccessful {
		return false, true, nil
	}
	amt, okAmt := new(big.Int).SetString(rec.Amount, 10)
	if !okAmt || amt.Sign() <= 0 {
		return false, false, errors.New("invalid pending deposit amount")
	}
	for _, lg := range receipt.Logs {
		if lg == nil || len(lg.Topics) < 3 || len(lg.Data) != 32 {
			continue
		}
		if lg.Index != uint(rec.LogIndex) {
			continue
		}
		if !strings.EqualFold(lg.Address.Hex(), rec.TokenContractAddress) {
			continue
		}
		if lg.Topics[0] != erc20TransferTopic {
			continue
		}
		toAddr := common.BytesToAddress(lg.Topics[2].Bytes()[12:]).Hex()
		if !strings.EqualFold(toAddr, rec.ToAddress) {
			continue
		}
		logAmount := new(big.Int).SetBytes(lg.Data)
		if logAmount.Cmp(amt) != 0 {
			continue
		}
		return true, false, nil
	}
	return false, true, nil
}

func (s *EVMScanner) filterTransferLogs(ctx context.Context, from uint64, to uint64) ([]types.Log, error) {
	q := s.transferFilterQuery(big.NewInt(int64(from)), big.NewInt(int64(to)))
	return s.client.FilterLogs(ctx, q)
}

func (s *EVMScanner) transferFilterQuery(fromBlock *big.Int, toBlock *big.Int) ethereum.FilterQuery {
	contracts := make([]common.Address, 0, len(s.cfg.TokenContracts))
	for _, t := range s.cfg.TokenContracts {
		t = strings.TrimSpace(t)
		if common.IsHexAddress(t) {
			contracts = append(contracts, common.HexToAddress(t))
		}
	}
	return ethereum.FilterQuery{
		FromBlock: fromBlock,
		ToBlock:   toBlock,
		Addresses: contracts,
		Topics:    [][]common.Hash{{erc20TransferTopic}},
	}
}

func (s *EVMScanner) loadAddressMap(ctx context.Context) (map[string]string, error) {
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
		out[addr] = rows[i].UserID
	}
	return out, nil
}
