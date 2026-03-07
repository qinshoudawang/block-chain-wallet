package evmindex

import (
	"context"
	"errors"
	"log"
	"math/big"
	"strings"
	"time"

	evmchain "wallet-system/internal/chain/evm"
	chainmodel "wallet-system/internal/storage/model/chain"
	"wallet-system/internal/storage/repo"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
)

var erc20TransferTopic = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

const (
	evmIngressStream     = "evm_ingress"
	legacyEVMTransferKey = "evm_transfer"
)

type EVMIndexerConfig struct {
	Chain          string
	TokenContracts []string
	Confirmations  uint64
	PollInterval   time.Duration
	BatchBlocks    uint64
	StartBlock     uint64
}

type EVMIndexer struct {
	cfg         EVMIndexerConfig
	evm         *evmchain.Client
	repo        *repo.ChainRepo
	addressRepo *repo.AddressRepo
}

func NewEVMIndexer(chainRepo *repo.ChainRepo, addressRepo *repo.AddressRepo, client *evmchain.Client, cfg EVMIndexerConfig) *EVMIndexer {
	return &EVMIndexer{
		cfg:         cfg,
		evm:         client,
		repo:        chainRepo,
		addressRepo: addressRepo,
	}
}

func (s *EVMIndexer) Run(ctx context.Context) {
	if s == nil || s.evm == nil || s.repo == nil {
		return
	}
	if len(s.cfg.TokenContracts) == 0 {
		log.Printf("[chain-indexer-evm] disabled chain=%s reason=no token contracts", s.cfg.Chain)
		return
	}
	poll := s.cfg.PollInterval
	if poll <= 0 {
		poll = 8 * time.Second
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

func (s *EVMIndexer) scanOnce(ctx context.Context) {
	cur, latest, ok := s.prepareState(ctx)
	if !ok {
		return
	}
	if !s.handleReorgIfNeeded(ctx, cur.NextBlockNumber) {
		return
	}
	finalized := latest - s.cfg.Confirmations
	if cur.NextBlockNumber > finalized {
		return
	}
	batch := s.cfg.BatchBlocks
	if batch == 0 {
		batch = 200
	}
	from := cur.NextBlockNumber
	to := finalized
	if to-from+1 > batch {
		to = from + batch - 1
	}
	addressSet, err := s.loadDepositAddressSet(ctx)
	if err != nil {
		log.Printf("[chain-indexer-evm] load deposit address set failed chain=%s err=%v", s.cfg.Chain, err)
		return
	}
	if err := s.indexRange(ctx, from, to, addressSet); err != nil {
		log.Printf("[chain-indexer-evm] index range failed chain=%s from=%d to=%d err=%v", s.cfg.Chain, from, to, err)
		return
	}
	if err := s.repo.SaveCursor(ctx, s.cfg.Chain, evmIngressStream, to+1); err != nil {
		log.Printf("[chain-indexer-evm] save cursor failed chain=%s next=%d err=%v", s.cfg.Chain, to+1, err)
		return
	}
	log.Printf("[chain-indexer-evm] indexed chain=%s from=%d to=%d", s.cfg.Chain, from, to)
}

func (s *EVMIndexer) prepareState(ctx context.Context) (*chainmodel.IndexCursor, uint64, bool) {
	cur, err := s.loadOrMigrateCursor(ctx)
	if err != nil {
		log.Printf("[chain-indexer-evm] load cursor failed chain=%s err=%v", s.cfg.Chain, err)
		return nil, 0, false
	}
	latest, err := s.evm.LatestHeight(ctx)
	if err != nil {
		log.Printf("[chain-indexer-evm] latest height failed chain=%s err=%v", s.cfg.Chain, err)
		return nil, 0, false
	}
	if latest < s.cfg.Confirmations {
		return cur, latest, false
	}
	return cur, latest, true
}

func (s *EVMIndexer) loadOrMigrateCursor(ctx context.Context) (*chainmodel.IndexCursor, error) {
	cur, err := s.repo.GetOrCreateCursor(ctx, s.cfg.Chain, evmIngressStream, s.cfg.StartBlock)
	if err != nil || cur == nil {
		return cur, err
	}
	if cur.NextBlockNumber != s.cfg.StartBlock {
		return cur, nil
	}
	legacy, err := s.repo.GetOrCreateCursor(ctx, s.cfg.Chain, legacyEVMTransferKey, s.cfg.StartBlock)
	if err != nil || legacy == nil {
		return cur, err
	}
	if legacy.NextBlockNumber <= s.cfg.StartBlock {
		return cur, nil
	}
	if err := s.repo.SaveCursor(ctx, s.cfg.Chain, evmIngressStream, legacy.NextBlockNumber); err != nil {
		return nil, err
	}
	cur.NextBlockNumber = legacy.NextBlockNumber
	return cur, nil
}

func (s *EVMIndexer) handleReorgIfNeeded(ctx context.Context, cursor uint64) bool {
	reorgFrom, err := s.detectReorgFromCursor(ctx, cursor)
	if err != nil {
		log.Printf("[chain-indexer-evm] detect reorg failed chain=%s err=%v", s.cfg.Chain, err)
		return false
	}
	if reorgFrom == 0 {
		return true
	}
	if err := s.repo.RevertFromBlock(ctx, s.cfg.Chain, reorgFrom); err != nil {
		log.Printf("[chain-indexer-evm] revert from block failed chain=%s from=%d err=%v", s.cfg.Chain, reorgFrom, err)
		return false
	}
	if err := s.repo.SaveCursor(ctx, s.cfg.Chain, evmIngressStream, reorgFrom); err != nil {
		log.Printf("[chain-indexer-evm] rewind cursor failed chain=%s from=%d err=%v", s.cfg.Chain, reorgFrom, err)
		return false
	}
	log.Printf("[chain-indexer-evm] reorg handled chain=%s rewind_to=%d", s.cfg.Chain, reorgFrom)
	return true
}

func (s *EVMIndexer) detectReorgFromCursor(ctx context.Context, cursor uint64) (uint64, error) {
	if cursor == 0 {
		return 0, nil
	}
	check := cursor - 1
	localHash, ok, err := s.repo.GetBlockHash(ctx, s.cfg.Chain, check)
	if err != nil || !ok {
		return 0, err
	}
	head, err := s.evm.HeaderByNumber(ctx, big.NewInt(int64(check)))
	if err != nil {
		return 0, err
	}
	if strings.EqualFold(localHash, head.Hash().Hex()) {
		return 0, nil
	}
	for b := check; ; b-- {
		h, ok, err := s.repo.GetBlockHash(ctx, s.cfg.Chain, b)
		if err != nil {
			return 0, err
		}
		if !ok {
			return b + 1, nil
		}
		head, err := s.evm.HeaderByNumber(ctx, big.NewInt(int64(b)))
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

func (s *EVMIndexer) indexRange(ctx context.Context, from uint64, to uint64, depositAddressSet map[string]struct{}) error {
	for n := from; n <= to; n++ {
		block, err := s.evm.BlockByNumber(ctx, big.NewInt(int64(n)))
		if err != nil {
			return err
		}
		if err := s.repo.UpsertBlock(ctx, s.cfg.Chain, n, block.Hash().Hex(), block.ParentHash().Hex()); err != nil {
			return err
		}
		for _, tx := range block.Transactions() {
			receipt, err := s.evm.TransactionReceipt(ctx, tx.Hash())
			if err != nil {
				return err
			}
			if receipt == nil {
				continue
			}
			for _, lg := range receipt.Logs {
				if lg == nil || lg.Removed || len(lg.Topics) < 3 || len(lg.Data) != 32 {
					continue
				}
				if lg.Topics[0] != erc20TransferTopic {
					continue
				}
				amount := new(big.Int).SetBytes(lg.Data)
				if amount.Sign() <= 0 {
					continue
				}
				toAddr := common.BytesToAddress(lg.Topics[2].Bytes()[12:]).Hex()
				if _, ok := depositAddressSet[strings.ToLower(strings.TrimSpace(toAddr))]; !ok {
					continue
				}
				_, err = s.repo.InsertApplyEvent(ctx, repo.ChainEventInput{
					Chain:                s.cfg.Chain,
					EventType:            chainmodel.EventTypeTransfer,
					BlockNumber:          receipt.BlockNumber.Uint64(),
					BlockHash:            receipt.BlockHash.Hex(),
					TxHash:               receipt.TxHash.Hex(),
					TxIndex:              uint(receipt.TransactionIndex),
					LogIndex:             uint(lg.Index),
					AssetContractAddress: lg.Address.Hex(),
					FromAddress:          common.BytesToAddress(lg.Topics[1].Bytes()[12:]).Hex(),
					ToAddress:            toAddr,
					Amount:               amount,
					Success:              receipt.Status == ethtypes.ReceiptStatusSuccessful,
				})
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *EVMIndexer) loadDepositAddressSet(ctx context.Context) (map[string]struct{}, error) {
	if s == nil || s.addressRepo == nil {
		return map[string]struct{}{}, nil
	}
	rows, err := s.addressRepo.ListByChain(ctx, s.cfg.Chain)
	if err != nil {
		return nil, err
	}
	out := make(map[string]struct{}, len(rows))
	for i := range rows {
		addr := strings.ToLower(strings.TrimSpace(rows[i].Address))
		if addr == "" {
			continue
		}
		out[addr] = struct{}{}
	}
	return out, nil
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
