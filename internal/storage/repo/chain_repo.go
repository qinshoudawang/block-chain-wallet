package repo

import (
	"context"
	"errors"
	"math/big"
	"strings"

	chainmodel "wallet-system/internal/storage/model/chain"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ChainRepo struct {
	db *gorm.DB
}

func NewChainRepo(db *gorm.DB) *ChainRepo {
	return &ChainRepo{db: db}
}

func (r *ChainRepo) GetOrCreateCursor(ctx context.Context, chain string, stream string, startBlock uint64) (*chainmodel.IndexCursor, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("chain repo not configured")
	}
	chain = normalizeChain(chain)
	stream = normalizeStream(stream)
	var cur chainmodel.IndexCursor
	if err := r.db.WithContext(ctx).Where("chain = ? AND stream = ?", chain, stream).First(&cur).Error; err == nil {
		return &cur, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	cur = chainmodel.IndexCursor{Chain: chain, Stream: stream, NextBlockNumber: startBlock}
	if err := r.db.WithContext(ctx).Create(&cur).Error; err != nil {
		if err2 := r.db.WithContext(ctx).Where("chain = ? AND stream = ?", chain, stream).First(&cur).Error; err2 != nil {
			return nil, err
		}
	}
	return &cur, nil
}

func (r *ChainRepo) SaveCursor(ctx context.Context, chain string, stream string, nextBlock uint64) error {
	if r == nil || r.db == nil {
		return errors.New("chain repo not configured")
	}
	return r.db.WithContext(ctx).
		Model(&chainmodel.IndexCursor{}).
		Where("chain = ? AND stream = ?", normalizeChain(chain), normalizeStream(stream)).
		Update("next_block_number", nextBlock).Error
}

func (r *ChainRepo) UpsertEVMBlock(ctx context.Context, chain string, blockNumber uint64, blockHash string, parentHash string) error {
	if r == nil || r.db == nil {
		return errors.New("chain repo not configured")
	}
	rec := chainmodel.EVMIndexedBlock{
		Chain:       normalizeChain(chain),
		BlockNumber: blockNumber,
		BlockHash:   strings.TrimSpace(blockHash),
		ParentHash:  strings.TrimSpace(parentHash),
	}
	return r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain"}, {Name: "block_number"}},
		DoUpdates: clause.AssignmentColumns([]string{"block_hash", "parent_hash", "updated_at"}),
	}).Create(&rec).Error
}

func (r *ChainRepo) UpsertBTCBlock(ctx context.Context, chain string, blockNumber uint64, blockHash string, parentHash string) error {
	if r == nil || r.db == nil {
		return errors.New("chain repo not configured")
	}
	rec := chainmodel.BTCIndexedBlock{
		Chain:       normalizeChain(chain),
		BlockNumber: blockNumber,
		BlockHash:   strings.TrimSpace(blockHash),
		ParentHash:  strings.TrimSpace(parentHash),
	}
	return r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain"}, {Name: "block_number"}},
		DoUpdates: clause.AssignmentColumns([]string{"block_hash", "parent_hash", "updated_at"}),
	}).Create(&rec).Error
}

func (r *ChainRepo) GetEVMBlockHash(ctx context.Context, chain string, blockNumber uint64) (string, bool, error) {
	if r == nil || r.db == nil {
		return "", false, errors.New("chain repo not configured")
	}
	var out chainmodel.EVMIndexedBlock
	if err := r.db.WithContext(ctx).
		Select("block_hash").
		Where("chain = ? AND block_number = ?", normalizeChain(chain), blockNumber).
		First(&out).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", false, nil
		}
		return "", false, err
	}
	return out.BlockHash, true, nil
}

func (r *ChainRepo) GetBTCBlockHash(ctx context.Context, chain string, blockNumber uint64) (string, bool, error) {
	if r == nil || r.db == nil {
		return "", false, errors.New("chain repo not configured")
	}
	var out chainmodel.BTCIndexedBlock
	if err := r.db.WithContext(ctx).
		Select("block_hash").
		Where("chain = ? AND block_number = ?", normalizeChain(chain), blockNumber).
		First(&out).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", false, nil
		}
		return "", false, err
	}
	return out.BlockHash, true, nil
}

type ChainEventInput struct {
	Chain                   string
	EventType               chainmodel.EventType
	BlockNumber             uint64
	BlockHash               string
	TxHash                  string
	TxIndex                 uint
	LogIndex                uint
	AssetContractAddress    string
	FromAddress             string
	ToAddress               string
	Amount                  *big.Int
	FeeAssetContractAddress string
	FeeAmount               *big.Int
	Success                 bool
}

func (r *ChainRepo) InsertApplyEvent(ctx context.Context, in ChainEventInput) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("chain repo not configured")
	}
	if in.Amount == nil {
		in.Amount = big.NewInt(0)
	}
	if in.Amount.Sign() < 0 {
		return false, errors.New("invalid chain event amount")
	}
	if in.FeeAmount == nil {
		in.FeeAmount = big.NewInt(0)
	}
	if in.FeeAmount.Sign() < 0 {
		return false, errors.New("invalid chain event fee amount")
	}
	rec := chainmodel.ChainEvent{
		Chain:                   normalizeChain(in.Chain),
		EventType:               in.EventType,
		Action:                  chainmodel.EventActionApply,
		BlockNumber:             in.BlockNumber,
		BlockHash:               strings.TrimSpace(in.BlockHash),
		TxHash:                  strings.TrimSpace(in.TxHash),
		TxIndex:                 in.TxIndex,
		LogIndex:                in.LogIndex,
		AssetContractAddress:    strings.TrimSpace(in.AssetContractAddress),
		FromAddress:             strings.TrimSpace(in.FromAddress),
		ToAddress:               strings.TrimSpace(in.ToAddress),
		Amount:                  in.Amount.String(),
		FeeAssetContractAddress: strings.TrimSpace(in.FeeAssetContractAddress),
		FeeAmount:               in.FeeAmount.String(),
		Success:                 in.Success,
	}
	res := r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "chain"},
			{Name: "event_type"},
			{Name: "action"},
			{Name: "tx_hash"},
			{Name: "log_index"},
		},
		DoNothing: true,
	}).Create(&rec)
	return res.RowsAffected > 0, res.Error
}

func (r *ChainRepo) RevertFromBlock(ctx context.Context, chain string, fromBlock uint64) error {
	if r == nil || r.db == nil {
		return errors.New("chain repo not configured")
	}
	chain = normalizeChain(chain)
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&chainmodel.ReorgNotice{
			Chain:     chain,
			FromBlock: fromBlock,
		}).Error; err != nil {
			return err
		}
		var rows []chainmodel.ChainEvent
		if err := tx.
			Where("chain = ? AND block_number >= ? AND action = ?", chain, fromBlock, chainmodel.EventActionApply).
			Order("id ASC").
			Find(&rows).Error; err != nil {
			return err
		}
		for i := range rows {
			rec := rows[i]
			rootID := rec.ID
			revertRec := chainmodel.ChainEvent{
				Chain:                   rec.Chain,
				EventType:               rec.EventType,
				Action:                  chainmodel.EventActionRevert,
				BlockNumber:             rec.BlockNumber,
				BlockHash:               rec.BlockHash,
				TxHash:                  rec.TxHash,
				TxIndex:                 rec.TxIndex,
				LogIndex:                rec.LogIndex,
				AssetContractAddress:    rec.AssetContractAddress,
				FromAddress:             rec.FromAddress,
				ToAddress:               rec.ToAddress,
				Amount:                  rec.Amount,
				FeeAssetContractAddress: rec.FeeAssetContractAddress,
				FeeAmount:               rec.FeeAmount,
				Success:                 rec.Success,
				RootEventID:             &rootID,
			}
			if err := tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{
					{Name: "chain"},
					{Name: "event_type"},
					{Name: "action"},
					{Name: "tx_hash"},
					{Name: "log_index"},
				},
				DoNothing: true,
			}).Create(&revertRec).Error; err != nil {
				return err
			}
		}
		return tx.Where("chain = ? AND block_number >= ?", chain, fromBlock).Delete(&chainmodel.EVMIndexedBlock{}).Error
	})
}

func (r *ChainRepo) DeleteBTCBlocksFrom(ctx context.Context, chain string, fromBlock uint64) error {
	if r == nil || r.db == nil {
		return errors.New("chain repo not configured")
	}
	return r.db.WithContext(ctx).
		Where("chain = ? AND block_number >= ?", normalizeChain(chain), fromBlock).
		Delete(&chainmodel.BTCIndexedBlock{}).Error
}

func (r *ChainRepo) ListReorgNoticesAfterID(ctx context.Context, chain string, lastID uint64, limit int) ([]chainmodel.ReorgNotice, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("chain repo not configured")
	}
	if limit <= 0 {
		limit = 100
	}
	var out []chainmodel.ReorgNotice
	err := r.db.WithContext(ctx).
		Where("chain = ? AND id > ?", normalizeChain(chain), lastID).
		Order("id ASC").
		Limit(limit).
		Find(&out).Error
	return out, err
}

func (r *ChainRepo) GetOrCreateProjectorCursor(ctx context.Context, name string) (*chainmodel.ProjectorCursor, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("chain repo not configured")
	}
	name = strings.TrimSpace(name)
	var cur chainmodel.ProjectorCursor
	if err := r.db.WithContext(ctx).Where("name = ?", name).First(&cur).Error; err == nil {
		return &cur, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	cur = chainmodel.ProjectorCursor{Name: name}
	if err := r.db.WithContext(ctx).Create(&cur).Error; err != nil {
		if err2 := r.db.WithContext(ctx).Where("name = ?", name).First(&cur).Error; err2 != nil {
			return nil, err
		}
	}
	return &cur, nil
}

func (r *ChainRepo) SaveProjectorCursor(ctx context.Context, name string, lastEventID uint64) error {
	if r == nil || r.db == nil {
		return errors.New("chain repo not configured")
	}
	return r.db.WithContext(ctx).
		Model(&chainmodel.ProjectorCursor{}).
		Where("name = ?", strings.TrimSpace(name)).
		Update("last_event_id", lastEventID).Error
}

func (r *ChainRepo) ListEventsAfterID(ctx context.Context, chain string, eventType chainmodel.EventType, lastID uint64, limit int) ([]chainmodel.ChainEvent, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("chain repo not configured")
	}
	if limit <= 0 {
		limit = 500
	}
	var out []chainmodel.ChainEvent
	err := r.db.WithContext(ctx).
		Where("chain = ? AND event_type = ? AND id > ?", normalizeChain(chain), eventType, lastID).
		Order("id ASC").
		Limit(limit).
		Find(&out).Error
	return out, err
}

func normalizeChain(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}

func normalizeStream(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}
