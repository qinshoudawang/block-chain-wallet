package repo

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	depositmodel "wallet-system/internal/storage/model/deposit"
	ledgermodel "wallet-system/internal/storage/model/ledger"
	userledgermodel "wallet-system/internal/storage/model/userledger"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type DepositRepo struct {
	db *gorm.DB
}

func NewDepositRepo(db *gorm.DB) *DepositRepo {
	return &DepositRepo{db: db}
}

func (r *DepositRepo) GetOrCreateCursor(ctx context.Context, chain string, startBlock uint64) (*depositmodel.DepositCursor, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("deposit repo not configured")
	}
	chain = strings.ToLower(strings.TrimSpace(chain))
	var cur depositmodel.DepositCursor
	if err := r.db.WithContext(ctx).Where("chain = ?", chain).First(&cur).Error; err == nil {
		return &cur, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	cur = depositmodel.DepositCursor{
		Chain:           chain,
		NextBlockNumber: startBlock,
	}
	if err := r.db.WithContext(ctx).Create(&cur).Error; err != nil {
		if err2 := r.db.WithContext(ctx).Where("chain = ?", chain).First(&cur).Error; err2 != nil {
			return nil, err
		}
	}
	return &cur, nil
}

func (r *DepositRepo) SaveCursor(ctx context.Context, chain string, nextBlock uint64) error {
	if r == nil || r.db == nil {
		return errors.New("deposit repo not configured")
	}
	chain = strings.ToLower(strings.TrimSpace(chain))
	return r.db.WithContext(ctx).
		Model(&depositmodel.DepositCursor{}).
		Where("chain = ?", chain).
		Update("next_block_number", nextBlock).Error
}

func (r *DepositRepo) UpsertBlock(ctx context.Context, chain string, blockNumber uint64, blockHash string, parentHash string) error {
	if r == nil || r.db == nil {
		return errors.New("deposit repo not configured")
	}
	chain = strings.ToLower(strings.TrimSpace(chain))
	rec := depositmodel.DepositBlock{
		Chain:       chain,
		BlockNumber: blockNumber,
		BlockHash:   strings.TrimSpace(blockHash),
		ParentHash:  strings.TrimSpace(parentHash),
	}
	return r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain"}, {Name: "block_number"}},
		DoUpdates: clause.AssignmentColumns([]string{"block_hash", "parent_hash", "updated_at"}),
	}).Create(&rec).Error
}

func (r *DepositRepo) GetBlockHash(ctx context.Context, chain string, blockNumber uint64) (string, bool, error) {
	if r == nil || r.db == nil {
		return "", false, errors.New("deposit repo not configured")
	}
	chain = strings.ToLower(strings.TrimSpace(chain))
	var out depositmodel.DepositBlock
	if err := r.db.WithContext(ctx).
		Select("block_hash").
		Where("chain = ? AND block_number = ?", chain, blockNumber).
		First(&out).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", false, nil
		}
		return "", false, err
	}
	return out.BlockHash, true, nil
}

func (r *DepositRepo) RevertAndPruneFromBlock(ctx context.Context, chain string, fromBlock uint64) error {
	if r == nil || r.db == nil {
		return errors.New("deposit repo not configured")
	}
	chain = strings.ToLower(strings.TrimSpace(chain))
	now := time.Now()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var rows []depositmodel.DepositRecord
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("chain = ? AND block_number >= ? AND status IN ?", chain, fromBlock, []depositmodel.Status{
				depositmodel.StatusPending, depositmodel.StatusConfirmed,
			}).
			Find(&rows).Error; err != nil {
			return err
		}
		for i := range rows {
			rec := rows[i]
			if rec.Status == depositmodel.StatusConfirmed {
				amount, ok := new(big.Int).SetString(rec.Amount, 10)
				if !ok || amount.Sign() <= 0 {
					return fmt.Errorf("invalid deposit amount: %s", rec.Amount)
				}
				if err := revertUserLedgerByDepositTx(tx, rec, amount); err != nil {
					return err
				}
				var acct ledgermodel.LedgerAccount
				if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
					Where("chain = ? AND address = ? AND asset_contract_address = ?", rec.Chain, rec.ToAddress, rec.TokenContractAddress).
					First(&acct).Error; err != nil {
					return err
				}
				avail, ok := new(big.Int).SetString(acct.AvailableAmount, 10)
				if !ok || avail.Sign() < 0 {
					return fmt.Errorf("invalid available amount: %s", acct.AvailableAmount)
				}
				if avail.Cmp(amount) < 0 {
					return errors.New("ledger available insufficient for deposit revert")
				}
				avail.Sub(avail, amount)
				if err := tx.Model(&ledgermodel.LedgerAccount{}).
					Where("id = ?", acct.ID).
					Update("available_amount", avail.String()).Error; err != nil {
					return err
				}
			}
			if err := tx.Model(&depositmodel.DepositRecord{}).
				Where("id = ?", rec.ID).
				Updates(map[string]any{
					"status":      depositmodel.StatusReverted,
					"reverted_at": &now,
					"updated_at":  now,
				}).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("chain = ? AND block_number >= ?", chain, fromBlock).Delete(&depositmodel.DepositBlock{}).Error; err != nil {
			return err
		}
		return nil
	})
}

type DepositUpsertInput struct {
	Chain                string
	TxHash               string
	LogIndex             uint
	BlockNumber          uint64
	TokenContractAddress string
	UserID               string
	FromAddress          string
	ToAddress            string
	Amount               *big.Int
}

func (r *DepositRepo) InsertPending(ctx context.Context, in DepositUpsertInput) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("deposit repo not configured")
	}
	if in.Amount == nil || in.Amount.Sign() <= 0 {
		return false, errors.New("invalid deposit amount")
	}
	in.Chain = strings.ToLower(strings.TrimSpace(in.Chain))
	in.TxHash = strings.TrimSpace(in.TxHash)
	in.TokenContractAddress = strings.TrimSpace(in.TokenContractAddress)
	in.UserID = strings.TrimSpace(in.UserID)
	in.FromAddress = strings.TrimSpace(in.FromAddress)
	in.ToAddress = strings.TrimSpace(in.ToAddress)
	if in.Chain == "" || in.TxHash == "" || in.UserID == "" || in.ToAddress == "" {
		return false, errors.New("invalid deposit input")
	}

	rec := depositmodel.DepositRecord{
		Chain:                in.Chain,
		TxHash:               in.TxHash,
		LogIndex:             in.LogIndex,
		BlockNumber:          in.BlockNumber,
		TokenContractAddress: in.TokenContractAddress,
		UserID:               in.UserID,
		ToAddress:            in.ToAddress,
		FromAddress:          in.FromAddress,
		Amount:               in.Amount.String(),
		Status:               depositmodel.StatusPending,
	}
	res := r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "chain"}, {Name: "tx_hash"}, {Name: "log_index"}},
		DoNothing: true,
	}).Create(&rec)
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected > 0, nil
}

func (r *DepositRepo) ListPendingConfirmable(ctx context.Context, chain string, finalizedBlock uint64, limit int) ([]depositmodel.DepositRecord, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("deposit repo not configured")
	}
	if limit <= 0 {
		limit = 500
	}
	chain = strings.ToLower(strings.TrimSpace(chain))
	var out []depositmodel.DepositRecord
	err := r.db.WithContext(ctx).
		Where("chain = ? AND status = ? AND block_number <= ?", chain, depositmodel.StatusPending, finalizedBlock).
		Order("block_number ASC").
		Limit(limit).
		Find(&out).Error
	return out, err
}

func (r *DepositRepo) MarkReverted(ctx context.Context, id uint64) error {
	if r == nil || r.db == nil {
		return errors.New("deposit repo not configured")
	}
	now := time.Now()
	return r.db.WithContext(ctx).Model(&depositmodel.DepositRecord{}).
		Where("id = ? AND status = ?", id, depositmodel.StatusPending).
		Updates(map[string]any{
			"status":      depositmodel.StatusReverted,
			"reverted_at": &now,
			"updated_at":  now,
		}).Error
}

func (r *DepositRepo) ListFinalizedAfterID(ctx context.Context, chain string, lastID uint64, limit int) ([]depositmodel.DepositRecord, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("deposit repo not configured")
	}
	if limit <= 0 {
		limit = 500
	}
	chain = strings.ToLower(strings.TrimSpace(chain))
	var out []depositmodel.DepositRecord
	err := r.db.WithContext(ctx).
		Where("chain = ? AND id > ? AND status IN ?", chain, lastID, []depositmodel.Status{
			depositmodel.StatusConfirmed, depositmodel.StatusReverted,
		}).
		Order("id ASC").
		Limit(limit).
		Find(&out).Error
	return out, err
}

func (r *DepositRepo) ConfirmAndCredit(ctx context.Context, id uint64) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("deposit repo not configured")
	}
	updated := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var rec depositmodel.DepositRecord
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", id).First(&rec).Error; err != nil {
			return err
		}
		if rec.Status == depositmodel.StatusConfirmed {
			return nil
		}
		if rec.Status != depositmodel.StatusPending {
			return nil
		}
		amount, ok := new(big.Int).SetString(rec.Amount, 10)
		if !ok || amount.Sign() <= 0 {
			return fmt.Errorf("invalid deposit amount: %s", rec.Amount)
		}

		if err := creditUserLedgerByDepositTx(tx, rec, amount); err != nil {
			return err
		}

		var acct ledgermodel.LedgerAccount
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("chain = ? AND address = ? AND asset_contract_address = ? AND user_id = ?", rec.Chain, rec.ToAddress, rec.TokenContractAddress, rec.UserID).
			First(&acct).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if errors.Is(err, gorm.ErrRecordNotFound) {
			acct = ledgermodel.LedgerAccount{
				UserID:               rec.UserID,
				Chain:                rec.Chain,
				Address:              rec.ToAddress,
				AssetContractAddress: rec.TokenContractAddress,
				AvailableAmount:      amount.String(),
				FrozenAmount:         "0",
			}
			if err := tx.Create(&acct).Error; err != nil {
				return err
			}
		} else {
			avail, ok := new(big.Int).SetString(acct.AvailableAmount, 10)
			if !ok || avail.Sign() < 0 {
				return fmt.Errorf("invalid available amount: %s", acct.AvailableAmount)
			}
			avail.Add(avail, amount)
			if err := tx.Model(&ledgermodel.LedgerAccount{}).
				Where("id = ?", acct.ID).
				Update("available_amount", avail.String()).Error; err != nil {
				return err
			}
		}

		now := time.Now()
		if err := tx.Model(&depositmodel.DepositRecord{}).
			Where("id = ? AND status = ?", rec.ID, depositmodel.StatusPending).
			Updates(map[string]any{
				"status":       depositmodel.StatusConfirmed,
				"confirmed_at": &now,
				"updated_at":   now,
			}).Error; err != nil {
			return err
		}
		updated = true
		return nil
	})
	return updated, err
}

func creditUserLedgerByDepositTx(tx *gorm.DB, rec depositmodel.DepositRecord, amount *big.Int) error {
	if amount == nil || amount.Sign() <= 0 {
		return errors.New("invalid deposit amount for user ledger")
	}
	var acct userledgermodel.UserLedgerAccount
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("user_id = ? AND chain = ? AND asset_contract_address = ?", rec.UserID, rec.Chain, rec.TokenContractAddress).
		First(&acct).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		acct = userledgermodel.UserLedgerAccount{
			UserID:               rec.UserID,
			Chain:                rec.Chain,
			AssetContractAddress: rec.TokenContractAddress,
			AvailableAmount:      amount.String(),
			FrozenAmount:         "0",
		}
		if err := tx.Create(&acct).Error; err != nil {
			return err
		}
	} else {
		avail, ok := new(big.Int).SetString(acct.AvailableAmount, 10)
		if !ok || avail.Sign() < 0 {
			return fmt.Errorf("invalid user ledger available amount: %s", acct.AvailableAmount)
		}
		avail.Add(avail, amount)
		if err := tx.Model(&userledgermodel.UserLedgerAccount{}).
			Where("id = ?", acct.ID).
			Update("available_amount", avail.String()).Error; err != nil {
			return err
		}
	}
	entry := userledgermodel.UserLedgerEntry{
		BizType:              userledgermodel.BizTypeDepositConfirm,
		BizID:                depositBizID(rec),
		UserID:               rec.UserID,
		Chain:                rec.Chain,
		AssetContractAddress: rec.TokenContractAddress,
		DeltaAvailable:       amount.String(),
		DeltaFrozen:          "0",
	}
	return tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "biz_type"}, {Name: "biz_id"}},
		DoNothing: true,
	}).Create(&entry).Error
}

func revertUserLedgerByDepositTx(tx *gorm.DB, rec depositmodel.DepositRecord, amount *big.Int) error {
	if amount == nil || amount.Sign() <= 0 {
		return errors.New("invalid revert amount for user ledger")
	}
	var acct userledgermodel.UserLedgerAccount
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("user_id = ? AND chain = ? AND asset_contract_address = ?", rec.UserID, rec.Chain, rec.TokenContractAddress).
		First(&acct).Error; err != nil {
		return err
	}
	avail, ok := new(big.Int).SetString(acct.AvailableAmount, 10)
	if !ok || avail.Sign() < 0 {
		return fmt.Errorf("invalid user ledger available amount: %s", acct.AvailableAmount)
	}
	if avail.Cmp(amount) < 0 {
		return errors.New("user ledger available insufficient for deposit revert")
	}
	avail.Sub(avail, amount)
	if err := tx.Model(&userledgermodel.UserLedgerAccount{}).
		Where("id = ?", acct.ID).
		Update("available_amount", avail.String()).Error; err != nil {
		return err
	}
	neg := new(big.Int).Neg(amount)
	entry := userledgermodel.UserLedgerEntry{
		BizType:              userledgermodel.BizTypeDepositRevert,
		BizID:                depositBizID(rec),
		UserID:               rec.UserID,
		Chain:                rec.Chain,
		AssetContractAddress: rec.TokenContractAddress,
		DeltaAvailable:       neg.String(),
		DeltaFrozen:          "0",
	}
	return tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "biz_type"}, {Name: "biz_id"}},
		DoNothing: true,
	}).Create(&entry).Error
}

func depositBizID(rec depositmodel.DepositRecord) string {
	return rec.Chain + ":" + rec.TxHash + ":" + strconv.FormatUint(uint64(rec.LogIndex), 10)
}
