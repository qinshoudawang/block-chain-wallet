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

func (r *DepositRepo) CreateConfirmedAndCredit(ctx context.Context, in DepositUpsertInput) error {
	if r == nil || r.db == nil {
		return errors.New("deposit repo not configured")
	}
	if in.Amount == nil || in.Amount.Sign() <= 0 {
		return errors.New("invalid deposit amount")
	}
	in.Chain = strings.ToLower(strings.TrimSpace(in.Chain))
	in.TxHash = strings.TrimSpace(in.TxHash)
	in.TokenContractAddress = strings.TrimSpace(in.TokenContractAddress)
	in.UserID = strings.TrimSpace(in.UserID)
	in.FromAddress = strings.TrimSpace(in.FromAddress)
	in.ToAddress = strings.TrimSpace(in.ToAddress)
	if in.Chain == "" || in.TxHash == "" || in.UserID == "" || in.ToAddress == "" {
		return errors.New("invalid deposit input")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var rec depositmodel.DepositRecord
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("chain = ? AND tx_hash = ? AND log_index = ?", in.Chain, in.TxHash, in.LogIndex).
			First(&rec).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			now := time.Now()
			rec = depositmodel.DepositRecord{
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
				ConfirmedAt:          &now,
			}
			if err := tx.Create(&rec).Error; err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if rec.Status == depositmodel.StatusConfirmed {
			return nil
		}
		if rec.Status == depositmodel.StatusReverted {
			return nil
		}
		if err := creditUserLedgerByDepositTx(tx, rec, in.Amount); err != nil {
			return err
		}
		var acct ledgermodel.LedgerAccount
		err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
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
				AvailableAmount:      in.Amount.String(),
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
			avail.Add(avail, in.Amount)
			if err := tx.Model(&ledgermodel.LedgerAccount{}).
				Where("id = ?", acct.ID).
				Update("available_amount", avail.String()).Error; err != nil {
				return err
			}
		}
		now := time.Now()
		return tx.Model(&depositmodel.DepositRecord{}).
			Where("id = ? AND status = ?", rec.ID, depositmodel.StatusPending).
			Updates(map[string]any{
				"status":       depositmodel.StatusConfirmed,
				"confirmed_at": &now,
				"updated_at":   now,
			}).Error
	})
}

func (r *DepositRepo) RevertByChainRef(ctx context.Context, chain string, txHash string, logIndex uint) error {
	if r == nil || r.db == nil {
		return errors.New("deposit repo not configured")
	}
	chain = strings.ToLower(strings.TrimSpace(chain))
	txHash = strings.TrimSpace(txHash)
	now := time.Now()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var rec depositmodel.DepositRecord
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("chain = ? AND tx_hash = ? AND log_index = ?", chain, txHash, logIndex).
			First(&rec).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		if rec.Status == depositmodel.StatusReverted {
			return nil
		}
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
		return tx.Model(&depositmodel.DepositRecord{}).
			Where("id = ?", rec.ID).
			Updates(map[string]any{
				"status":      depositmodel.StatusReverted,
				"reverted_at": &now,
				"updated_at":  now,
			}).Error
	})
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
