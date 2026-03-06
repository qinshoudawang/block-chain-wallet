package repo

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	ledgermodel "wallet-system/internal/storage/model/ledger"
	sweepmodel "wallet-system/internal/storage/model/sweep"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type SweepCandidate struct {
	UserID               string
	Chain                string
	Address              string
	AssetContractAddress string
	AvailableAmount      string
}

type SweepCreateInput struct {
	SweepID              string
	Chain                string
	UserID               string
	FromAddress          string
	ToAddress            string
	AssetContractAddress string
	Amount               string
}

type SweepRepo struct {
	db *gorm.DB
}

func NewSweepRepo(db *gorm.DB) *SweepRepo {
	return &SweepRepo{db: db}
}

func (r *SweepRepo) ListCandidates(
	ctx context.Context,
	chain string,
	assetContractAddress string,
	limit int,
) ([]SweepCandidate, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("sweep repo not configured")
	}
	if limit <= 0 {
		limit = 200
	}
	chain = strings.ToLower(strings.TrimSpace(chain))
	assetContractAddress = strings.TrimSpace(assetContractAddress)

	var out []SweepCandidate
	err := r.db.WithContext(ctx).
		Table("ledger_accounts").
		Select("user_id, chain, address, asset_contract_address, available_amount").
		Where("chain = ? AND asset_contract_address = ? AND user_id <> ''", chain, assetContractAddress).
		Limit(limit).
		Scan(&out).Error
	return out, err
}

func (r *SweepRepo) HasActiveSweep(
	ctx context.Context,
	chain string,
	fromAddress string,
	assetContractAddress string,
) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("sweep repo not configured")
	}
	var cnt int64
	err := r.db.WithContext(ctx).
		Model(&sweepmodel.SweepOrder{}).
		Where("chain = ? AND from_address = ? AND asset_contract_address = ? AND status IN ?",
			strings.ToLower(strings.TrimSpace(chain)),
			strings.TrimSpace(fromAddress),
			strings.TrimSpace(assetContractAddress),
			[]sweepmodel.SweepStatus{sweepmodel.SweepStatusInit, sweepmodel.SweepStatusBroadcasted},
		).
		Count(&cnt).Error
	return cnt > 0, err
}

func (r *SweepRepo) CreateInit(ctx context.Context, in SweepCreateInput) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("sweep repo not configured")
	}
	var created bool
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		rec := sweepmodel.SweepOrder{
			SweepID:              strings.TrimSpace(in.SweepID),
			Chain:                strings.ToLower(strings.TrimSpace(in.Chain)),
			UserID:               strings.TrimSpace(in.UserID),
			FromAddress:          strings.TrimSpace(in.FromAddress),
			ToAddress:            strings.TrimSpace(in.ToAddress),
			AssetContractAddress: strings.TrimSpace(in.AssetContractAddress),
			Amount:               strings.TrimSpace(in.Amount),
			Status:               sweepmodel.SweepStatusInit,
		}
		res := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "sweep_id"}},
			DoNothing: true,
		}).Create(&rec)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			created = false
			return nil
		}

		amt, ok := new(big.Int).SetString(strings.TrimSpace(rec.Amount), 10)
		if !ok || amt.Sign() <= 0 {
			return fmt.Errorf("invalid sweep amount: %s", rec.Amount)
		}
		var acct ledgermodel.LedgerAccount
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("chain = ? AND address = ? AND asset_contract_address = ? AND user_id = ?",
				rec.Chain, rec.FromAddress, rec.AssetContractAddress, rec.UserID).
			First(&acct).Error; err != nil {
			return err
		}
		avail, ok := new(big.Int).SetString(strings.TrimSpace(acct.AvailableAmount), 10)
		if !ok || avail.Sign() < 0 {
			return fmt.Errorf("invalid ledger available amount: %s", acct.AvailableAmount)
		}
		frozen, ok := new(big.Int).SetString(strings.TrimSpace(acct.FrozenAmount), 10)
		if !ok || frozen.Sign() < 0 {
			return fmt.Errorf("invalid ledger frozen amount: %s", acct.FrozenAmount)
		}
		if avail.Cmp(amt) < 0 {
			return errors.New("ledger available insufficient for sweep freeze")
		}
		avail.Sub(avail, amt)
		frozen.Add(frozen, amt)
		if err := tx.Model(&ledgermodel.LedgerAccount{}).
			Where("id = ?", acct.ID).
			Updates(map[string]any{
				"available_amount": avail.String(),
				"frozen_amount":    frozen.String(),
			}).Error; err != nil {
			return err
		}
		created = true
		return nil
	})
	return created, err
}

func (r *SweepRepo) MarkBroadcasted(ctx context.Context, sweepID string, txHash string) error {
	if r == nil || r.db == nil {
		return errors.New("sweep repo not configured")
	}
	now := time.Now()
	return r.db.WithContext(ctx).
		Model(&sweepmodel.SweepOrder{}).
		Where("sweep_id = ?", strings.TrimSpace(sweepID)).
		Updates(map[string]any{
			"status":         sweepmodel.SweepStatusBroadcasted,
			"tx_hash":        strings.TrimSpace(txHash),
			"last_error":     "",
			"broadcasted_at": &now,
			"updated_at":     now,
		}).Error
}

func (r *SweepRepo) MarkFailed(ctx context.Context, sweepID string, errMsg string) error {
	if r == nil || r.db == nil {
		return errors.New("sweep repo not configured")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var rec sweepmodel.SweepOrder
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("sweep_id = ?", strings.TrimSpace(sweepID)).
			First(&rec).Error; err != nil {
			return err
		}
		if rec.Status == sweepmodel.SweepStatusFailed {
			return nil
		}
		if rec.Status == sweepmodel.SweepStatusConfirmed {
			return nil
		}

		amt, ok := new(big.Int).SetString(strings.TrimSpace(rec.Amount), 10)
		if !ok || amt.Sign() <= 0 {
			return fmt.Errorf("invalid sweep amount: %s", rec.Amount)
		}
		var acct ledgermodel.LedgerAccount
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("chain = ? AND address = ? AND asset_contract_address = ? AND user_id = ?",
				rec.Chain, rec.FromAddress, rec.AssetContractAddress, rec.UserID).
			First(&acct).Error; err != nil {
			return err
		}
		avail, ok := new(big.Int).SetString(strings.TrimSpace(acct.AvailableAmount), 10)
		if !ok || avail.Sign() < 0 {
			return fmt.Errorf("invalid ledger available amount: %s", acct.AvailableAmount)
		}
		frozen, ok := new(big.Int).SetString(strings.TrimSpace(acct.FrozenAmount), 10)
		if !ok || frozen.Sign() < 0 {
			return fmt.Errorf("invalid ledger frozen amount: %s", acct.FrozenAmount)
		}
		if frozen.Cmp(amt) < 0 {
			return errors.New("ledger frozen insufficient for sweep release")
		}
		frozen.Sub(frozen, amt)
		avail.Add(avail, amt)
		if err := tx.Model(&ledgermodel.LedgerAccount{}).
			Where("id = ?", acct.ID).
			Updates(map[string]any{
				"available_amount": avail.String(),
				"frozen_amount":    frozen.String(),
			}).Error; err != nil {
			return err
		}

		return tx.Model(&sweepmodel.SweepOrder{}).
			Where("id = ?", rec.ID).
			Updates(map[string]any{
				"status":         sweepmodel.SweepStatusFailed,
				"last_error":     errMsg,
				"updated_at":     time.Now(),
				"tx_hash":        "",
				"broadcasted_at": nil,
			}).Error
	})
}

func (r *SweepRepo) ListBroadcastedToConfirm(ctx context.Context, limit int) ([]sweepmodel.SweepOrder, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("sweep repo not configured")
	}
	if limit <= 0 {
		limit = 200
	}
	var out []sweepmodel.SweepOrder
	err := r.db.WithContext(ctx).
		Where("status = ? AND tx_hash <> ''", sweepmodel.SweepStatusBroadcasted).
		Order("updated_at ASC").
		Limit(limit).
		Find(&out).Error
	return out, err
}

func (r *SweepRepo) UpdateConfirmations(
	ctx context.Context,
	sweepID string,
	blockNum uint64,
	conf int,
	threshold int,
) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("sweep repo not configured")
	}
	updates := map[string]any{
		"block_number":  blockNum,
		"confirmations": conf,
		"last_error":    "",
	}
	if conf >= threshold {
		now := time.Now()
		updates["status"] = sweepmodel.SweepStatusConfirmed
		updates["confirmed_at"] = &now
	}
	res := r.db.WithContext(ctx).
		Model(&sweepmodel.SweepOrder{}).
		Where("sweep_id = ? AND status = ?", strings.TrimSpace(sweepID), sweepmodel.SweepStatusBroadcasted).
		Updates(updates)
	return res.RowsAffected > 0, res.Error
}

func (r *SweepRepo) ConfirmWithSettlement(
	ctx context.Context,
	sweepID string,
	blockNum uint64,
	conf int,
	threshold int,
	transferAssetContractAddress string,
	transferSpentAmount *big.Int,
) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("sweep repo not configured")
	}
	var updated bool
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var rec sweepmodel.SweepOrder
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("sweep_id = ?", strings.TrimSpace(sweepID)).
			First(&rec).Error; err != nil {
			return err
		}
		if rec.Status == sweepmodel.SweepStatusConfirmed {
			updated = false
			return nil
		}
		if rec.Status != sweepmodel.SweepStatusBroadcasted {
			updated = false
			return nil
		}

		amt := new(big.Int)
		if transferSpentAmount != nil && transferSpentAmount.Sign() > 0 {
			amt.Set(transferSpentAmount)
		} else if _, ok := amt.SetString(strings.TrimSpace(rec.Amount), 10); !ok || amt.Sign() <= 0 {
			return fmt.Errorf("invalid sweep amount: %s", rec.Amount)
		}

		asset := strings.TrimSpace(transferAssetContractAddress)
		if asset == "" {
			asset = strings.TrimSpace(rec.AssetContractAddress)
		}

		reserved, ok := new(big.Int).SetString(strings.TrimSpace(rec.Amount), 10)
		if !ok || reserved.Sign() <= 0 {
			return fmt.Errorf("invalid reserved sweep amount: %s", rec.Amount)
		}

		var acct ledgermodel.LedgerAccount
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("chain = ? AND address = ? AND asset_contract_address = ? AND user_id = ?",
				rec.Chain, rec.FromAddress, asset, rec.UserID).
			First(&acct).Error; err != nil {
			return err
		}
		avail, ok := new(big.Int).SetString(strings.TrimSpace(acct.AvailableAmount), 10)
		if !ok || avail.Sign() < 0 {
			return fmt.Errorf("invalid ledger available amount: %s", acct.AvailableAmount)
		}
		frozen, ok := new(big.Int).SetString(strings.TrimSpace(acct.FrozenAmount), 10)
		if !ok || frozen.Sign() < 0 {
			return fmt.Errorf("invalid ledger frozen amount: %s", acct.FrozenAmount)
		}
		if frozen.Cmp(reserved) < 0 {
			return errors.New("ledger frozen insufficient for sweep settle")
		}
		frozen.Sub(frozen, reserved)
		switch reserved.Cmp(amt) {
		case 1:
			// Reserved more than actually transferred; refund the delta.
			refund := new(big.Int).Sub(reserved, amt)
			avail.Add(avail, refund)
		case -1:
			// Rare path: actual transfer > reserved, consume shortfall from available.
			shortfall := new(big.Int).Sub(amt, reserved)
			if avail.Cmp(shortfall) < 0 {
				return errors.New("ledger available insufficient for sweep settle shortfall")
			}
			avail.Sub(avail, shortfall)
		}
		if err := tx.Model(&ledgermodel.LedgerAccount{}).
			Where("id = ?", acct.ID).
			Updates(map[string]any{
				"available_amount": avail.String(),
				"frozen_amount":    frozen.String(),
			}).Error; err != nil {
			return err
		}

		// Credit destination (normally hot wallet) to keep ledger movement balanced.
		var dst ledgermodel.LedgerAccount
		dstErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("chain = ? AND address = ? AND asset_contract_address = ?", rec.Chain, rec.ToAddress, asset).
			First(&dst).Error
		if dstErr != nil && !errors.Is(dstErr, gorm.ErrRecordNotFound) {
			return dstErr
		}
		if errors.Is(dstErr, gorm.ErrRecordNotFound) {
			dst = ledgermodel.LedgerAccount{
				UserID:               "",
				Chain:                rec.Chain,
				Address:              rec.ToAddress,
				AssetContractAddress: asset,
				AvailableAmount:      amt.String(),
				FrozenAmount:         "0",
			}
			if err := tx.Create(&dst).Error; err != nil {
				return err
			}
		} else {
			dstAvail, ok := new(big.Int).SetString(strings.TrimSpace(dst.AvailableAmount), 10)
			if !ok || dstAvail.Sign() < 0 {
				return fmt.Errorf("invalid destination ledger available amount: %s", dst.AvailableAmount)
			}
			dstAvail.Add(dstAvail, amt)
			if err := tx.Model(&ledgermodel.LedgerAccount{}).
				Where("id = ?", dst.ID).
				Update("available_amount", dstAvail.String()).Error; err != nil {
				return err
			}
		}

		updates := map[string]any{
			"status":        sweepmodel.SweepStatusConfirmed,
			"block_number":  blockNum,
			"confirmations": conf,
			"last_error":    "",
			"confirmed_at":  time.Now(),
			"updated_at":    time.Now(),
		}
		res := tx.Model(&sweepmodel.SweepOrder{}).
			Where("id = ? AND status = ?", rec.ID, sweepmodel.SweepStatusBroadcasted).
			Updates(updates)
		if res.Error != nil {
			return res.Error
		}
		updated = res.RowsAffected > 0
		return nil
	})
	return updated, err
}
