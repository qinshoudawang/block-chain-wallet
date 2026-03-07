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

func (r *SweepRepo) GetBroadcastedByTxHash(ctx context.Context, chain string, txHash string) (*sweepmodel.SweepOrder, bool, error) {
	if r == nil || r.db == nil {
		return nil, false, errors.New("sweep repo not configured")
	}
	var out sweepmodel.SweepOrder
	err := r.db.WithContext(ctx).
		Where("chain = ? AND tx_hash = ? AND status = ?",
			strings.ToLower(strings.TrimSpace(chain)),
			strings.TrimSpace(txHash),
			sweepmodel.SweepStatusBroadcasted,
		).
		First(&out).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &out, true, nil
}

func (r *SweepRepo) GetTrackedByTxHash(ctx context.Context, chain string, txHash string) (*sweepmodel.SweepOrder, bool, error) {
	if r == nil || r.db == nil {
		return nil, false, errors.New("sweep repo not configured")
	}
	var out sweepmodel.SweepOrder
	err := r.db.WithContext(ctx).
		Where("chain = ? AND tx_hash = ? AND status IN ?",
			strings.ToLower(strings.TrimSpace(chain)),
			strings.TrimSpace(txHash),
			[]sweepmodel.SweepStatus{sweepmodel.SweepStatusBroadcasted, sweepmodel.SweepStatusConfirmed, sweepmodel.SweepStatusFailed},
		).
		Order("id DESC").
		First(&out).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &out, true, nil
}

func (r *SweepRepo) ExistsTrackedTxHash(ctx context.Context, chain string, txHash string) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("sweep repo not configured")
	}
	var cnt int64
	err := r.db.WithContext(ctx).
		Model(&sweepmodel.SweepOrder{}).
		Where("chain = ? AND tx_hash = ? AND status IN ?",
			strings.ToLower(strings.TrimSpace(chain)),
			strings.TrimSpace(txHash),
			[]sweepmodel.SweepStatus{sweepmodel.SweepStatusBroadcasted, sweepmodel.SweepStatusConfirmed, sweepmodel.SweepStatusFailed},
		).
		Count(&cnt).Error
	return cnt > 0, err
}

func (r *SweepRepo) ListTrackedTxHashes(ctx context.Context, chain string, limit int) ([]string, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("sweep repo not configured")
	}
	if limit <= 0 {
		limit = 500
	}
	var out []string
	err := r.db.WithContext(ctx).
		Model(&sweepmodel.SweepOrder{}).
		Distinct("tx_hash").
		Where("chain = ? AND tx_hash <> '' AND status IN ?",
			strings.ToLower(strings.TrimSpace(chain)),
			[]sweepmodel.SweepStatus{sweepmodel.SweepStatusBroadcasted, sweepmodel.SweepStatusConfirmed, sweepmodel.SweepStatusFailed},
		).
		Limit(limit).
		Pluck("tx_hash", &out).Error
	return out, err
}

func (r *SweepRepo) FailWithSettlement(
	ctx context.Context,
	sweepID string,
	transferAssetContractAddress string,
	transferSpentAmount *big.Int,
	networkFeeAssetContractAddress string,
	networkFeeAmount *big.Int,
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
		if rec.Status == sweepmodel.SweepStatusFailed {
			updated = false
			return nil
		}
		if rec.Status != sweepmodel.SweepStatusBroadcasted {
			updated = false
			return nil
		}
		amt := big.NewInt(0)
		if transferSpentAmount != nil {
			amt.Set(transferSpentAmount)
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
		refund := new(big.Int).Sub(reserved, amt)
		if refund.Sign() > 0 {
			avail.Add(avail, refund)
		}
		if err := tx.Model(&ledgermodel.LedgerAccount{}).
			Where("id = ?", acct.ID).
			Updates(map[string]any{"available_amount": avail.String(), "frozen_amount": frozen.String()}).Error; err != nil {
			return err
		}
		if err := settleSweepNetworkFeeWithDB(tx, rec.Chain, rec.FromAddress, rec.UserID, networkFeeAssetContractAddress, networkFeeAmount); err != nil {
			return err
		}
		res := tx.Model(&sweepmodel.SweepOrder{}).
			Where("id = ? AND status = ?", rec.ID, sweepmodel.SweepStatusBroadcasted).
			Updates(map[string]any{
				"status":                             sweepmodel.SweepStatusFailed,
				"last_error":                         "onchain execution failed",
				"updated_at":                         time.Now(),
				"actual_spent_amount":                amt.String(),
				"transfer_asset_contract_address":    asset,
				"network_fee_asset_contract_address": strings.TrimSpace(networkFeeAssetContractAddress),
				"network_fee_amount":                 bigIntString(networkFeeAmount),
			})
		if res.Error != nil {
			return res.Error
		}
		updated = res.RowsAffected > 0
		return nil
	})
	return updated, err
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

func (r *SweepRepo) ListFinalizedAfterID(ctx context.Context, chain string, lastID uint64, limit int) ([]sweepmodel.SweepOrder, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("sweep repo not configured")
	}
	if limit <= 0 {
		limit = 500
	}
	chain = strings.ToLower(strings.TrimSpace(chain))
	var out []sweepmodel.SweepOrder
	err := r.db.WithContext(ctx).
		Where("chain = ? AND id > ? AND status IN ?", chain, lastID, []sweepmodel.SweepStatus{
			sweepmodel.SweepStatusConfirmed,
			sweepmodel.SweepStatusFailed,
		}).
		Order("id ASC").
		Limit(limit).
		Find(&out).Error
	return out, err
}

func (r *SweepRepo) ListReorgAffectedAfterID(ctx context.Context, chain string, fromBlock uint64, lastID uint64, limit int) ([]sweepmodel.SweepOrder, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("sweep repo not configured")
	}
	if limit <= 0 {
		limit = 200
	}
	chain = strings.ToLower(strings.TrimSpace(chain))
	var out []sweepmodel.SweepOrder
	err := r.db.WithContext(ctx).
		Where("chain = ? AND id > ? AND tx_hash <> '' AND block_number IS NOT NULL AND block_number >= ? AND status IN ?", chain, lastID, fromBlock, []sweepmodel.SweepStatus{
			sweepmodel.SweepStatusConfirmed,
			sweepmodel.SweepStatusFailed,
		}).
		Order("id ASC").
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
	networkFeeAssetContractAddress string,
	networkFeeAmount *big.Int,
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
		if err := settleSweepNetworkFeeWithDB(
			tx,
			rec.Chain,
			rec.FromAddress,
			rec.UserID,
			networkFeeAssetContractAddress,
			networkFeeAmount,
		); err != nil {
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
			"status":                             sweepmodel.SweepStatusConfirmed,
			"block_number":                       blockNum,
			"confirmations":                      conf,
			"last_error":                         "",
			"confirmed_at":                       time.Now(),
			"updated_at":                         time.Now(),
			"actual_spent_amount":                amt.String(),
			"transfer_asset_contract_address":    asset,
			"network_fee_asset_contract_address": strings.TrimSpace(networkFeeAssetContractAddress),
			"network_fee_amount":                 bigIntString(networkFeeAmount),
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

func (r *SweepRepo) RevertConfirmationWithSettlement(ctx context.Context, sweepID string) (bool, error) {
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
		if rec.Status == sweepmodel.SweepStatusBroadcasted {
			updated = false
			return nil
		}
		if rec.Status != sweepmodel.SweepStatusConfirmed && rec.Status != sweepmodel.SweepStatusFailed {
			updated = false
			return nil
		}

		actualSpent := big.NewInt(0)
		if v, ok := new(big.Int).SetString(strings.TrimSpace(rec.ActualSpentAmount), 10); ok && v.Sign() >= 0 {
			actualSpent = v
		} else if v, ok := new(big.Int).SetString(strings.TrimSpace(rec.Amount), 10); ok && v.Sign() > 0 {
			actualSpent = v
		}
		reserved, ok := new(big.Int).SetString(strings.TrimSpace(rec.Amount), 10)
		if !ok || reserved.Sign() <= 0 {
			return fmt.Errorf("invalid reserved sweep amount: %s", rec.Amount)
		}
		asset := strings.TrimSpace(rec.TransferAssetContractAddress)
		if asset == "" {
			asset = strings.TrimSpace(rec.AssetContractAddress)
		}
		var src ledgermodel.LedgerAccount
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("chain = ? AND address = ? AND asset_contract_address = ? AND user_id = ?",
				rec.Chain, rec.FromAddress, asset, rec.UserID).
			First(&src).Error; err != nil {
			return err
		}
		srcAvail, ok := new(big.Int).SetString(strings.TrimSpace(src.AvailableAmount), 10)
		if !ok || srcAvail.Sign() < 0 {
			return fmt.Errorf("invalid ledger available amount: %s", src.AvailableAmount)
		}
		srcFrozen, ok := new(big.Int).SetString(strings.TrimSpace(src.FrozenAmount), 10)
		if !ok || srcFrozen.Sign() < 0 {
			return fmt.Errorf("invalid ledger frozen amount: %s", src.FrozenAmount)
		}
		switch reserved.Cmp(actualSpent) {
		case 1:
			refund := new(big.Int).Sub(reserved, actualSpent)
			if srcAvail.Cmp(refund) < 0 {
				return errors.New("ledger available insufficient for sweep settlement revert refund")
			}
			srcAvail.Sub(srcAvail, refund)
		case -1:
			shortfall := new(big.Int).Sub(actualSpent, reserved)
			srcAvail.Add(srcAvail, shortfall)
		}
		srcFrozen.Add(srcFrozen, reserved)
		if err := tx.Model(&ledgermodel.LedgerAccount{}).
			Where("id = ?", src.ID).
			Updates(map[string]any{
				"available_amount": srcAvail.String(),
				"frozen_amount":    srcFrozen.String(),
			}).Error; err != nil {
			return err
		}

		if err := revertSweepNetworkFeeWithDB(tx, rec.Chain, rec.FromAddress, rec.UserID, rec.NetworkFeeAssetContractAddress, parseBigOrZero(rec.NetworkFeeAmount)); err != nil {
			return err
		}

		if rec.Status == sweepmodel.SweepStatusConfirmed {
			var dst ledgermodel.LedgerAccount
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("chain = ? AND address = ? AND asset_contract_address = ?", rec.Chain, rec.ToAddress, asset).
				First(&dst).Error; err != nil {
				return err
			}
			dstAvail, ok := new(big.Int).SetString(strings.TrimSpace(dst.AvailableAmount), 10)
			if !ok || dstAvail.Sign() < 0 {
				return fmt.Errorf("invalid destination ledger available amount: %s", dst.AvailableAmount)
			}
			if dstAvail.Cmp(actualSpent) < 0 {
				return errors.New("destination available insufficient for sweep settlement revert")
			}
			dstAvail.Sub(dstAvail, actualSpent)
			if err := tx.Model(&ledgermodel.LedgerAccount{}).
				Where("id = ?", dst.ID).
				Update("available_amount", dstAvail.String()).Error; err != nil {
				return err
			}
		}

		res := tx.Model(&sweepmodel.SweepOrder{}).
			Where("id = ? AND status IN ?", rec.ID, []sweepmodel.SweepStatus{sweepmodel.SweepStatusConfirmed, sweepmodel.SweepStatusFailed}).
			Updates(map[string]any{
				"status":                             sweepmodel.SweepStatusBroadcasted,
				"block_number":                       nil,
				"confirmations":                      nil,
				"confirmed_at":                       nil,
				"actual_spent_amount":                "",
				"transfer_asset_contract_address":    "",
				"network_fee_asset_contract_address": "",
				"network_fee_amount":                 "",
				"last_error":                         "",
				"updated_at":                         time.Now(),
			})
		if res.Error != nil {
			return res.Error
		}
		updated = res.RowsAffected > 0
		return nil
	})
	return updated, err
}

func settleSweepNetworkFeeWithDB(
	tx *gorm.DB,
	chain string,
	fromAddress string,
	userID string,
	networkFeeAssetContractAddress string,
	networkFeeAmount *big.Int,
) error {
	if networkFeeAmount == nil || networkFeeAmount.Sign() <= 0 {
		return nil
	}
	feeAsset := strings.TrimSpace(networkFeeAssetContractAddress)
	var feeAcct ledgermodel.LedgerAccount
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("chain = ? AND address = ? AND asset_contract_address = ? AND user_id = ?",
			chain, fromAddress, feeAsset, userID).
		First(&feeAcct).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("missing fee ledger account for sweep: chain=%s address=%s user_id=%s asset=%s", chain, fromAddress, userID, feeAsset)
		}
		return err
	}
	feeAvail, ok := new(big.Int).SetString(strings.TrimSpace(feeAcct.AvailableAmount), 10)
	if !ok || feeAvail.Sign() < 0 {
		return fmt.Errorf("invalid fee ledger available amount: %s", feeAcct.AvailableAmount)
	}
	if feeAvail.Cmp(networkFeeAmount) < 0 {
		return errors.New("ledger available insufficient for sweep network fee")
	}
	feeAvail.Sub(feeAvail, networkFeeAmount)
	return tx.Model(&ledgermodel.LedgerAccount{}).
		Where("id = ?", feeAcct.ID).
		Update("available_amount", feeAvail.String()).Error
}

func revertSweepNetworkFeeWithDB(
	tx *gorm.DB,
	chain string,
	fromAddress string,
	userID string,
	networkFeeAssetContractAddress string,
	networkFeeAmount *big.Int,
) error {
	if networkFeeAmount == nil || networkFeeAmount.Sign() <= 0 {
		return nil
	}
	feeAsset := strings.TrimSpace(networkFeeAssetContractAddress)
	var feeAcct ledgermodel.LedgerAccount
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("chain = ? AND address = ? AND asset_contract_address = ? AND user_id = ?",
			chain, fromAddress, feeAsset, userID).
		First(&feeAcct).Error; err != nil {
		return err
	}
	feeAvail, ok := new(big.Int).SetString(strings.TrimSpace(feeAcct.AvailableAmount), 10)
	if !ok || feeAvail.Sign() < 0 {
		return fmt.Errorf("invalid fee ledger available amount: %s", feeAcct.AvailableAmount)
	}
	feeAvail.Add(feeAvail, networkFeeAmount)
	return tx.Model(&ledgermodel.LedgerAccount{}).
		Where("id = ?", feeAcct.ID).
		Update("available_amount", feeAvail.String()).Error
}

func bigIntString(v *big.Int) string {
	if v == nil {
		return ""
	}
	return v.String()
}

func parseBigOrZero(v string) *big.Int {
	out, ok := new(big.Int).SetString(strings.TrimSpace(v), 10)
	if !ok {
		return big.NewInt(0)
	}
	return out
}
