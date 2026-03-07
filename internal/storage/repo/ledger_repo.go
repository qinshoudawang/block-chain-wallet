package repo

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	ledgermodel "wallet-system/internal/storage/model/ledger"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrInsufficientBalance = errors.New("insufficient ledger balance")
	ErrLedgerAccountAbsent = errors.New("ledger account not found")
)

type LedgerRepo struct {
	db *gorm.DB
}

func NewLedgerRepo(db *gorm.DB) *LedgerRepo {
	return &LedgerRepo{db: db}
}

func (r *LedgerRepo) ListFreezesByBiz(ctx context.Context, bizType string, bizID string) ([]ledgermodel.LedgerFreeze, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("ledger repo not configured")
	}
	var out []ledgermodel.LedgerFreeze
	err := r.db.WithContext(ctx).
		Where("biz_type = ? AND biz_id = ?",
			strings.ToUpper(strings.TrimSpace(bizType)),
			strings.TrimSpace(bizID),
		).
		Order("id ASC").
		Find(&out).Error
	return out, err
}

func (r *LedgerRepo) GetByChainAddressAsset(
	ctx context.Context,
	chain string,
	address string,
	assetContractAddress string,
) (*ledgermodel.LedgerAccount, bool, error) {
	if r == nil || r.db == nil {
		return nil, false, errors.New("ledger repo not configured")
	}
	var acct ledgermodel.LedgerAccount
	err := r.db.WithContext(ctx).
		Where("chain = ? AND address = ? AND asset_contract_address = ?",
			strings.ToLower(strings.TrimSpace(chain)),
			strings.TrimSpace(address),
			normalizeAssetContractAddress(assetContractAddress),
		).
		First(&acct).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &acct, true, nil
}

// FreezeWithdraw moves available -> frozen for a withdrawal request and asset, keyed by withdrawID for idempotency.
func (r *LedgerRepo) FreezeWithdraw(
	ctx context.Context,
	chain string,
	address string,
	assetContractAddress string,
	withdrawID string,
	amount *big.Int,
) (string, error) {
	if r == nil || r.db == nil {
		return "", errors.New("ledger repo not configured")
	}
	if amount == nil || amount.Sign() <= 0 {
		return "", errors.New("invalid freeze amount")
	}
	assetContractAddress = normalizeAssetContractAddress(assetContractAddress)

	var freezeID string
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var existing ledgermodel.LedgerFreeze
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("biz_type = ? AND biz_id = ? AND asset_contract_address = ?", ledgermodel.LedgerBizTypeWithdraw, withdrawID, assetContractAddress).
			First(&existing).Error
		if err == nil {
			if existing.Chain != chain || existing.Address != address || existing.Amount != amount.String() || existing.AssetContractAddress != assetContractAddress {
				return fmt.Errorf("ledger freeze biz conflict: withdraw_id=%s", withdrawID)
			}
			if existing.Status != ledgermodel.LedgerFreezeFrozen {
				return fmt.Errorf("ledger freeze status invalid: %s", existing.Status)
			}
			freezeID = existing.FreezeID
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		var acct ledgermodel.LedgerAccount
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("chain = ? AND address = ? AND asset_contract_address = ?", chain, address, assetContractAddress).
			First(&acct).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrLedgerAccountAbsent
			}
			return err
		}

		avail, err := parseAmount(acct.AvailableAmount)
		if err != nil {
			return fmt.Errorf("parse ledger available: %w", err)
		}
		frozen, err := parseAmount(acct.FrozenAmount)
		if err != nil {
			return fmt.Errorf("parse ledger frozen: %w", err)
		}
		if avail.Cmp(amount) < 0 {
			return ErrInsufficientBalance
		}

		avail.Sub(avail, amount)
		frozen.Add(frozen, amount)
		if err := tx.Model(&ledgermodel.LedgerAccount{}).
			Where("id = ?", acct.ID).
			Updates(map[string]any{
				"available_amount": avail.String(),
				"frozen_amount":    frozen.String(),
			}).Error; err != nil {
			return err
		}

		freezeID = uuid.NewString()
		fz := ledgermodel.LedgerFreeze{
			FreezeID:             freezeID,
			BizType:              ledgermodel.LedgerBizTypeWithdraw,
			BizID:                withdrawID,
			Chain:                chain,
			Address:              address,
			AssetContractAddress: assetContractAddress,
			Amount:               amount.String(),
			Status:               ledgermodel.LedgerFreezeFrozen,
		}
		return tx.Create(&fz).Error
	})
	if err != nil {
		return "", err
	}
	return freezeID, nil
}

// ReleaseWithdrawFreeze compensates a failed pre-sign flow by moving frozen -> available.
func (r *LedgerRepo) ReleaseWithdrawFreeze(ctx context.Context, withdrawID string) error {
	if r == nil || r.db == nil {
		return errors.New("ledger repo not configured")
	}

	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var freezes []ledgermodel.LedgerFreeze
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("biz_type = ? AND biz_id = ?", ledgermodel.LedgerBizTypeWithdraw, withdrawID).
			Find(&freezes).Error; err != nil {
			return err
		}
		if len(freezes) == 0 {
			return nil
		}
		now := time.Now()
		for _, fz := range freezes {
			if fz.Status == ledgermodel.LedgerFreezeReleased {
				continue
			}
			if fz.Status != ledgermodel.LedgerFreezeFrozen {
				return fmt.Errorf("freeze not releasable: %s", fz.Status)
			}

			var acct ledgermodel.LedgerAccount
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("chain = ? AND address = ? AND asset_contract_address = ?", fz.Chain, fz.Address, fz.AssetContractAddress).
				First(&acct).Error; err != nil {
				return err
			}

			amt, err := parseAmount(fz.Amount)
			if err != nil {
				return fmt.Errorf("parse freeze amount: %w", err)
			}
			avail, err := parseAmount(acct.AvailableAmount)
			if err != nil {
				return fmt.Errorf("parse ledger available: %w", err)
			}
			frozen, err := parseAmount(acct.FrozenAmount)
			if err != nil {
				return fmt.Errorf("parse ledger frozen: %w", err)
			}
			if frozen.Cmp(amt) < 0 {
				return errors.New("ledger frozen amount underflow")
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

			if err := tx.Model(&ledgermodel.LedgerFreeze{}).
				Where("id = ?", fz.ID).
				Updates(map[string]any{
					"status":      ledgermodel.LedgerFreezeReleased,
					"released_at": &now,
					"updated_at":  now,
				}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// ConsumeWithdrawFreeze finalizes a successful withdrawal by reducing frozen balance only.
func (r *LedgerRepo) ConsumeWithdrawFreeze(ctx context.Context, withdrawID string) error {
	if r == nil || r.db == nil {
		return errors.New("ledger repo not configured")
	}

	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var freezes []ledgermodel.LedgerFreeze
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("biz_type = ? AND biz_id = ?", ledgermodel.LedgerBizTypeWithdraw, withdrawID).
			Find(&freezes).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		if len(freezes) == 0 {
			return nil
		}
		now := time.Now()
		for _, fz := range freezes {
			if fz.Status == ledgermodel.LedgerFreezeConsumed {
				continue
			}
			if fz.Status != ledgermodel.LedgerFreezeFrozen {
				return fmt.Errorf("freeze not consumable: %s", fz.Status)
			}

			var acct ledgermodel.LedgerAccount
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("chain = ? AND address = ? AND asset_contract_address = ?", fz.Chain, fz.Address, fz.AssetContractAddress).
				First(&acct).Error; err != nil {
				return err
			}

			amt, err := parseAmount(fz.Amount)
			if err != nil {
				return fmt.Errorf("parse freeze amount: %w", err)
			}
			frozen, err := parseAmount(acct.FrozenAmount)
			if err != nil {
				return fmt.Errorf("parse ledger frozen: %w", err)
			}
			if frozen.Cmp(amt) < 0 {
				return errors.New("ledger frozen amount underflow")
			}

			frozen.Sub(frozen, amt)
			if err := tx.Model(&ledgermodel.LedgerAccount{}).
				Where("id = ?", acct.ID).
				Updates(map[string]any{
					"frozen_amount": frozen.String(),
				}).Error; err != nil {
				return err
			}

			if err := tx.Model(&ledgermodel.LedgerFreeze{}).
				Where("id = ?", fz.ID).
				Updates(map[string]any{
					"status":      ledgermodel.LedgerFreezeConsumed,
					"consumed_at": &now,
					"updated_at":  now,
				}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// SettleWithdrawFreeze finalizes a withdrawal using actual spent amount.
// It consumes the whole reserved freeze and refunds/deducts the delta to available.
func (r *LedgerRepo) SettleWithdrawFreeze(ctx context.Context, withdrawID string, assetContractAddress string, actualSpent *big.Int) error {
	if r == nil || r.db == nil {
		return errors.New("ledger repo not configured")
	}
	if actualSpent == nil || actualSpent.Sign() < 0 {
		return errors.New("invalid actual spent amount")
	}
	assetContractAddress = normalizeAssetContractAddress(assetContractAddress)

	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return r.settleWithdrawFreezeWithDB(tx, withdrawID, assetContractAddress, actualSpent)
	})
}

func (r *LedgerRepo) settleWithdrawFreezeWithDB(db *gorm.DB, withdrawID string, assetContractAddress string, actualSpent *big.Int) error {
	assetContractAddress = normalizeAssetContractAddress(assetContractAddress)
	if actualSpent == nil {
		actualSpent = big.NewInt(0)
	}
	if actualSpent == nil || actualSpent.Sign() < 0 {
		return errors.New("invalid actual spent amount")
	}
	tx := db
	var fz ledgermodel.LedgerFreeze
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("biz_type = ? AND biz_id = ? AND asset_contract_address = ?", ledgermodel.LedgerBizTypeWithdraw, withdrawID, assetContractAddress).
		First(&fz).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}
	if fz.Status == ledgermodel.LedgerFreezeConsumed {
		return nil
	}
	if fz.Status != ledgermodel.LedgerFreezeFrozen {
		return fmt.Errorf("freeze not settleable: %s", fz.Status)
	}

	var acct ledgermodel.LedgerAccount
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("chain = ? AND address = ? AND asset_contract_address = ?", fz.Chain, fz.Address, fz.AssetContractAddress).
		First(&acct).Error; err != nil {
		return err
	}

	reserved, err := parseAmount(fz.Amount)
	if err != nil {
		return fmt.Errorf("parse freeze amount: %w", err)
	}
	avail, err := parseAmount(acct.AvailableAmount)
	if err != nil {
		return fmt.Errorf("parse ledger available: %w", err)
	}
	frozen, err := parseAmount(acct.FrozenAmount)
	if err != nil {
		return fmt.Errorf("parse ledger frozen: %w", err)
	}
	if frozen.Cmp(reserved) < 0 {
		return errors.New("ledger frozen amount underflow")
	}

	// Always clear reserved frozen amount once the chain result is final.
	frozen.Sub(frozen, reserved)

	switch reserved.Cmp(actualSpent) {
	case 1:
		refund := new(big.Int).Sub(reserved, actualSpent)
		avail.Add(avail, refund)
	case -1:
		shortfall := new(big.Int).Sub(actualSpent, reserved)
		if avail.Cmp(shortfall) < 0 {
			return errors.New("ledger available insufficient for settlement shortfall")
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

	now := time.Now()
	return tx.Model(&ledgermodel.LedgerFreeze{}).
		Where("id = ?", fz.ID).
		Updates(map[string]any{
			"status":      ledgermodel.LedgerFreezeConsumed,
			"consumed_at": &now,
			"updated_at":  now,
		}).Error
}

func parseAmount(v string) (*big.Int, error) {
	n := new(big.Int)
	if _, ok := n.SetString(v, 10); !ok || n.Sign() < 0 {
		return nil, fmt.Errorf("invalid amount: %q", v)
	}
	return n, nil
}

func normalizeAssetContractAddress(v string) string {
	return strings.TrimSpace(v)
}

func (r *LedgerRepo) settleSystemTransferWithDB(
	db *gorm.DB,
	chain string,
	fromAddr string,
	toAddr string,
	assetContractAddress string,
	amount *big.Int,
	toUserID string,
) error {
	if amount == nil || amount.Sign() <= 0 {
		return errors.New("invalid system transfer amount")
	}
	tx := db
	chain = strings.ToLower(strings.TrimSpace(chain))
	fromAddr = strings.TrimSpace(fromAddr)
	toAddr = strings.TrimSpace(toAddr)
	assetContractAddress = normalizeAssetContractAddress(assetContractAddress)
	toUserID = strings.TrimSpace(toUserID)

	var from ledgermodel.LedgerAccount
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("chain = ? AND address = ? AND asset_contract_address = ?", chain, fromAddr, assetContractAddress).
		First(&from).Error; err != nil {
		return err
	}
	fromAvail, err := parseAmount(from.AvailableAmount)
	if err != nil {
		return fmt.Errorf("parse source available: %w", err)
	}
	if fromAvail.Cmp(amount) < 0 {
		return errors.New("source available insufficient for system transfer")
	}
	fromAvail.Sub(fromAvail, amount)
	if err := tx.Model(&ledgermodel.LedgerAccount{}).
		Where("id = ?", from.ID).
		Update("available_amount", fromAvail.String()).Error; err != nil {
		return err
	}

	var to ledgermodel.LedgerAccount
	toErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("chain = ? AND address = ? AND asset_contract_address = ?", chain, toAddr, assetContractAddress).
		First(&to).Error
	if toErr != nil && !errors.Is(toErr, gorm.ErrRecordNotFound) {
		return toErr
	}
	if errors.Is(toErr, gorm.ErrRecordNotFound) {
		to = ledgermodel.LedgerAccount{
			UserID:               toUserID,
			Chain:                chain,
			Address:              toAddr,
			AssetContractAddress: assetContractAddress,
			AvailableAmount:      amount.String(),
			FrozenAmount:         "0",
		}
		return tx.Create(&to).Error
	}
	toAvail, err := parseAmount(to.AvailableAmount)
	if err != nil {
		return fmt.Errorf("parse destination available: %w", err)
	}
	toAvail.Add(toAvail, amount)
	updates := map[string]any{
		"available_amount": toAvail.String(),
	}
	if strings.TrimSpace(to.UserID) == "" && toUserID != "" {
		updates["user_id"] = toUserID
	}
	return tx.Model(&ledgermodel.LedgerAccount{}).
		Where("id = ?", to.ID).
		Updates(updates).Error
}
