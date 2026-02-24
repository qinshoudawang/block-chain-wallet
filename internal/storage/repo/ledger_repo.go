package repo

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"wallet-system/internal/storage/model"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const ledgerBizTypeWithdraw = "WITHDRAW"

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

// FreezeWithdraw moves available -> frozen for a withdrawal request, keyed by withdrawID for idempotency.
func (r *LedgerRepo) FreezeWithdraw(ctx context.Context, chain, address, withdrawID string, amount *big.Int) (string, error) {
	if r == nil || r.db == nil {
		return "", errors.New("ledger repo not configured")
	}
	if amount == nil || amount.Sign() <= 0 {
		return "", errors.New("invalid freeze amount")
	}

	var freezeID string
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var existing model.LedgerFreeze
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("biz_type = ? AND biz_id = ?", ledgerBizTypeWithdraw, withdrawID).
			First(&existing).Error
		if err == nil {
			if existing.Chain != chain || existing.Address != address || existing.Amount != amount.String() {
				return fmt.Errorf("ledger freeze biz conflict: withdraw_id=%s", withdrawID)
			}
			if existing.Status != model.LedgerFreezeFrozen {
				return fmt.Errorf("ledger freeze status invalid: %s", existing.Status)
			}
			freezeID = existing.FreezeID
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		var acct model.LedgerAccount
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("chain = ? AND address = ?", chain, address).
			First(&acct).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrLedgerAccountAbsent
			}
			return err
		}

		avail, err := parseWei(acct.AvailableWei)
		if err != nil {
			return fmt.Errorf("parse ledger available: %w", err)
		}
		frozen, err := parseWei(acct.FrozenWei)
		if err != nil {
			return fmt.Errorf("parse ledger frozen: %w", err)
		}
		if avail.Cmp(amount) < 0 {
			return ErrInsufficientBalance
		}

		avail.Sub(avail, amount)
		frozen.Add(frozen, amount)
		if err := tx.Model(&model.LedgerAccount{}).
			Where("id = ?", acct.ID).
			Updates(map[string]any{
				"available_wei": avail.String(),
				"frozen_wei":    frozen.String(),
			}).Error; err != nil {
			return err
		}

		freezeID = uuid.NewString()
		fz := model.LedgerFreeze{
			FreezeID: freezeID,
			BizType:  ledgerBizTypeWithdraw,
			BizID:    withdrawID,
			Chain:    chain,
			Address:  address,
			Amount:   amount.String(),
			Status:   model.LedgerFreezeFrozen,
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
		var fz model.LedgerFreeze
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("biz_type = ? AND biz_id = ?", ledgerBizTypeWithdraw, withdrawID).
			First(&fz).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		if fz.Status == model.LedgerFreezeReleased {
			return nil
		}
		if fz.Status != model.LedgerFreezeFrozen {
			return fmt.Errorf("freeze not releasable: %s", fz.Status)
		}

		var acct model.LedgerAccount
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("chain = ? AND address = ?", fz.Chain, fz.Address).
			First(&acct).Error; err != nil {
			return err
		}

		amt, err := parseWei(fz.Amount)
		if err != nil {
			return fmt.Errorf("parse freeze amount: %w", err)
		}
		avail, err := parseWei(acct.AvailableWei)
		if err != nil {
			return fmt.Errorf("parse ledger available: %w", err)
		}
		frozen, err := parseWei(acct.FrozenWei)
		if err != nil {
			return fmt.Errorf("parse ledger frozen: %w", err)
		}
		if frozen.Cmp(amt) < 0 {
			return errors.New("ledger frozen amount underflow")
		}

		frozen.Sub(frozen, amt)
		avail.Add(avail, amt)
		if err := tx.Model(&model.LedgerAccount{}).
			Where("id = ?", acct.ID).
			Updates(map[string]any{
				"available_wei": avail.String(),
				"frozen_wei":    frozen.String(),
			}).Error; err != nil {
			return err
		}

		now := time.Now()
		return tx.Model(&model.LedgerFreeze{}).
			Where("id = ?", fz.ID).
			Updates(map[string]any{
				"status":      model.LedgerFreezeReleased,
				"released_at": &now,
				"updated_at":  now,
			}).Error
	})
}

// ConsumeWithdrawFreeze finalizes a successful withdrawal by reducing frozen balance only.
func (r *LedgerRepo) ConsumeWithdrawFreeze(ctx context.Context, withdrawID string) error {
	if r == nil || r.db == nil {
		return errors.New("ledger repo not configured")
	}

	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var fz model.LedgerFreeze
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("biz_type = ? AND biz_id = ?", ledgerBizTypeWithdraw, withdrawID).
			First(&fz).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		if fz.Status == model.LedgerFreezeConsumed {
			return nil
		}
		if fz.Status != model.LedgerFreezeFrozen {
			return fmt.Errorf("freeze not consumable: %s", fz.Status)
		}

		var acct model.LedgerAccount
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("chain = ? AND address = ?", fz.Chain, fz.Address).
			First(&acct).Error; err != nil {
			return err
		}

		amt, err := parseWei(fz.Amount)
		if err != nil {
			return fmt.Errorf("parse freeze amount: %w", err)
		}
		frozen, err := parseWei(acct.FrozenWei)
		if err != nil {
			return fmt.Errorf("parse ledger frozen: %w", err)
		}
		if frozen.Cmp(amt) < 0 {
			return errors.New("ledger frozen amount underflow")
		}

		frozen.Sub(frozen, amt)
		if err := tx.Model(&model.LedgerAccount{}).
			Where("id = ?", acct.ID).
			Updates(map[string]any{
				"frozen_wei": frozen.String(),
			}).Error; err != nil {
			return err
		}

		now := time.Now()
		return tx.Model(&model.LedgerFreeze{}).
			Where("id = ?", fz.ID).
			Updates(map[string]any{
				"status":      model.LedgerFreezeConsumed,
				"consumed_at": &now,
				"updated_at":  now,
			}).Error
	})
}

// SettleWithdrawFreeze finalizes a withdrawal using actual spent amount.
// It consumes the whole reserved freeze and refunds/deducts the delta to available.
func (r *LedgerRepo) SettleWithdrawFreeze(ctx context.Context, withdrawID string, actualSpent *big.Int) error {
	if r == nil || r.db == nil {
		return errors.New("ledger repo not configured")
	}
	if actualSpent == nil || actualSpent.Sign() < 0 {
		return errors.New("invalid actual spent amount")
	}

	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return r.settleWithdrawFreezeWithDB(tx, withdrawID, actualSpent)
	})
}

func (r *LedgerRepo) settleWithdrawFreezeWithDB(db *gorm.DB, withdrawID string, actualSpent *big.Int) error {
	if actualSpent == nil || actualSpent.Sign() < 0 {
		return errors.New("invalid actual spent amount")
	}
	tx := db
	var fz model.LedgerFreeze
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("biz_type = ? AND biz_id = ?", ledgerBizTypeWithdraw, withdrawID).
		First(&fz).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}
	if fz.Status == model.LedgerFreezeConsumed {
		return nil
	}
	if fz.Status != model.LedgerFreezeFrozen {
		return fmt.Errorf("freeze not settleable: %s", fz.Status)
	}

	var acct model.LedgerAccount
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("chain = ? AND address = ?", fz.Chain, fz.Address).
		First(&acct).Error; err != nil {
		return err
	}

	reserved, err := parseWei(fz.Amount)
	if err != nil {
		return fmt.Errorf("parse freeze amount: %w", err)
	}
	avail, err := parseWei(acct.AvailableWei)
	if err != nil {
		return fmt.Errorf("parse ledger available: %w", err)
	}
	frozen, err := parseWei(acct.FrozenWei)
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

	if err := tx.Model(&model.LedgerAccount{}).
		Where("id = ?", acct.ID).
		Updates(map[string]any{
			"available_wei": avail.String(),
			"frozen_wei":    frozen.String(),
		}).Error; err != nil {
		return err
	}

	now := time.Now()
	return tx.Model(&model.LedgerFreeze{}).
		Where("id = ?", fz.ID).
		Updates(map[string]any{
			"status":      model.LedgerFreezeConsumed,
			"consumed_at": &now,
			"updated_at":  now,
		}).Error
}

func parseWei(v string) (*big.Int, error) {
	n := new(big.Int)
	if _, ok := n.SetString(v, 10); !ok || n.Sign() < 0 {
		return nil, fmt.Errorf("invalid wei amount: %q", v)
	}
	return n, nil
}
