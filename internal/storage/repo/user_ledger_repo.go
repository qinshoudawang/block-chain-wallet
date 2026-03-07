package repo

import (
	"context"
	"errors"
	"strings"

	userledgermodel "wallet-system/internal/storage/model/userledger"

	"gorm.io/gorm"
)

type UserLedgerRepo struct {
	db *gorm.DB
}

func NewUserLedgerRepo(db *gorm.DB) *UserLedgerRepo {
	return &UserLedgerRepo{db: db}
}

func (r *UserLedgerRepo) GetEntryByBiz(ctx context.Context, bizType string, bizID string) (*userledgermodel.UserLedgerEntry, bool, error) {
	if r == nil || r.db == nil {
		return nil, false, errors.New("user ledger repo not configured")
	}
	var entry userledgermodel.UserLedgerEntry
	err := r.db.WithContext(ctx).
		Where("biz_type = ? AND biz_id = ?", strings.ToUpper(strings.TrimSpace(bizType)), strings.TrimSpace(bizID)).
		First(&entry).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &entry, true, nil
}
