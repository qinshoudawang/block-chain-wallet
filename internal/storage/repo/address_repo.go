package repo

import (
	"context"
	"errors"
	"wallet-system/internal/helpers"
	"wallet-system/internal/storage/model"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type AddressRepo struct {
	db *gorm.DB
}

func NewAddressRepo(db *gorm.DB) *AddressRepo {
	return &AddressRepo{db: db}
}

func (r *AddressRepo) InTx(ctx context.Context, fn func(tx *gorm.DB) error) error {
	if r == nil || r.db == nil {
		return errors.New("address repo not configured")
	}
	return r.db.WithContext(ctx).Transaction(fn)
}

func (r *AddressRepo) CreateUserAddressTx(tx *gorm.DB, addr *model.UserAddress) error {
	return tx.Create(addr).Error
}

func (r *AddressRepo) AllocateIndexTx(tx *gorm.DB, spec helpers.ChainSpec) (*model.HDWallet, uint32, error) {
	var wallet model.HDWallet
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("chain = ?", spec.CanonicalChain).
		First(&wallet).Error
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, 0, err
		}
		wallet = model.HDWallet{
			Chain:     spec.CanonicalChain,
			Purpose:   spec.Purpose,
			CoinType:  spec.CoinType,
			Account:   spec.Account,
			NextIndex: 0,
		}
		if err := tx.Create(&wallet).Error; err != nil {
			return nil, 0, err
		}
		// lock fresh row for consistent update path
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", wallet.ID).
			First(&wallet).Error; err != nil {
			return nil, 0, err
		}
	}

	index := wallet.NextIndex
	if err := tx.Model(&model.HDWallet{}).
		Where("id = ?", wallet.ID).
		Update("next_index", index+1).Error; err != nil {
		return nil, 0, err
	}
	return &wallet, index, nil
}
