package repo

import (
	"context"
	"errors"
	"strings"
	"wallet-system/internal/helpers"
	addressmodel "wallet-system/internal/storage/model/address"

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

func (r *AddressRepo) CreateUserAddressTx(tx *gorm.DB, addr *addressmodel.UserAddress) error {
	return tx.Create(addr).Error
}

func (r *AddressRepo) AllocateIndexTx(tx *gorm.DB, spec helpers.ChainSpec) (*addressmodel.HDWallet, uint32, error) {
	var wallet addressmodel.HDWallet
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("chain = ?", spec.CanonicalChain).
		First(&wallet).Error
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, 0, err
		}
		wallet = addressmodel.HDWallet{
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
	if err := tx.Model(&addressmodel.HDWallet{}).
		Where("id = ?", wallet.ID).
		Update("next_index", index+1).Error; err != nil {
		return nil, 0, err
	}
	return &wallet, index, nil
}

func (r *AddressRepo) ListByChain(ctx context.Context, chain string) ([]addressmodel.UserAddress, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("address repo not configured")
	}
	var out []addressmodel.UserAddress
	err := r.db.WithContext(ctx).
		Where("chain = ?", chain).
		Find(&out).Error
	return out, err
}

func (r *AddressRepo) FindUserIDByChainAddress(ctx context.Context, chain string, address string) (string, error) {
	if r == nil || r.db == nil {
		return "", errors.New("address repo not configured")
	}
	chain = strings.TrimSpace(chain)
	address = strings.TrimSpace(address)
	var out addressmodel.UserAddress
	if err := r.db.WithContext(ctx).
		Select("user_id").
		Where("chain = ? AND lower(address) = lower(?)", chain, address).
		First(&out).Error; err != nil {
		return "", err
	}
	return out.UserID, nil
}

func (r *AddressRepo) GetByChainAddress(ctx context.Context, chain string, address string) (*addressmodel.UserAddress, bool, error) {
	if r == nil || r.db == nil {
		return nil, false, errors.New("address repo not configured")
	}
	chain = strings.TrimSpace(chain)
	address = strings.TrimSpace(address)
	var out addressmodel.UserAddress
	if err := r.db.WithContext(ctx).
		Where("chain = ? AND lower(address) = lower(?)", chain, address).
		First(&out).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &out, true, nil
}
