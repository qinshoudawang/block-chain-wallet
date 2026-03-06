package addressmodel

import "time"

type UserAddress struct {
	ID uint64 `gorm:"primaryKey"`

	UserID string `gorm:"type:varchar(64);index:idx_user_chain,priority:1"`
	Chain  string `gorm:"type:varchar(32);index:idx_user_chain,priority:2"`

	Address      string `gorm:"type:varchar(128);uniqueIndex:uk_chain_address,priority:2"`
	HDWalletID   uint64 `gorm:"uniqueIndex:uk_wallet_index,priority:1"`
	AddressIndex uint32 `gorm:"uniqueIndex:uk_wallet_index,priority:2"`

	DerivationPath string `gorm:"type:varchar(128)"`

	CreatedAt time.Time
}
