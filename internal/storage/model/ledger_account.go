package model

import "time"

// LedgerAccount stores on-platform balances for withdrawal pre-freeze.
// Amount fields are atomic-unit decimal strings.
type LedgerAccount struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	Chain                string `gorm:"type:varchar(32);not null;uniqueIndex:uk_ledger_chain_addr_asset,priority:1"`
	Address              string `gorm:"type:varchar(128);not null;uniqueIndex:uk_ledger_chain_addr_asset,priority:2"`
	AssetContractAddress string `gorm:"type:varchar(128);not null;default:'';uniqueIndex:uk_ledger_chain_addr_asset,priority:3"`

	AvailableAmount string `gorm:"type:varchar(80);not null;default:0"`
	FrozenAmount    string `gorm:"type:varchar(80);not null;default:0"`

	CreatedAt time.Time
	UpdatedAt time.Time
}
