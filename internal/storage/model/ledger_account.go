package model

import "time"

// LedgerAccount stores on-platform balances for withdrawal pre-freeze.
// Amount fields are wei decimal strings.
type LedgerAccount struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	Chain   string `gorm:"type:varchar(32);not null;uniqueIndex:uk_ledger_chain_addr,priority:1"`
	Address string `gorm:"type:varchar(64);not null;uniqueIndex:uk_ledger_chain_addr,priority:2"`

	AvailableWei string `gorm:"type:varchar(80);not null;default:0"`
	FrozenWei    string `gorm:"type:varchar(80);not null;default:0"`

	CreatedAt time.Time
	UpdatedAt time.Time
}
