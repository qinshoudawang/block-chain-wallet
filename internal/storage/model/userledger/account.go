package userledger

import "time"

type UserLedgerAccount struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	UserID               string `gorm:"type:varchar(64);not null;uniqueIndex:uk_user_ledger_acct,priority:1"`
	Chain                string `gorm:"type:varchar(32);not null;uniqueIndex:uk_user_ledger_acct,priority:2"`
	AssetContractAddress string `gorm:"type:varchar(128);not null;default:'';uniqueIndex:uk_user_ledger_acct,priority:3"`

	AvailableAmount string `gorm:"type:varchar(80);not null;default:0"`
	FrozenAmount    string `gorm:"type:varchar(80);not null;default:0"`

	CreatedAt time.Time
	UpdatedAt time.Time
}
