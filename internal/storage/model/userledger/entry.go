package userledger

import "time"

const (
	BizTypeDepositConfirm = "DEPOSIT_CONFIRM"
	BizTypeDepositRevert  = "DEPOSIT_REVERT"
)

type UserLedgerEntry struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	BizType string `gorm:"type:varchar(32);not null;uniqueIndex:uk_user_ledger_biz,priority:1"`
	BizID   string `gorm:"type:varchar(128);not null;uniqueIndex:uk_user_ledger_biz,priority:2"`

	UserID               string `gorm:"type:varchar(64);not null;index:idx_user_ledger_user_chain,priority:1"`
	Chain                string `gorm:"type:varchar(32);not null;index:idx_user_ledger_user_chain,priority:2"`
	AssetContractAddress string `gorm:"type:varchar(128);not null;default:''"`

	DeltaAvailable string `gorm:"type:varchar(80);not null"`
	DeltaFrozen    string `gorm:"type:varchar(80);not null;default:'0'"`

	CreatedAt time.Time
}
