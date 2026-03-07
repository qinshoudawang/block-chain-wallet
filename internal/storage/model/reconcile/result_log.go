package reconcilemodel

import "time"

type OnchainLedgerReconciliationLog struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	Scope  string `gorm:"type:varchar(16);not null;index"`
	UserID string `gorm:"type:varchar(64);not null;default:'';index"`

	Chain                string `gorm:"type:varchar(32);not null;index"`
	Address              string `gorm:"type:varchar(128);not null;index"`
	AssetContractAddress string `gorm:"type:varchar(128);not null;default:'';index"`

	OnchainBalanceAmount string `gorm:"column:onchain_balance_amount;type:varchar(80);not null;default:0"`
	LedgerBalanceAmount  string `gorm:"column:ledger_balance_amount;type:varchar(80);not null;default:0"`
	BalanceDiffAmount    string `gorm:"column:balance_diff_amount;type:varchar(80);not null;default:0"`

	ReconciliationStatus string    `gorm:"column:reconciliation_status;type:varchar(16);not null;index"`
	LastErrorMessage     string    `gorm:"column:last_error_message;type:text"`
	HasMismatch          bool      `gorm:"column:has_mismatch;not null;default:false;index"`
	ReconciledAt         time.Time `gorm:"column:reconciled_at;not null;index"`

	CreatedAt time.Time `gorm:"index"`
}
