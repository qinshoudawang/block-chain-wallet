package reconcilemodel

import "time"

const (
	ScopeUser = "USER"
	ScopeHot  = "HOT"

	StatusMatch    = "MATCH"
	StatusMismatch = "MISMATCH"
	StatusError    = "ERROR"
)

type OnchainLedgerReconciliation struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	Scope  string `gorm:"column:scope;type:varchar(16);not null;uniqueIndex:uk_onchain_ledger_recon_entity,priority:1;index:idx_onchain_ledger_recon_scope_chain,priority:1"`
	UserID string `gorm:"type:varchar(64);not null;default:'';index"`

	Chain                string `gorm:"type:varchar(32);not null;uniqueIndex:uk_onchain_ledger_recon_entity,priority:2;index:idx_onchain_ledger_recon_scope_chain,priority:2"`
	Address              string `gorm:"type:varchar(128);not null;uniqueIndex:uk_onchain_ledger_recon_entity,priority:3"`
	AssetContractAddress string `gorm:"type:varchar(128);not null;default:'';uniqueIndex:uk_onchain_ledger_recon_entity,priority:4"`

	OnchainBalanceAmount string `gorm:"column:onchain_balance_amount;type:varchar(80);not null;default:0"`
	LedgerBalanceAmount  string `gorm:"column:ledger_balance_amount;type:varchar(80);not null;default:0"`
	BalanceDiffAmount    string `gorm:"column:balance_diff_amount;type:varchar(80);not null;default:0"`

	ReconciliationStatus string     `gorm:"column:reconciliation_status;type:varchar(16);not null;index"`
	LastErrorMessage     string     `gorm:"column:last_error_message;type:text"`
	HasMismatch          bool       `gorm:"column:has_mismatch;not null;default:false;index"`
	ReconciledAt         *time.Time `gorm:"column:reconciled_at;index"`

	CreatedAt time.Time
	UpdatedAt time.Time
}
