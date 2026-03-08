package reconcilemodel

import "time"

type BalanceDeltaReconciliation struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	Scope  string `gorm:"type:varchar(16);not null;uniqueIndex:uk_balance_delta_window,priority:1;index"`
	UserID string `gorm:"type:varchar(64);not null;default:'';index"`

	Chain                string `gorm:"type:varchar(32);not null;uniqueIndex:uk_balance_delta_window,priority:2;index"`
	Address              string `gorm:"type:varchar(128);not null;uniqueIndex:uk_balance_delta_window,priority:3;index"`
	AssetContractAddress string `gorm:"type:varchar(128);not null;default:'';uniqueIndex:uk_balance_delta_window,priority:4;index"`

	WindowStartedAt time.Time `gorm:"not null;uniqueIndex:uk_balance_delta_window,priority:5;index"`
	WindowEndedAt   time.Time `gorm:"not null;uniqueIndex:uk_balance_delta_window,priority:6;index"`

	StartingLedgerBalanceAmount string `gorm:"type:varchar(80);not null;default:0"`
	EndingLedgerBalanceAmount   string `gorm:"type:varchar(80);not null;default:0"`
	ExpectedDeltaAmount         string `gorm:"type:varchar(80);not null;default:0"`
	ActualDeltaAmount           string `gorm:"type:varchar(80);not null;default:0"`
	DeltaDiffAmount             string `gorm:"type:varchar(80);not null;default:0"`

	ReconciliationStatus string    `gorm:"type:varchar(16);not null;index"`
	LastErrorMessage     string    `gorm:"type:text"`
	HasMismatch          bool      `gorm:"not null;default:false;index"`
	ReconciledAt         time.Time `gorm:"not null;index"`

	CreatedAt time.Time
	UpdatedAt time.Time
}
