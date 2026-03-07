package reconcilemodel

import "time"

type BusinessFlowReconciliationLog struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	FlowSource   string `gorm:"column:flow_source;type:varchar(24);not null;index"`
	BusinessType string `gorm:"column:business_type;type:varchar(32);not null;index"`
	BusinessID   string `gorm:"column:business_id;type:varchar(128);not null;index"`

	UserID               string `gorm:"type:varchar(64);not null;default:'';index"`
	Chain                string `gorm:"type:varchar(32);not null;index"`
	AssetContractAddress string `gorm:"type:varchar(128);not null;default:'';index"`

	ExpectedChangeAmount string `gorm:"column:expected_change_amount;type:varchar(80);not null;default:0"`
	ActualChangeAmount   string `gorm:"column:actual_change_amount;type:varchar(80);not null;default:0"`

	ReconciliationStatus string    `gorm:"column:reconciliation_status;type:varchar(16);not null;index"`
	LastErrorMessage     string    `gorm:"column:last_error_message;type:text"`
	HasMismatch          bool      `gorm:"column:has_mismatch;not null;default:false;index"`
	ReconciledAt         time.Time `gorm:"column:reconciled_at;not null;index"`

	CreatedAt time.Time `gorm:"index"`
}
