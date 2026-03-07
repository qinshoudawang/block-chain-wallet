package reconcilemodel

import "time"

const (
	FlowSourceDeposit  = "DEPOSIT"
	FlowSourceWithdraw = "WITHDRAW"
	FlowSourceSweep    = "SWEEP"
	FlowSourceTopUp    = "TOPUP"

	FlowStatusMatch    = "MATCH"
	FlowStatusMissing  = "MISSING"
	FlowStatusMismatch = "MISMATCH"
	FlowStatusError    = "ERROR"
)

type BusinessFlowReconciliation struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	FlowSource   string `gorm:"column:flow_source;type:varchar(24);not null;uniqueIndex:uk_business_flow_recon_biz,priority:1;index"`
	BusinessType string `gorm:"column:business_type;type:varchar(32);not null;uniqueIndex:uk_business_flow_recon_biz,priority:2;index"`
	BusinessID   string `gorm:"column:business_id;type:varchar(128);not null;uniqueIndex:uk_business_flow_recon_biz,priority:3"`

	UserID               string `gorm:"type:varchar(64);not null;default:'';index"`
	Chain                string `gorm:"type:varchar(32);not null;index"`
	AssetContractAddress string `gorm:"type:varchar(128);not null;default:''"`

	ExpectedChangeAmount string `gorm:"column:expected_change_amount;type:varchar(80);not null;default:0"`
	ActualChangeAmount   string `gorm:"column:actual_change_amount;type:varchar(80);not null;default:0"`

	ReconciliationStatus string     `gorm:"column:reconciliation_status;type:varchar(16);not null;index"`
	LastErrorMessage     string     `gorm:"column:last_error_message;type:text"`
	HasMismatch          bool       `gorm:"column:has_mismatch;not null;default:false;index"`
	ReconciledAt         *time.Time `gorm:"column:reconciled_at;index"`

	CreatedAt time.Time
	UpdatedAt time.Time
}
