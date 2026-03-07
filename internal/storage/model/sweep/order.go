package sweepmodel

import "time"

type SweepStatus string

const (
	SweepStatusInit        SweepStatus = "INIT"
	SweepStatusBroadcasted SweepStatus = "BROADCASTED"
	SweepStatusConfirmed   SweepStatus = "CONFIRMED"
	SweepStatusFailed      SweepStatus = "FAILED"
)

type SweepOrder struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	SweepID string `gorm:"type:varchar(96);not null;uniqueIndex:uk_sweep_id"`

	Chain                string `gorm:"type:varchar(32);not null;index:idx_sweep_status_chain,priority:2"`
	UserID               string `gorm:"type:varchar(64);not null;index:idx_sweep_user_chain,priority:1"`
	FromAddress          string `gorm:"type:varchar(128);not null"`
	ToAddress            string `gorm:"type:varchar(128);not null"`
	AssetContractAddress string `gorm:"type:varchar(128);not null;default:''"`
	Amount               string `gorm:"type:varchar(80);not null"`

	Status                         SweepStatus `gorm:"type:varchar(24);not null;index:idx_sweep_status_chain,priority:1"`
	TxHash                         string      `gorm:"type:varchar(128);index"`
	LastError                      string      `gorm:"type:text"`
	ActualSpentAmount              string      `gorm:"type:varchar(80);not null;default:''"`
	TransferAssetContractAddress   string      `gorm:"type:varchar(128);not null;default:''"`
	NetworkFeeAssetContractAddress string      `gorm:"type:varchar(128);not null;default:''"`
	NetworkFeeAmount               string      `gorm:"type:varchar(80);not null;default:''"`

	BlockNumber   *uint64
	Confirmations *int
	BroadcastedAt *time.Time
	ConfirmedAt   *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}
