package model

import "time"

type WithdrawStatus string

const (
	StatusINIT         WithdrawStatus = "INIT"
	StatusFROZEN       WithdrawStatus = "FROZEN"
	StatusSIGNED       WithdrawStatus = "SIGNED"
	StatusBROADCASTING WithdrawStatus = "BROADCASTING"
	StatusBROADCASTED  WithdrawStatus = "BROADCASTED"
	StatusCONFIRMED    WithdrawStatus = "CONFIRMED"
	StatusFAILED       WithdrawStatus = "FAILED"
)

type WithdrawOrder struct {
	ID         uint64 `gorm:"primaryKey;autoIncrement"`
	WithdrawID string `gorm:"type:varchar(64);not null;uniqueIndex:uk_withdraw_id"`
	RequestID  string `gorm:"type:varchar(64);not null;uniqueIndex:uk_request_id"`

	Chain    string `gorm:"type:varchar(32);not null;index:idx_chain_status,priority:1"`
	FromAddr string `gorm:"type:varchar(64);not null;uniqueIndex:uk_chain_from_nonce,priority:2"`
	ToAddr   string `gorm:"type:varchar(64);not null"`
	Amount   string `gorm:"type:varchar(80);not null"`
	Nonce    uint64 `gorm:"not null;uniqueIndex:uk_chain_from_nonce,priority:3"`

	SignedTxHex  string `gorm:"type:text"`
	SignedTxHash string `gorm:"type:varchar(64);index"`

	TxHash string `gorm:"type:varchar(80);index"`

	GasUsed              *uint64
	EffectiveGasPriceWei string `gorm:"type:varchar(80)"`
	GasFeeWei            string `gorm:"type:varchar(80)"`
	ActualSpentWei       string `gorm:"type:varchar(80)"`

	Status      WithdrawStatus `gorm:"type:varchar(24);not null;index:idx_status_next_retry,priority:1;index:idx_status_updated,priority:1"`
	RetryCount  int            `gorm:"not null;default:0"`
	NextRetryAt *time.Time     `gorm:"index:idx_status_next_retry,priority:2"`
	LastError   string         `gorm:"type:text"`

	BlockNumber   *uint64
	Confirmations *int
	ConfirmedAt   *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}
