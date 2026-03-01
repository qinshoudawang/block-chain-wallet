package model

import "time"

type LedgerFreezeStatus string

const (
	LedgerFreezeFrozen   LedgerFreezeStatus = "FROZEN"
	LedgerFreezeReleased LedgerFreezeStatus = "RELEASED"
	LedgerFreezeConsumed LedgerFreezeStatus = "CONSUMED"
)

type LedgerFreeze struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	FreezeID string `gorm:"type:varchar(64);not null;uniqueIndex:uk_freeze_id"`

	BizType string `gorm:"type:varchar(32);not null;uniqueIndex:uk_biz,priority:1"`
	BizID   string `gorm:"type:varchar(64);not null;uniqueIndex:uk_biz,priority:2"`

	Chain   string `gorm:"type:varchar(32);not null;index:idx_chain_addr_status,priority:1"`
	Address string `gorm:"type:varchar(128);not null;index:idx_chain_addr_status,priority:2"`
	Amount  string `gorm:"type:varchar(80);not null"`

	Status LedgerFreezeStatus `gorm:"type:varchar(24);not null;index:idx_chain_addr_status,priority:3"`

	ReleasedAt *time.Time
	ConsumedAt *time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}
