package chainmodel

import "time"

type ReorgNotice struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	Chain     string `gorm:"type:varchar(32);not null;index:idx_chain_reorg_notice,priority:1"`
	FromBlock uint64 `gorm:"not null"`

	CreatedAt time.Time
}
