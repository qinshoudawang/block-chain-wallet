package chainmodel

import "time"

type IndexCursor struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	Chain           string `gorm:"type:varchar(32);not null;uniqueIndex:uk_chain_cursor,priority:1"`
	Stream          string `gorm:"type:varchar(64);not null;uniqueIndex:uk_chain_cursor,priority:2"`
	NextBlockNumber uint64 `gorm:"not null"`

	CreatedAt time.Time
	UpdatedAt time.Time
}
