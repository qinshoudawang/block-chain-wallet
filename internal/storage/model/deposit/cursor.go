package deposit

import "time"

type DepositCursor struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	Chain           string `gorm:"type:varchar(32);not null;uniqueIndex:uk_dep_cursor_chain"`
	NextBlockNumber uint64 `gorm:"not null"`

	CreatedAt time.Time
	UpdatedAt time.Time
}
