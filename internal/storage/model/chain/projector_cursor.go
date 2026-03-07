package chainmodel

import "time"

type ProjectorCursor struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	Name        string `gorm:"type:varchar(96);not null;uniqueIndex:uk_projector_cursor_name"`
	LastEventID uint64 `gorm:"not null"`

	CreatedAt time.Time
	UpdatedAt time.Time
}
