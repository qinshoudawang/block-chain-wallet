package chainmodel

import "time"

type TrackerCursor struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	Name        string `gorm:"type:varchar(96);not null;uniqueIndex:uk_tracker_cursor_name"`
	LastEventID uint64 `gorm:"not null"`

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (TrackerCursor) TableName() string {
	return "tracker_cursors"
}
