package addressmodel

import "time"

type HDWallet struct {
	ID uint64 `gorm:"primaryKey"`

	Chain    string `gorm:"type:varchar(32);uniqueIndex:uk_chain"`
	Purpose  uint32
	CoinType uint32
	Account  uint32

	NextIndex uint32 `gorm:"not null;default:0"`

	CreatedAt time.Time
	UpdatedAt time.Time
}
