package deposit

import "time"

type DepositBlock struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	Chain       string `gorm:"type:varchar(32);not null;uniqueIndex:uk_dep_block_chain_num,priority:1"`
	BlockNumber uint64 `gorm:"not null;uniqueIndex:uk_dep_block_chain_num,priority:2;index:idx_dep_block_chain_num,priority:2"`
	BlockHash   string `gorm:"type:varchar(128);not null"`
	ParentHash  string `gorm:"type:varchar(128);not null"`

	CreatedAt time.Time
	UpdatedAt time.Time
}
