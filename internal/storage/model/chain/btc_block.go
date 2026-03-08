package chainmodel

import "time"

type BTCIndexedBlock struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	Chain       string `gorm:"type:varchar(32);not null;uniqueIndex:uk_btc_chain_block,priority:1"`
	BlockNumber uint64 `gorm:"not null;uniqueIndex:uk_btc_chain_block,priority:2;index:idx_btc_chain_block_num,priority:2"`
	BlockHash   string `gorm:"type:varchar(128);not null"`
	ParentHash  string `gorm:"type:varchar(128);not null"`

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (BTCIndexedBlock) TableName() string {
	return "btc_indexed_blocks"
}
