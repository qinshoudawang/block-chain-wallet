package deposit

import "time"

type Status string

const (
	StatusPending   Status = "PENDING"
	StatusConfirmed Status = "CONFIRMED"
	StatusReverted  Status = "REVERTED"
)

type DepositRecord struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	Chain                string `gorm:"type:varchar(32);not null;index:idx_dep_chain_block,priority:1;uniqueIndex:uk_dep_chain_tx_log,priority:1"`
	TxHash               string `gorm:"type:varchar(128);not null;uniqueIndex:uk_dep_chain_tx_log,priority:2"`
	LogIndex             uint   `gorm:"not null;uniqueIndex:uk_dep_chain_tx_log,priority:3"`
	BlockNumber          uint64 `gorm:"not null;index:idx_dep_chain_block,priority:2"`
	TokenContractAddress string `gorm:"type:varchar(128);not null;default:''"`

	UserID      string `gorm:"type:varchar(64);not null;index:idx_dep_user_chain,priority:1"`
	ToAddress   string `gorm:"type:varchar(128);not null;index:idx_dep_user_chain,priority:3"`
	FromAddress string `gorm:"type:varchar(128);not null"`
	Amount      string `gorm:"type:varchar(80);not null"`
	Status      Status `gorm:"type:varchar(24);not null;default:'PENDING';index:idx_dep_user_chain,priority:2;index:idx_dep_status_block,priority:1"`
	ConfirmedAt *time.Time
	RevertedAt  *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}
