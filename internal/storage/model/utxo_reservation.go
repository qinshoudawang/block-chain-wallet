package model

import "time"

type UTXOReservationStatus string

const (
	UTXOReservationReserved UTXOReservationStatus = "RESERVED"
	UTXOReservationReleased UTXOReservationStatus = "RELEASED"
)

type UTXOReservation struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	WithdrawID string `gorm:"type:varchar(64);not null;index:idx_utxo_withdraw_status,priority:1;uniqueIndex:uk_withdraw_utxo,priority:1"`
	Chain      string `gorm:"type:varchar(32);not null;index:idx_utxo_chain_addr_status,priority:1"`
	Address    string `gorm:"type:varchar(128);not null;index:idx_utxo_chain_addr_status,priority:2"`
	UTXOKey    string `gorm:"type:varchar(128);not null;uniqueIndex:uk_withdraw_utxo,priority:2"`

	Status UTXOReservationStatus `gorm:"type:varchar(16);not null;index:idx_utxo_chain_addr_status,priority:3;index:idx_utxo_withdraw_status,priority:2"`

	ReservedAt time.Time
	ReleasedAt *time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}
