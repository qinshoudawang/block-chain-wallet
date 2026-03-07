package chainmodel

import "time"

type EventType string

const (
	EventTypeTransfer    EventType = "TRANSFER"
	EventTypeTransaction EventType = "TRANSACTION"
)

type EventAction string

const (
	EventActionApply  EventAction = "APPLY"
	EventActionRevert EventAction = "REVERT"
)

type ChainEvent struct {
	ID uint64 `gorm:"primaryKey;autoIncrement;index:idx_chain_event_stream_id,priority:3"`

	Chain       string      `gorm:"type:varchar(32);not null;index:idx_chain_event_lookup,priority:1;index:idx_chain_event_stream,priority:1;index:idx_chain_event_stream_id,priority:1;index:idx_chain_event_tx_type,priority:1;uniqueIndex:uk_chain_event_apply_revert,priority:1"`
	EventType   EventType   `gorm:"type:varchar(32);not null;index:idx_chain_event_stream,priority:2;index:idx_chain_event_stream_id,priority:2;index:idx_chain_event_tx_type,priority:3;uniqueIndex:uk_chain_event_apply_revert,priority:2"`
	Action      EventAction `gorm:"type:varchar(16);not null;index:idx_chain_event_stream,priority:3;uniqueIndex:uk_chain_event_apply_revert,priority:3"`
	BlockNumber uint64      `gorm:"not null;index:idx_chain_event_lookup,priority:2"`
	BlockHash   string      `gorm:"type:varchar(128);not null"`
	TxHash      string      `gorm:"type:varchar(128);not null;index:idx_chain_event_lookup,priority:3;index:idx_chain_event_tx_type,priority:2;uniqueIndex:uk_chain_event_apply_revert,priority:4"`
	TxIndex     uint        `gorm:"not null"`
	LogIndex    uint        `gorm:"not null;uniqueIndex:uk_chain_event_apply_revert,priority:5"`

	AssetContractAddress    string `gorm:"type:varchar(128);not null;default:''"`
	FromAddress             string `gorm:"type:varchar(128);not null;default:''"`
	ToAddress               string `gorm:"type:varchar(128);not null;default:''"`
	Amount                  string `gorm:"type:varchar(80);not null;default:'0'"`
	FeeAssetContractAddress string `gorm:"type:varchar(128);not null;default:''"`
	FeeAmount               string `gorm:"type:varchar(80);not null;default:'0'"`
	Success                 bool   `gorm:"not null;default:true"`

	RootEventID *uint64 `gorm:"index"`

	CreatedAt time.Time
	UpdatedAt time.Time
}
