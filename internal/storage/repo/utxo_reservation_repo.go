package repo

import (
	"context"
	"errors"
	"time"

	utxomodel "wallet-system/internal/storage/model/utxo"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type UTXOReservationRepo struct {
	db *gorm.DB
}

func NewUTXOReservationRepo(db *gorm.DB) *UTXOReservationRepo {
	return &UTXOReservationRepo{db: db}
}

func (r *UTXOReservationRepo) UpsertReserved(ctx context.Context, chain, address, withdrawID string, utxoKeys []string) error {
	if r == nil || r.db == nil {
		return errors.New("utxo reservation repo not configured")
	}
	if len(utxoKeys) == 0 {
		return nil
	}
	now := time.Now()
	rows := make([]utxomodel.UTXOReservation, 0, len(utxoKeys))
	for _, key := range utxoKeys {
		rows = append(rows, utxomodel.UTXOReservation{
			WithdrawID: withdrawID,
			Chain:      chain,
			Address:    address,
			UTXOKey:    key,
			Status:     utxomodel.UTXOReservationReserved,
			ReservedAt: now,
			ReleasedAt: nil,
		})
	}
	return r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "withdraw_id"},
				{Name: "utxo_key"},
			},
			DoUpdates: clause.Assignments(map[string]any{
				"chain":       chain,
				"address":     address,
				"status":      utxomodel.UTXOReservationReserved,
				"reserved_at": now,
				"released_at": nil,
				"updated_at":  now,
			}),
		}).
		Create(&rows).Error
}

func (r *UTXOReservationRepo) ReleaseByWithdrawID(ctx context.Context, withdrawID string) error {
	if r == nil || r.db == nil {
		return errors.New("utxo reservation repo not configured")
	}
	now := time.Now()
	return r.db.WithContext(ctx).
		Model(&utxomodel.UTXOReservation{}).
		Where("withdraw_id = ? AND status = ?", withdrawID, utxomodel.UTXOReservationReserved).
		Updates(map[string]any{
			"status":      utxomodel.UTXOReservationReleased,
			"released_at": &now,
			"updated_at":  now,
		}).Error
}

func (r *UTXOReservationRepo) ListReservedKeysByAccount(ctx context.Context, chain, address string) ([]string, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("utxo reservation repo not configured")
	}
	rows, err := r.ListReservedByAccount(ctx, chain, address)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		keys = append(keys, row.UTXOKey)
	}
	return keys, nil
}

func (r *UTXOReservationRepo) ListReservedByAccount(ctx context.Context, chain, address string) ([]utxomodel.UTXOReservation, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("utxo reservation repo not configured")
	}
	var out []utxomodel.UTXOReservation
	err := r.db.WithContext(ctx).
		Model(&utxomodel.UTXOReservation{}).
		Where("chain = ? AND address = ? AND status = ?", chain, address, utxomodel.UTXOReservationReserved).
		Select("withdraw_id", "utxo_key").
		Find(&out).Error
	return out, err
}
