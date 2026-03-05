package migrate

import (
	"wallet-system/internal/storage/model"

	"gorm.io/gorm"
)

// All creates/updates tables used by withdraw execution and broadcaster workers.
func All(db *gorm.DB) error {
	if err := db.AutoMigrate(
		&model.WithdrawOrder{},
		&model.LedgerAccount{},
		&model.LedgerFreeze{},
		&model.HDWallet{},
		&model.UserAddress{},
		&model.UTXOReservation{},
	); err != nil {
		return err
	}
	m := db.Migrator()
	// Non-compatible schema refresh for token multi-asset ledger indexes.
	if m.HasIndex(&model.LedgerAccount{}, "uk_ledger_chain_addr") {
		if err := m.DropIndex(&model.LedgerAccount{}, "uk_ledger_chain_addr"); err != nil {
			return err
		}
	}
	if !m.HasIndex(&model.LedgerAccount{}, "uk_ledger_chain_addr_asset") {
		if err := m.CreateIndex(&model.LedgerAccount{}, "uk_ledger_chain_addr_asset"); err != nil {
			return err
		}
	}
	if m.HasIndex(&model.LedgerFreeze{}, "idx_chain_addr_status") {
		if err := m.DropIndex(&model.LedgerFreeze{}, "idx_chain_addr_status"); err != nil {
			return err
		}
	}
	if m.HasIndex(&model.LedgerFreeze{}, "uk_biz") {
		if err := m.DropIndex(&model.LedgerFreeze{}, "uk_biz"); err != nil {
			return err
		}
	}
	if !m.HasIndex(&model.LedgerFreeze{}, "idx_chain_addr_asset_status") {
		if err := m.CreateIndex(&model.LedgerFreeze{}, "idx_chain_addr_asset_status"); err != nil {
			return err
		}
	}
	if !m.HasIndex(&model.LedgerFreeze{}, "uk_biz") {
		if err := m.CreateIndex(&model.LedgerFreeze{}, "uk_biz"); err != nil {
			return err
		}
	}
	return nil
}
