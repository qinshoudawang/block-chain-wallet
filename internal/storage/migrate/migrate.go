package migrate

import (
	addressmodel "wallet-system/internal/storage/model/address"
	depositmodel "wallet-system/internal/storage/model/deposit"
	ledgermodel "wallet-system/internal/storage/model/ledger"
	userledgermodel "wallet-system/internal/storage/model/userledger"
	utxomodel "wallet-system/internal/storage/model/utxo"
	withdrawmodel "wallet-system/internal/storage/model/withdraw"

	"gorm.io/gorm"
)

var resetTables = []string{
	// normalized table names in this project
	"withdraw_orders",
	"deposit_records",
	"deposit_cursors",
	"deposit_blocks",
	"ledger_accounts",
	"ledger_freezes",
	"user_ledger_accounts",
	"user_ledger_entries",
	"hd_wallets",
	"user_addresses",
	"utxo_reservations",
	// accidental generic names caused by old struct names
	"records",
	"cursors",
	"blocks",
	"accounts",
	"entries",
}

// All creates/updates tables used by withdraw execution and broadcaster workers.
func All(db *gorm.DB) error {
	if err := db.AutoMigrate(
		&withdrawmodel.WithdrawOrder{},
		&depositmodel.DepositRecord{},
		&depositmodel.DepositCursor{},
		&depositmodel.DepositBlock{},
		&ledgermodel.LedgerAccount{},
		&ledgermodel.LedgerFreeze{},
		&userledgermodel.UserLedgerAccount{},
		&userledgermodel.UserLedgerEntry{},
		&addressmodel.HDWallet{},
		&addressmodel.UserAddress{},
		&utxomodel.UTXOReservation{},
	); err != nil {
		return err
	}
	m := db.Migrator()
	// Non-compatible schema refresh for token multi-asset ledger indexes.
	if m.HasIndex(&ledgermodel.LedgerAccount{}, "uk_ledger_chain_addr") {
		if err := m.DropIndex(&ledgermodel.LedgerAccount{}, "uk_ledger_chain_addr"); err != nil {
			return err
		}
	}
	if !m.HasIndex(&ledgermodel.LedgerAccount{}, "uk_ledger_chain_addr_asset") {
		if err := m.CreateIndex(&ledgermodel.LedgerAccount{}, "uk_ledger_chain_addr_asset"); err != nil {
			return err
		}
	}
	if m.HasIndex(&ledgermodel.LedgerFreeze{}, "idx_chain_addr_status") {
		if err := m.DropIndex(&ledgermodel.LedgerFreeze{}, "idx_chain_addr_status"); err != nil {
			return err
		}
	}
	if m.HasIndex(&ledgermodel.LedgerFreeze{}, "uk_biz") {
		if err := m.DropIndex(&ledgermodel.LedgerFreeze{}, "uk_biz"); err != nil {
			return err
		}
	}
	if !m.HasIndex(&ledgermodel.LedgerFreeze{}, "idx_chain_addr_asset_status") {
		if err := m.CreateIndex(&ledgermodel.LedgerFreeze{}, "idx_chain_addr_asset_status"); err != nil {
			return err
		}
	}
	if !m.HasIndex(&ledgermodel.LedgerFreeze{}, "uk_biz") {
		if err := m.CreateIndex(&ledgermodel.LedgerFreeze{}, "uk_biz"); err != nil {
			return err
		}
	}
	return nil
}

// RecreateAll drops known tables and rebuilds schema from current models.
// This is destructive and intended for non-compatible dev resets.
func RecreateAll(db *gorm.DB) error {
	if err := db.Exec("DROP TABLE IF EXISTS " + joinTables(resetTables) + " CASCADE").Error; err != nil {
		return err
	}
	return All(db)
}

func joinTables(tables []string) string {
	if len(tables) == 0 {
		return ""
	}
	out := tables[0]
	for i := 1; i < len(tables); i++ {
		out += ", " + tables[i]
	}
	return out
}
