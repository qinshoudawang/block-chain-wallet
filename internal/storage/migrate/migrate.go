package migrate

import (
	addressmodel "wallet-system/internal/storage/model/address"
	depositmodel "wallet-system/internal/storage/model/deposit"
	ledgermodel "wallet-system/internal/storage/model/ledger"
	sweepmodel "wallet-system/internal/storage/model/sweep"
	userledgermodel "wallet-system/internal/storage/model/userledger"
	utxomodel "wallet-system/internal/storage/model/utxo"
	withdrawmodel "wallet-system/internal/storage/model/withdraw"

	"gorm.io/gorm"
)

var resetTables = []string{
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
	"sweep_orders",
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
		&sweepmodel.SweepOrder{},
	); err != nil {
		return err
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
