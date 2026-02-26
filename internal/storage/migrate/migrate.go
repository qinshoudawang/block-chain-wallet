package migrate

import (
	"wallet-system/internal/storage/model"

	"gorm.io/gorm"
)

// All creates/updates tables used by withdraw execution and broadcaster workers.
func All(db *gorm.DB) error {
	return db.AutoMigrate(
		&model.WithdrawOrder{},
		&model.LedgerAccount{},
		&model.LedgerFreeze{},
		&model.HDWallet{},
		&model.UserAddress{},
	)
}
