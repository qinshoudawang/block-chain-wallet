package address

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"wallet-system/internal/helpers"
	addressmodel "wallet-system/internal/storage/model/address"
	ledgermodel "wallet-system/internal/storage/model/ledger"
	"wallet-system/internal/storage/repo"
	signpb "wallet-system/proto/signer"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type AddressService struct {
	repo         *repo.AddressRepo
	signerClient signpb.SignerServiceClient
}

func NewAddressService(db *gorm.DB, signerClient signpb.SignerServiceClient) *AddressService {
	return &AddressService{
		repo:         repo.NewAddressRepo(db),
		signerClient: signerClient,
	}
}

func (s *AddressService) CreateUserAddress(ctx context.Context, userID string, chain string) (*addressmodel.UserAddress, error) {
	if strings.TrimSpace(userID) == "" {
		return nil, errors.New("userID is required")
	}
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return nil, err
	}

	var out *addressmodel.UserAddress
	err = s.repo.InTx(ctx, func(tx *gorm.DB) error {
		wallet, index, err := s.repo.AllocateIndexTx(tx, spec)
		if err != nil {
			return err
		}

		resp, err := s.signerClient.DeriveAddress(ctx, &signpb.DeriveAddressRequest{
			Chain: spec.CanonicalChain,
			Index: index,
		})
		if err != nil {
			return err
		}

		addr := &addressmodel.UserAddress{
			UserID:         userID,
			Chain:          spec.CanonicalChain,
			Address:        resp.Address,
			HDWalletID:     wallet.ID,
			AddressIndex:   index,
			DerivationPath: fmt.Sprintf(spec.PathFmt, index),
		}
		if err := s.repo.CreateUserAddressTx(tx, addr); err != nil {
			return err
		}
		// Pre-create native asset ledger row for this address so fee deduction paths
		// (e.g. sweep network fee) won't fail on missing account.
		if err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "chain"},
				{Name: "address"},
				{Name: "asset_contract_address"},
			},
			DoUpdates: clause.Assignments(map[string]any{
				"user_id": gorm.Expr("CASE WHEN user_id = '' THEN ? ELSE user_id END", strings.TrimSpace(userID)),
			}),
		}).Create(&ledgermodel.LedgerAccount{
			UserID:               strings.TrimSpace(userID),
			Chain:                spec.CanonicalChain,
			Address:              resp.Address,
			AssetContractAddress: "",
			AvailableAmount:      "0",
			FrozenAmount:         "0",
		}).Error; err != nil {
			return err
		}
		out = addr
		return nil
	})
	return out, err
}
