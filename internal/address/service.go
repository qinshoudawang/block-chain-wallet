package address

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"wallet-system/internal/helpers"
	"wallet-system/internal/storage/model"
	"wallet-system/internal/storage/repo"
	signpb "wallet-system/proto/signer"

	"gorm.io/gorm"
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

func (s *AddressService) CreateUserAddress(ctx context.Context, userID string, chain string) (*model.UserAddress, error) {
	if strings.TrimSpace(userID) == "" {
		return nil, errors.New("userID is required")
	}
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return nil, err
	}

	var out *model.UserAddress
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

		addr := &model.UserAddress{
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
		out = addr
		return nil
	})
	return out, err
}
