package evmprovider

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"math/big"
	"strings"

	"wallet-system/internal/signer/derivation"
	"wallet-system/internal/storage/repo"
	signpb "wallet-system/proto/signer"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func resolveSweepSigner(ctx context.Context, deriver *derivation.Deriver, addrs *repo.AddressRepo, req *signpb.SignRequest) (*ecdsa.PrivateKey, string, error) {
	if deriver == nil {
		return nil, "", errors.New("evm deriver not configured")
	}
	if addrs == nil {
		return nil, "", errors.New("address repo not configured")
	}
	fromAddr := strings.TrimSpace(req.GetFromAddress())
	row, ok, err := addrs.GetByChainAddress(ctx, req.GetChain(), fromAddr)
	if err != nil {
		return nil, "", err
	}
	if !ok {
		return nil, "", errors.New("sweep from_address is not a derived user address")
	}
	priv, derivedAddr, err := deriver.DeriveEVMPrivateKey(req.GetChain(), row.AddressIndex)
	if err != nil {
		return nil, "", err
	}
	if !strings.EqualFold(derivedAddr, fromAddr) {
		return nil, "", errors.New("sweep signer address mismatch")
	}
	return priv, fromAddr, nil
}

func signUnsignedWithKey(
	unsignedTx []byte,
	expectedChainID *big.Int,
	priv *ecdsa.PrivateKey,
	expectedFrom string,
) ([]byte, error) {
	if priv == nil {
		return nil, errors.New("evm private key is required")
	}
	var tx types.Transaction
	if err := tx.UnmarshalBinary(unsignedTx); err != nil {
		return nil, err
	}
	chainID := tx.ChainId()
	if chainID == nil || chainID.Sign() <= 0 {
		return nil, errors.New("missing chain id in evm tx")
	}
	if expectedChainID != nil && expectedChainID.Sign() > 0 && chainID.Cmp(expectedChainID) != 0 {
		return nil, errors.New("evm chain id mismatch")
	}
	signer := types.LatestSignerForChainID(chainID)
	signedTx, err := types.SignTx(&tx, signer, priv)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(expectedFrom) != "" {
		from, err := types.Sender(signer, signedTx)
		if err != nil {
			return nil, err
		}
		if from != common.HexToAddress(expectedFrom) {
			return nil, errors.New("evm signed from mismatch")
		}
	}
	return signedTx.MarshalBinary()
}
