package evmprovider

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"math/big"
	"strings"

	"wallet-system/internal/signer/derivation"
	rootprovider "wallet-system/internal/signer/provider"
	"wallet-system/internal/storage/repo"
	signpb "wallet-system/proto/signer"

	"github.com/ethereum/go-ethereum/crypto"
)

type Signer struct {
	privKey *ecdsa.PrivateKey
	chainID *big.Int
	deriver *derivation.Deriver
	addrs   *repo.AddressRepo
}

func NewSigner(hexPriv string, chainID *big.Int, deriver *derivation.Deriver, addrs *repo.AddressRepo) (*Signer, error) {
	if hexPriv == "" {
		return nil, errors.New("empty EVM private key")
	}
	priv, err := crypto.HexToECDSA(rootprovider.Trim0x(hexPriv))
	if err != nil {
		return nil, err
	}
	return &Signer{privKey: priv, chainID: chainID, deriver: deriver, addrs: addrs}, nil
}

func (s *Signer) Sign(ctx context.Context, req *signpb.SignRequest) ([]byte, error) {
	if req == nil {
		return nil, errors.New("sign request is nil")
	}
	priv := s.privKey
	expectedFrom := ""
	if strings.EqualFold(strings.TrimSpace(req.GetCaller()), "sweeper") {
		var err error
		priv, expectedFrom, err = resolveSweepSigner(ctx, s.deriver, s.addrs, req)
		if err != nil {
			return nil, err
		}
	}
	return signUnsignedWithKey(req.GetUnsignedTx(), s.chainID, priv, expectedFrom)
}
