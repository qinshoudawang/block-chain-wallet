package provider

import (
	"crypto/ecdsa"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

type EVMSigner struct {
	privKey *ecdsa.PrivateKey
	chainID *big.Int
}

func NewEVMSigner(hexPriv string, chainID *big.Int) (*EVMSigner, error) {
	if hexPriv == "" {
		return nil, errors.New("empty EVM private key")
	}
	priv, err := crypto.HexToECDSA(trim0x(hexPriv))
	if err != nil {
		return nil, err
	}
	return &EVMSigner{privKey: priv, chainID: chainID}, nil
}

func (s *EVMSigner) Sign(unsignedTx []byte) ([]byte, error) {
	var tx types.Transaction
	if err := tx.UnmarshalBinary(unsignedTx); err != nil {
		return nil, err
	}

	signer := types.LatestSignerForChainID(s.chainID)
	signedTx, err := types.SignTx(&tx, signer, s.privKey)
	if err != nil {
		return nil, err
	}

	return signedTx.MarshalBinary()
}
