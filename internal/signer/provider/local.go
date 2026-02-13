package provider

import (
	"crypto/ecdsa"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

type EVMLocalSigner struct {
	privKey *ecdsa.PrivateKey
	chainID *big.Int
}

func NewEVMLocalSigner(hexPriv string, chainID *big.Int) (*EVMLocalSigner, error) {
	if hexPriv == "" {
		return nil, errors.New("empty EVM private key")
	}
	priv, err := crypto.HexToECDSA(trim0x(hexPriv))
	if err != nil {
		return nil, err
	}
	return &EVMLocalSigner{privKey: priv, chainID: chainID}, nil
}

func (s *EVMLocalSigner) Sign(unsignedTx []byte) ([]byte, error) {
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

func trim0x(s string) string {
	if len(s) >= 2 && (s[:2] == "0x" || s[:2] == "0X") {
		return s[2:]
	}
	return s
}
