package config

import (
	"math/big"

	"wallet-system/internal/config/env"
)

type ChainProfile struct {
	Chain         string
	FromAddress   string
	ChainID       *big.Int
	FreezeReserve *big.Int
	MinConf       int
	FeeTarget     int64
	FeeRate       int64
}

func LoadChainProfilesFromEnv() (map[string]ChainProfile, error) {
	evm, err := env.LoadEVMProfileFromEnv()
	if err != nil {
		return nil, err
	}
	profiles := map[string]ChainProfile{
		evm.Chain: {
			Chain:         evm.Chain,
			FromAddress:   evm.From.Hex(),
			ChainID:       evm.ChainID,
			FreezeReserve: evm.FreezeReserve,
		},
	}

	btc, ok, err := env.LoadBTCProfileFromEnv()
	if err != nil {
		return nil, err
	}
	if ok {
		profiles[btc.Chain] = ChainProfile{
			Chain:         btc.Chain,
			FromAddress:   btc.FromAddress,
			FreezeReserve: btc.FreezeReserve,
			MinConf:       btc.MinConf,
			FeeTarget:     btc.FeeTarget,
			FeeRate:       btc.FeeRate,
		}
	}
	return profiles, nil
}
