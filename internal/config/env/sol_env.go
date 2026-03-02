package env

import (
	"errors"
	"math/big"

	"wallet-system/internal/helpers"
)

type SOLProfile struct {
	Chain         string
	RPC           string
	FromAddress   string
	FreezeReserve *big.Int
}

func LoadSOLProfileFromEnv() (SOLProfile, bool, error) {
	from := helpers.Getenv("SOL_FROM_ADDRESS", "")
	if from == "" {
		return SOLProfile{}, false, nil
	}
	chainRaw := helpers.Getenv("SOL_CHAIN", "sol")
	spec, err := helpers.ResolveChainSpec(chainRaw)
	if err != nil {
		return SOLProfile{}, false, err
	}
	if spec.Family != helpers.FamilySOL {
		return SOLProfile{}, false, errors.New("SOL_CHAIN must be a sol family chain")
	}
	rpc := helpers.Getenv("SOL_RPC", "")
	if rpc == "" {
		return SOLProfile{}, false, errors.New("SOL_RPC is required")
	}
	reserve := big.NewInt(0)
	if v := helpers.Getenv("SOL_WITHDRAW_FEE_RESERVE_LAMPORTS", "0"); v != "" {
		if _, ok := reserve.SetString(v, 10); !ok || reserve.Sign() < 0 {
			return SOLProfile{}, false, errors.New("invalid SOL_WITHDRAW_FEE_RESERVE_LAMPORTS")
		}
	}
	return SOLProfile{
		Chain:         spec.CanonicalChain,
		RPC:           rpc,
		FromAddress:   from,
		FreezeReserve: reserve,
	}, true, nil
}
