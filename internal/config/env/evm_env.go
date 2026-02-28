package env

import (
	"errors"
	"math/big"

	"wallet-system/internal/helpers"

	"github.com/ethereum/go-ethereum/common"
)

type EVMProfile struct {
	Chain         string
	RPC           string
	ChainID       *big.Int
	From          common.Address
	FreezeReserve *big.Int
}

func LoadEVMProfileFromEnv() (EVMProfile, error) {
	chainRaw := helpers.Getenv("ETH_CHAIN", "")
	if chainRaw == "" {
		return EVMProfile{}, errors.New("ETH_CHAIN is required")
	}
	spec, err := helpers.ResolveChainSpec(chainRaw)
	if err != nil {
		return EVMProfile{}, err
	}
	if spec.Family != "evm" {
		return EVMProfile{}, errors.New("ETH_CHAIN must be an evm chain")
	}

	rpc := helpers.Getenv("ETH_RPC", "")
	if rpc == "" {
		return EVMProfile{}, errors.New("ETH_RPC is required")
	}

	chainIDStr := helpers.Getenv("ETH_CHAIN_ID", "")
	if chainIDStr == "" {
		return EVMProfile{}, errors.New("ETH_CHAIN_ID is required")
	}
	chainID, ok := new(big.Int).SetString(chainIDStr, 10)
	if !ok {
		return EVMProfile{}, errors.New("invalid ETH_CHAIN_ID")
	}

	from := common.HexToAddress(helpers.Getenv("ETH_FROM_ADDRESS", ""))
	if from == (common.Address{}) {
		return EVMProfile{}, errors.New("invalid ETH_FROM_ADDRESS")
	}

	gasReserveWeiStr := helpers.Getenv("ETH_WITHDRAW_GAS_RESERVE_WEI", "0")
	gasReserveWei, ok := new(big.Int).SetString(gasReserveWeiStr, 10)
	if !ok || gasReserveWei.Sign() < 0 {
		return EVMProfile{}, errors.New("invalid ETH_WITHDRAW_GAS_RESERVE_WEI")
	}

	return EVMProfile{
		Chain:         spec.CanonicalChain,
		RPC:           rpc,
		ChainID:       chainID,
		From:          from,
		FreezeReserve: gasReserveWei,
	}, nil
}
