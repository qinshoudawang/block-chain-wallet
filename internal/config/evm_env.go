package config

import (
	"errors"
	"math/big"

	"wallet-system/internal/helpers"

	"github.com/ethereum/go-ethereum/common"
)

type EVMNetwork struct {
	Chain   string
	RPC     string
	ChainID *big.Int
}

type WithdrawProfile struct {
	From          common.Address
	FreezeReserve *big.Int
}

func LoadEVMNetworkFromEnv() (EVMNetwork, error) {
	chainRaw := helpers.Getenv("ETH_CHAIN", "")
	if chainRaw == "" {
		return EVMNetwork{}, errors.New("ETH_CHAIN is required")
	}
	spec, err := helpers.ResolveChainSpec(chainRaw)
	if err != nil {
		return EVMNetwork{}, err
	}
	if spec.Family != "evm" {
		return EVMNetwork{}, errors.New("ETH_CHAIN must be an evm chain")
	}

	rpc := helpers.Getenv("ETH_RPC", "")
	if rpc == "" {
		return EVMNetwork{}, errors.New("ETH_RPC is required")
	}

	chainIDStr := helpers.Getenv("ETH_CHAIN_ID", "")
	if chainIDStr == "" {
		return EVMNetwork{}, errors.New("ETH_CHAIN_ID is required")
	}
	chainID, ok := new(big.Int).SetString(chainIDStr, 10)
	if !ok {
		return EVMNetwork{}, errors.New("invalid ETH_CHAIN_ID")
	}

	return EVMNetwork{
		Chain:   spec.CanonicalChain,
		RPC:     rpc,
		ChainID: chainID,
	}, nil
}

func LoadWithdrawProfileFromEnv() (WithdrawProfile, error) {
	from := common.HexToAddress(helpers.Getenv("FROM_ADDRESS", ""))
	if from == (common.Address{}) {
		return WithdrawProfile{}, errors.New("invalid FROM_ADDRESS")
	}

	gasReserveWeiStr := helpers.Getenv("WITHDRAW_GAS_RESERVE_WEI", "0")
	gasReserveWei, ok := new(big.Int).SetString(gasReserveWeiStr, 10)
	if !ok || gasReserveWei.Sign() < 0 {
		return WithdrawProfile{}, errors.New("invalid WITHDRAW_GAS_RESERVE_WEI")
	}

	return WithdrawProfile{
		From:          from,
		FreezeReserve: gasReserveWei,
	}, nil
}
