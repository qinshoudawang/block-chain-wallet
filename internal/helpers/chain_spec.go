package helpers

import (
	"errors"
	"fmt"
	"strings"
)

var ErrUnsupportedChain = errors.New("unsupported chain")

const (
	FamilyEVM = "evm"
	FamilyBTC = "btc"
	FamilySOL = "sol"
)

type ChainSpec struct {
	CanonicalChain string
	Family         string
	IsTestnet      bool

	Purpose  uint32
	CoinType uint32
	Account  uint32
	PathFmt  string
}

func ResolveChainSpec(chain string) (ChainSpec, error) {
	c := strings.ToLower(strings.TrimSpace(chain))
	switch c {
	case "evm":
		return evmSpec("evm"), nil
	case "eth", "ethereum":
		return evmSpec("ethereum"), nil
	case "sepolia":
		return evmSpec("sepolia"), nil
	case "bsc", "bnb":
		return evmSpec("bsc"), nil
	case "polygon", "matic":
		return evmSpec("polygon"), nil
	case "arbitrum", "arb":
		return evmSpec("arbitrum"), nil
	case "optimism", "op":
		return evmSpec("optimism"), nil
	case "base":
		return evmSpec("base"), nil
	case "avalanche", "avax":
		return evmSpec("avalanche"), nil
	case "linea":
		return evmSpec("linea"), nil
	case "btc", "bitcoin":
		return ChainSpec{
			CanonicalChain: "btc",
			Family:         FamilyBTC,
			IsTestnet:      false,
			Purpose:        84,
			CoinType:       0,
			Account:        0,
			PathFmt:        "m/84'/0'/0'/0/%d",
		}, nil
	case "btc-testnet", "bitcoin-testnet", "btctest", "btc_testnet":
		return ChainSpec{
			CanonicalChain: "btc-testnet",
			Family:         FamilyBTC,
			IsTestnet:      true,
			Purpose:        84,
			CoinType:       1,
			Account:        0,
			PathFmt:        "m/84'/1'/0'/0/%d",
		}, nil
	case "sol", "solana":
		return ChainSpec{
			CanonicalChain: "sol",
			Family:         FamilySOL,
			Purpose:        44,
			CoinType:       501,
			Account:        0,
			PathFmt:        "m/44'/501'/%d'/0'",
		}, nil
	case "sol-devnet", "solana-devnet", "sol_devnet":
		return ChainSpec{
			CanonicalChain: "sol-devnet",
			Family:         FamilySOL,
			IsTestnet:      true,
			Purpose:        44,
			CoinType:       501,
			Account:        0,
			PathFmt:        "m/44'/501'/%d'/0'",
		}, nil
	case "sol-testnet", "solana-testnet", "sol_testnet":
		return ChainSpec{
			CanonicalChain: "sol-testnet",
			Family:         FamilySOL,
			IsTestnet:      true,
			Purpose:        44,
			CoinType:       501,
			Account:        0,
			PathFmt:        "m/44'/501'/%d'/0'",
		}, nil
	default:
		return ChainSpec{}, fmt.Errorf("%w: %s", ErrUnsupportedChain, chain)
	}
}

func evmSpec(canonical string) ChainSpec {
	return ChainSpec{
		CanonicalChain: canonical,
		Family:         FamilyEVM,
		Purpose:        44,
		CoinType:       60,
		Account:        0,
		PathFmt:        "m/44'/60'/0'/0/%d",
	}
}

// NeedReservation indicates whether the chain requires reservation management.
// Current rule: BTC family uses UTXO reservation.
func NeedReservation(chain string) bool {
	spec, err := ResolveChainSpec(chain)
	if err != nil {
		return false
	}
	return spec.Family == FamilyBTC
}
