package chainclient

import (
	"errors"
	"strings"
	"wallet-system/internal/helpers"

	"github.com/ethereum/go-ethereum/common"
)

// NormalizeWithdrawTokenContract validates and normalizes withdraw token contract.
// For non-token withdraws it returns empty string.
func NormalizeWithdrawTokenContract(chain string, tokenContract string) (string, error) {
	tokenContract = strings.TrimSpace(tokenContract)
	if tokenContract == "" {
		return "", nil
	}
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return "", err
	}
	if spec.Family != helpers.FamilyEVM {
		return "", errors.New("token withdraw is only supported on evm chains")
	}
	if !common.IsHexAddress(tokenContract) {
		return "", errors.New("invalid evm token contract address")
	}
	return common.HexToAddress(tokenContract).Hex(), nil
}
