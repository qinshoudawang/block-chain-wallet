package policy

import (
	"errors"
	"math/big"
	"wallet-system/internal/helpers"
)

type PolicyEngine struct {
	MaxAmount *big.Int
	WhiteList map[string]bool
}

func NewPolicyEngine() *PolicyEngine {
	return &PolicyEngine{
		MaxAmount: big.NewInt(1000000000000000000), // 1 ETH
		WhiteList: map[string]bool{
			helpers.Getenv("ETH_TO_ADDRESS", ""): true,
		},
	}
}

func (p *PolicyEngine) Validate(to string, amount *big.Int) error {
	if !p.WhiteList[to] {
		return errors.New("address not in whitelist")
	}

	if amount.Cmp(p.MaxAmount) > 0 {
		return errors.New("amount exceeds limit")
	}

	return nil
}
