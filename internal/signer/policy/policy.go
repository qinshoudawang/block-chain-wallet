package policy

import (
	"errors"
	"math/big"
)

type PolicyEngine struct {
	MaxAmount *big.Int
	WhiteList map[string]bool
}

func NewPolicyEngine() *PolicyEngine {
	return &PolicyEngine{
		MaxAmount: big.NewInt(1000000000000000000), // 1 ETH
		WhiteList: map[string]bool{
			"0x123...": true,
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
