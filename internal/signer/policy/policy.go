package policy

import (
	"errors"
	"math/big"
	"os"
)

type PolicyEngine struct {
	MaxAmount *big.Int
	WhiteList map[string]bool
}

func NewPolicyEngine() *PolicyEngine {
	return &PolicyEngine{
		MaxAmount: big.NewInt(1000000000000000000), // 1 ETH
		WhiteList: map[string]bool{
			os.Getenv("TO_ADDRESS"): true,
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
