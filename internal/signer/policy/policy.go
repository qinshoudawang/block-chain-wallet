package policy

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"wallet-system/internal/helpers"
	"wallet-system/internal/storage/repo"
)

type PolicyEngine struct {
	MaxAmount *big.Int
	WhiteList map[string]bool
	addresses *repo.AddressRepo
}

func NewPolicyEngine(addressRepo *repo.AddressRepo) *PolicyEngine {
	whitelist := map[string]bool{}
	if addr := helpers.Getenv("ETH_TO_ADDRESS", ""); addr != "" {
		whitelist[strings.ToLower(strings.TrimSpace(addr))] = true
	}
	// Sweep/top-up flows send funds to hot wallet address.
	if addr := helpers.Getenv("ETH_FROM_ADDRESS", ""); addr != "" {
		whitelist[strings.ToLower(strings.TrimSpace(addr))] = true
	}
	if addr := helpers.Getenv("SOL_TO_ADDRESS", ""); addr != "" {
		whitelist[strings.ToLower(strings.TrimSpace(addr))] = true
	}
	if addr := helpers.Getenv("BTC_TO_ADDRESS", ""); addr != "" {
		whitelist[strings.ToLower(strings.TrimSpace(addr))] = true
	}
	return &PolicyEngine{
		MaxAmount: big.NewInt(1000000000000000000), // 1 ETH
		WhiteList: whitelist,
		addresses: addressRepo,
	}
}

func (p *PolicyEngine) Validate(ctx context.Context, chain string, to string, amount *big.Int) error {
	if p == nil {
		return errors.New("policy not configured")
	}
	if amount == nil || amount.Sign() < 0 {
		return errors.New("invalid amount")
	}
	if p.MaxAmount != nil && amount.Cmp(p.MaxAmount) > 0 {
		return errors.New("amount exceeds limit")
	}
	toNorm := strings.ToLower(strings.TrimSpace(to))
	if p.WhiteList[toNorm] {
		return nil
	}
	if p.addresses != nil {
		_, ok, err := p.addresses.GetByChainAddress(ctx, chain, to)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}
	return errors.New("address not in whitelist")
}
