package chainclient

import (
	"errors"
	"fmt"
	"strings"
	"wallet-system/internal/chain/btc"
	"wallet-system/internal/chain/evm"

	"wallet-system/internal/helpers"
)

type Registry struct {
	byFamily map[string]Client
}

type EVMRegistration struct {
	Client *evm.Client
}

type BTCRegistration struct {
	Client *btc.Client
}

func NewRegistry() *Registry {
	r := &Registry{byFamily: make(map[string]Client)}
	r.Register(helpers.FamilySOL, newSolanaClient())
	return r
}

func (r *Registry) Register(family string, cli Client) {
	if r == nil {
		return
	}
	if r.byFamily == nil {
		r.byFamily = make(map[string]Client)
	}
	r.byFamily[strings.ToLower(strings.TrimSpace(family))] = cli
}

func (r *Registry) RegisterEVM(reg EVMRegistration) {
	r.Register(helpers.FamilyEVM, newEVMClient(reg.Client))
}

func (r *Registry) RegisterBTC(reg BTCRegistration) {
	r.Register(helpers.FamilyBTC, newBTCClientWithRPC(reg.Client))
}

func (r *Registry) ResolveByChain(chain string) (Client, error) {
	if r == nil {
		return nil, errors.New("chain client registry is required")
	}
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return nil, err
	}
	cli, ok := r.byFamily[spec.Family]
	if !ok || cli == nil {
		return nil, fmt.Errorf("chain client not registered for family=%s chain=%s", spec.Family, spec.CanonicalChain)
	}
	return cli, nil
}
