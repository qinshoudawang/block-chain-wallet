package chainclient

import (
	"errors"
	"fmt"
	"strings"

	"wallet-system/internal/helpers"

	"github.com/ethereum/go-ethereum/ethclient"
)

type Registry struct {
	byFamily map[string]Client
}

func NewRegistry() *Registry {
	r := &Registry{byFamily: make(map[string]Client)}
	r.Register("btc", newBTCClient())
	r.Register("sol", newSolanaClient())
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

func (r *Registry) RegisterEVM(eth *ethclient.Client) {
	r.Register("evm", newEVMClient(eth))
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
