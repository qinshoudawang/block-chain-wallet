package chainclient

import (
	"errors"
	"fmt"
	"strings"

	"wallet-system/internal/helpers"
)

type Registry struct {
	byChain map[string]Client
}

func NewRegistry() *Registry {
	r := &Registry{byChain: make(map[string]Client)}
	r.Register("btc", newBTCClient())
	r.Register("sol", newSolanaClient())
	return r
}

func (r *Registry) Register(chain string, cli Client) error {
	if r == nil {
		return errors.New("broadcaster chain client registry is required")
	}
	if cli == nil {
		return errors.New("broadcaster chain client is required")
	}
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return err
	}
	if r.byChain == nil {
		r.byChain = make(map[string]Client)
	}
	key := strings.ToLower(strings.TrimSpace(spec.CanonicalChain))
	if _, exists := r.byChain[key]; exists {
		return fmt.Errorf("duplicate broadcaster chain client for chain=%s", key)
	}
	r.byChain[key] = cli
	return nil
}

func (r *Registry) Resolve(chain string) (string, Client, error) {
	if r == nil {
		return "", nil, errors.New("broadcaster chain client registry is required")
	}
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return "", nil, err
	}
	key := strings.ToLower(strings.TrimSpace(spec.CanonicalChain))
	cli, ok := r.byChain[key]
	if !ok || cli == nil {
		return "", nil, fmt.Errorf("broadcaster chain client not configured for chain=%s", key)
	}
	return key, cli, nil
}
