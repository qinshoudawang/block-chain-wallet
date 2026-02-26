package provider

import (
	"errors"
	"fmt"
	"strings"

	"wallet-system/internal/helpers"
)

type Registry struct {
	byChain map[string]SignerProvider
}

func NewRegistry() *Registry {
	return &Registry{byChain: make(map[string]SignerProvider)}
}

func (r *Registry) Register(chain string, p SignerProvider) error {
	if r == nil {
		return errors.New("provider registry is required")
	}
	if p == nil {
		return errors.New("signer provider is required")
	}
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return err
	}
	if r.byChain == nil {
		r.byChain = make(map[string]SignerProvider)
	}
	key := strings.ToLower(strings.TrimSpace(spec.CanonicalChain))
	if _, exists := r.byChain[key]; exists {
		return fmt.Errorf("duplicate signer provider for chain=%s", key)
	}
	r.byChain[key] = p
	return nil
}

func (r *Registry) Resolve(chain string) (SignerProvider, error) {
	if r == nil {
		return nil, errors.New("provider registry is required")
	}
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return nil, err
	}
	key := strings.ToLower(strings.TrimSpace(spec.CanonicalChain))
	p, ok := r.byChain[key]
	if !ok || p == nil {
		return nil, fmt.Errorf("signer provider not configured for chain=%s", key)
	}
	return p, nil
}
