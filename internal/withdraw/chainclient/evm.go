package chainclient

import (
	"context"
	"errors"
	"math/big"

	"wallet-system/internal/chain/evm"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

type evmClient struct {
	eth *ethclient.Client
}

func newEVMClient(eth *ethclient.Client) Client {
	return &evmClient{eth: eth}
}

func (c *evmClient) AllocateNonce(ctx context.Context, rt Runtime, nonceFloorProvider NonceFloorProvider) (uint64, error) {
	if c == nil || c.eth == nil {
		return 0, errors.New("evm client is required")
	}
	if rt.Redis == nil {
		return 0, errors.New("redis is required")
	}
	if nonceFloorProvider == nil {
		return 0, errors.New("nonce floor provider is required")
	}

	nm := evm.NewNonceManager(rt.Redis, c.eth, rt.Chain, rt.From)
	nm.WithNonceFloorProvider(nonceFloorProvider)
	if err := nm.EnsureInitialized(ctx); err != nil {
		return 0, err
	}
	return nm.Allocate(ctx)
}

func (c *evmClient) BuildUnsignedWithdrawTx(
	ctx context.Context,
	rt Runtime,
	to common.Address,
	amount *big.Int,
	nonce uint64,
) ([]byte, error) {
	if c == nil || c.eth == nil {
		return nil, errors.New("evm client is required")
	}
	if rt.ChainID == nil {
		return nil, errors.New("chain id is required")
	}
	return evm.NewEVMBuilder(c.eth).BuildUnsignedTx(ctx, rt.From, to, amount, nil, rt.ChainID, nonce)
}
