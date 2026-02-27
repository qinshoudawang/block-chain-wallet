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

func (c *evmClient) RequiresNonce() bool { return true }

func (c *evmClient) ValidateWithdrawInput(chain string, to string, amount string) (string, *big.Int, error) {
	addr := common.HexToAddress(to)
	if addr == (common.Address{}) {
		return "", nil, errors.New("invalid to address")
	}
	amt := new(big.Int)
	if _, ok := amt.SetString(amount, 10); !ok || amt.Sign() <= 0 {
		return "", nil, errors.New("invalid amount")
	}
	return addr.Hex(), amt, nil
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
	if !common.IsHexAddress(rt.FromAddress) {
		return 0, errors.New("invalid evm from address")
	}
	from := common.HexToAddress(rt.FromAddress)

	nm := evm.NewNonceManager(rt.Redis, c.eth, rt.Chain, from)
	nm.WithNonceFloorProvider(nonceFloorProvider)
	if err := nm.EnsureInitialized(ctx); err != nil {
		return 0, err
	}
	return nm.Allocate(ctx)
}

func (c *evmClient) BuildUnsignedWithdrawTx(
	ctx context.Context,
	rt Runtime,
	toAddr string,
	amount *big.Int,
	nonce uint64,
) ([]byte, error) {
	if c == nil || c.eth == nil {
		return nil, errors.New("evm client is required")
	}
	if rt.ChainID == nil {
		return nil, errors.New("chain id is required")
	}
	if !common.IsHexAddress(rt.FromAddress) {
		return nil, errors.New("invalid evm from address")
	}
	if !common.IsHexAddress(toAddr) {
		return nil, errors.New("invalid evm to address")
	}
	if amount == nil || amount.Sign() <= 0 {
		return nil, errors.New("invalid evm amount")
	}
	from := common.HexToAddress(rt.FromAddress)
	to := common.HexToAddress(toAddr)
	return evm.NewEVMBuilder(c.eth).BuildUnsignedTx(ctx, from, to, amount, nil, rt.ChainID, nonce)
}
