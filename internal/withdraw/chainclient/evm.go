package chainclient

import (
	"context"
	"errors"
	"math/big"

	"wallet-system/internal/chain/evm"
	"wallet-system/internal/sequence/nonce"

	"github.com/ethereum/go-ethereum/common"
	"github.com/redis/go-redis/v9"
)

type evmClient struct {
	evm *evm.Client
}

func newEVMClient(cli *evm.Client) Client {
	return &evmClient{evm: cli}
}

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

func (c *evmClient) AllocateSequence(ctx context.Context, redisClient *redis.Client, rt *Runtime, sequenceFloorProvider SequenceFloorProvider) (uint64, error) {
	if c == nil || c.evm == nil {
		return 0, errors.New("evm client is required")
	}
	if redisClient == nil {
		return 0, errors.New("redis is required")
	}
	if sequenceFloorProvider == nil {
		return 0, errors.New("sequence floor provider is required")
	}
	if rt == nil {
		return 0, errors.New("runtime is required")
	}
	if !common.IsHexAddress(rt.FromAddress) {
		return 0, errors.New("invalid evm from address")
	}
	from := common.HexToAddress(rt.FromAddress)

	nm := nonce.NewManager(redisClient, rt.Chain, from.Hex(), func(ctx context.Context) (uint64, error) {
		return c.evm.PendingNonceAt(ctx, from)
	})
	nm.WithNonceFloorProvider(sequenceFloorProvider)
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
	if c == nil || c.evm == nil {
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
	return c.evm.BuildUnsignedTx(ctx, from, to, amount, nil, rt.ChainID, nonce)
}

func (c *evmClient) BuildRBFUnsignedWithdrawTx(
	_ context.Context,
	_ string,
	_ string,
	_ string,
	_ string,
	_ string,
	_ int64,
	_ int64,
) (*RBFUnsignedBuildResult, error) {
	return nil, ErrNotImplemented
}
