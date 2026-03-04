package chainclient

import (
	"context"
	"errors"
	"math/big"
	solchain "wallet-system/internal/chain/sol"
	"wallet-system/internal/helpers"
	"wallet-system/internal/sequence/nonce"

	"github.com/redis/go-redis/v9"
)

type solanaClient struct {
	sol *solchain.Client
}

func newSolanaClient() Client {
	return &solanaClient{sol: solchain.NewClient("")}
}

func newSolanaClientWithClient(cli *solchain.Client) Client {
	return &solanaClient{sol: cli}
}

func (c *solanaClient) ValidateWithdrawInput(chain string, to string, amount string) (string, *big.Int, error) {
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return "", nil, err
	}
	if spec.Family != helpers.FamilySOL {
		return "", nil, errors.New("invalid solana chain")
	}
	if c == nil || c.sol == nil {
		return "", nil, errors.New("solana client is required")
	}
	normalizedTo, err := c.sol.NormalizeAddress(to)
	if err != nil {
		return "", nil, errors.New("invalid to address")
	}
	amt := new(big.Int)
	if _, ok := amt.SetString(amount, 10); !ok || amt.Sign() <= 0 {
		return "", nil, errors.New("invalid amount")
	}
	if !amt.IsUint64() {
		return "", nil, errors.New("solana amount too large")
	}
	return normalizedTo, amt, nil
}

func (c *solanaClient) AllocateSequence(
	ctx context.Context,
	redisClient *redis.Client,
	rt *Runtime,
	sequenceFloorProvider SequenceFloorProvider,
) (uint64, error) {
	if redisClient == nil {
		return 0, errors.New("redis is required")
	}
	if rt == nil {
		return 0, errors.New("runtime is required")
	}
	if sequenceFloorProvider == nil {
		return 0, errors.New("sequence floor provider is required")
	}
	if c == nil || c.sol == nil {
		return 0, errors.New("solana client is required")
	}
	account, err := c.sol.NormalizeAddress(rt.FromAddress)
	if err != nil {
		return 0, errors.New("invalid solana from address")
	}
	nm := nonce.NewManager(redisClient, rt.Chain, account, sequenceFloorProvider)
	if err := nm.EnsureInitialized(ctx); err != nil {
		return 0, err
	}
	return nm.Allocate(ctx)
}

func (c *solanaClient) BuildUnsignedWithdrawTx(
	ctx context.Context,
	rt Runtime,
	toAddr string,
	amount *big.Int,
	sequence uint64,
) ([]byte, error) {
	_ = sequence
	if c == nil || c.sol == nil {
		return nil, errors.New("solana client is required")
	}
	if amount == nil || amount.Sign() <= 0 {
		return nil, errors.New("invalid solana amount")
	}
	if !amount.IsUint64() {
		return nil, errors.New("solana amount too large")
	}
	return c.sol.BuildUnsignedWithdrawTx(ctx, rt.FromAddress, toAddr, amount.Uint64())
}

func (c *solanaClient) BuildRBFUnsignedWithdrawTx(
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
