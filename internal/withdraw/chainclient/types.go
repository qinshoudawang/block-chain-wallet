package chainclient

import (
	"context"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/redis/go-redis/v9"
)

var ErrNotImplemented = errors.New("chain client not implemented")

type Runtime struct {
	Redis   *redis.Client
	Chain   string
	ChainID *big.Int
	From    common.Address
}

type NonceFloorProvider func(context.Context) (uint64, error)

type Client interface {
	AllocateNonce(ctx context.Context, rt Runtime, nonceFloorProvider NonceFloorProvider) (uint64, error)
	BuildUnsignedWithdrawTx(ctx context.Context, rt Runtime, to common.Address, amount *big.Int, nonce uint64) ([]byte, error)
}
