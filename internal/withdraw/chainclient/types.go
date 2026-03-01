package chainclient

import (
	"context"
	"errors"
	"math/big"

	"github.com/redis/go-redis/v9"
)

var ErrNotImplemented = errors.New("chain client not implemented")

type Runtime struct {
	Chain       string
	ChainID     *big.Int
	FromAddress string
	MinConf     int
	FeeTarget   int64
	FeeRate     int64
}

type NonceFloorProvider func(context.Context) (uint64, error)

type Client interface {
	ValidateWithdrawInput(chain string, to string, amount string) (toAddr string, amountValue *big.Int, err error)
	BuildUnsignedWithdrawTx(ctx context.Context, rt Runtime, toAddr string, amount *big.Int, nonce uint64) ([]byte, error)
}

type EVMNonceAllocator interface {
	AllocateEVMNonce(ctx context.Context, redisClient *redis.Client, rt Runtime, nonceFloorProvider NonceFloorProvider) (uint64, error)
}
