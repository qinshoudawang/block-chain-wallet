package chainclient

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"math/big"
)

var ErrNotImplemented = errors.New("chain client not implemented")

type Runtime struct {
	Redis       *redis.Client
	Chain       string
	ChainID     *big.Int
	FromAddress string
	MinConf     int
	FeeTarget   int64
	FeeRate     int64
}

type NonceFloorProvider func(context.Context) (uint64, error)

type Client interface {
	RequiresNonce() bool
	ValidateWithdrawInput(chain string, to string, amount string) (toAddr string, amountValue *big.Int, err error)
	AllocateNonce(ctx context.Context, rt Runtime, nonceFloorProvider NonceFloorProvider) (uint64, error)
	BuildUnsignedWithdrawTx(ctx context.Context, rt Runtime, toAddr string, amount *big.Int, nonce uint64) ([]byte, error)
}
