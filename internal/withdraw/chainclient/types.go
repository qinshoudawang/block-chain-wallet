package chainclient

import (
	"context"
	"errors"
	"math/big"

	"github.com/redis/go-redis/v9"
)

var ErrNotImplemented = errors.New("chain client not implemented")

type ReserveUTXOKeysFunc func(ctx context.Context, utxoKeys []string) error

type UTXOReserveManager interface {
	ListReservedForAccount(ctx context.Context, chain string, address string) (map[string]struct{}, error)
	ReserveByWithdrawID(ctx context.Context, chain string, address string, withdrawID string, utxoKeys []string) error
}

type Runtime struct {
	Chain            string
	ChainID          *big.Int
	FromAddress      string
	WithdrawID       string
	MinConf          int
	FeeTarget        int64
	FeeRate          int64
	ExcludedUTXOKeys map[string]struct{}
	ReserveUTXO      ReserveUTXOKeysFunc
	UTXOReserve      UTXOReserveManager
}

type SequenceFloorProvider func(context.Context) (uint64, error)

type Client interface {
	ValidateWithdrawInput(chain string, to string, amount string) (toAddr string, amountValue *big.Int, err error)
	BuildUnsignedWithdrawTx(ctx context.Context, rt Runtime, toAddr string, amount *big.Int, nonce uint64) ([]byte, error)
}

// SequenceAllocator allows a chain client to provide chain-specific sequence allocation.
// If not implemented, caller should fall back to local sequence allocation.
type SequenceAllocator interface {
	AllocateSequence(ctx context.Context, redisClient *redis.Client, rt *Runtime, sequenceFloorProvider SequenceFloorProvider) (uint64, error)
}
