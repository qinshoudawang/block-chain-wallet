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
	TokenContract    string
	WithdrawID       string
	MinConf          int
	FeeTarget        int64
	FeeRate          int64
	ExcludedUTXOKeys map[string]struct{}
	ReserveUTXO      ReserveUTXOKeysFunc
	UTXOReserve      UTXOReserveManager
}

type SequenceFloorProvider func(context.Context) (uint64, error)

type RBFUnsignedBuildResult struct {
	UnsignedPayload []byte
	OldFeeRate      int64
	NewFeeRate      int64
	OldFee          int64
	NewFee          int64
}

type Client interface {
	ValidateWithdrawInput(chain string, to string, amount string) (toAddr string, amountValue *big.Int, err error)
	AllocateSequence(ctx context.Context, redisClient *redis.Client, rt *Runtime, sequenceFloorProvider SequenceFloorProvider) (uint64, error)
	BuildUnsignedWithdrawTx(ctx context.Context, rt Runtime, toAddr string, amount *big.Int, sequence uint64) ([]byte, error)
	BuildRBFUnsignedWithdrawTx(
		ctx context.Context,
		chain string,
		fromAddr string,
		toAddr string,
		amount string,
		signedPayload string,
	) (*RBFUnsignedBuildResult, error)
}
