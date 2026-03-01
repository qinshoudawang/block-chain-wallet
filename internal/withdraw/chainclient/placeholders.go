package chainclient

import (
	"context"
	"math/big"

	"github.com/redis/go-redis/v9"
)

type solanaClient struct{}

func newSolanaClient() Client { return &solanaClient{} }

func (c *solanaClient) ValidateWithdrawInput(chain string, to string, amount string) (string, *big.Int, error) {
	return "", nil, ErrNotImplemented
}

func (c *solanaClient) AllocateSequence(
	ctx context.Context,
	redisClient *redis.Client,
	rt *Runtime,
	sequenceFloorProvider SequenceFloorProvider,
) (uint64, error) {
	return 0, ErrNotImplemented
}

func (c *solanaClient) BuildUnsignedWithdrawTx(
	ctx context.Context,
	rt Runtime,
	toAddr string,
	amount *big.Int,
	nonce uint64,
) ([]byte, error) {
	return nil, ErrNotImplemented
}
