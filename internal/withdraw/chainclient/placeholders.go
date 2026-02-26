package chainclient

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

type btcClient struct{}

func newBTCClient() Client { return &btcClient{} }

func (c *btcClient) AllocateNonce(ctx context.Context, rt Runtime, nonceFloorProvider NonceFloorProvider) (uint64, error) {
	return 0, ErrNotImplemented
}

func (c *btcClient) BuildUnsignedWithdrawTx(
	ctx context.Context,
	rt Runtime,
	to common.Address,
	amount *big.Int,
	nonce uint64,
) ([]byte, error) {
	return nil, ErrNotImplemented
}

type solanaClient struct{}

func newSolanaClient() Client { return &solanaClient{} }

func (c *solanaClient) AllocateNonce(ctx context.Context, rt Runtime, nonceFloorProvider NonceFloorProvider) (uint64, error) {
	return 0, ErrNotImplemented
}

func (c *solanaClient) BuildUnsignedWithdrawTx(
	ctx context.Context,
	rt Runtime,
	to common.Address,
	amount *big.Int,
	nonce uint64,
) ([]byte, error) {
	return nil, ErrNotImplemented
}
