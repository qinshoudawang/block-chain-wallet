package chainclient

import (
	"context"
	"math/big"
)

type solanaClient struct{}

func newSolanaClient() Client { return &solanaClient{} }

func (c *solanaClient) RequiresNonce() bool { return false }

func (c *solanaClient) ValidateWithdrawInput(chain string, to string, amount string) (string, *big.Int, error) {
	return "", nil, ErrNotImplemented
}

func (c *solanaClient) AllocateNonce(ctx context.Context, rt Runtime, nonceFloorProvider NonceFloorProvider) (uint64, error) {
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
