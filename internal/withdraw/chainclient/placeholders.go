package chainclient

import (
	"context"
	"math/big"
)

type solanaClient struct{}

func newSolanaClient() Client { return &solanaClient{} }

func (c *solanaClient) ValidateWithdrawInput(chain string, to string, amount string) (string, *big.Int, error) {
	return "", nil, ErrNotImplemented
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
