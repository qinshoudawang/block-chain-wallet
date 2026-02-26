package chainclient

import (
	"context"
)

type btcClient struct{}

func newBTCClient() Client { return &btcClient{} }

func (c *btcClient) ValidateWithdrawInput(to string, amount string) (ValidatedWithdrawInput, error) {
	return nil, ErrNotImplemented
}

func (c *btcClient) AllocateNonce(ctx context.Context, rt Runtime, nonceFloorProvider NonceFloorProvider) (uint64, error) {
	return 0, ErrNotImplemented
}

func (c *btcClient) BuildUnsignedWithdrawTx(
	ctx context.Context,
	rt Runtime,
	in ValidatedWithdrawInput,
	nonce uint64,
) ([]byte, error) {
	return nil, ErrNotImplemented
}

type solanaClient struct{}

func newSolanaClient() Client { return &solanaClient{} }

func (c *solanaClient) ValidateWithdrawInput(to string, amount string) (ValidatedWithdrawInput, error) {
	return nil, ErrNotImplemented
}

func (c *solanaClient) AllocateNonce(ctx context.Context, rt Runtime, nonceFloorProvider NonceFloorProvider) (uint64, error) {
	return 0, ErrNotImplemented
}

func (c *solanaClient) BuildUnsignedWithdrawTx(
	ctx context.Context,
	rt Runtime,
	in ValidatedWithdrawInput,
	nonce uint64,
) ([]byte, error) {
	return nil, ErrNotImplemented
}
