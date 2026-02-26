package chainclient

import "context"

type btcClient struct{}

func newBTCClient() Client { return &btcClient{} }

func (c *btcClient) BroadcastSignedTxHex(ctx context.Context, signedTxHex string) (string, error) {
	return "", ErrNotImplemented
}

func (c *btcClient) GetLatestHeight(ctx context.Context) (uint64, error) {
	return 0, ErrNotImplemented
}

func (c *btcClient) GetConfirmation(ctx context.Context, txHash string, amount string, latestHeight uint64) (*Confirmation, error) {
	return nil, ErrNotImplemented
}

type solanaClient struct{}

func newSolanaClient() Client { return &solanaClient{} }

func (c *solanaClient) BroadcastSignedTxHex(ctx context.Context, signedTxHex string) (string, error) {
	return "", ErrNotImplemented
}

func (c *solanaClient) GetLatestHeight(ctx context.Context) (uint64, error) {
	return 0, ErrNotImplemented
}

func (c *solanaClient) GetConfirmation(ctx context.Context, txHash string, amount string, latestHeight uint64) (*Confirmation, error) {
	return nil, ErrNotImplemented
}
