package chainclient

import "context"

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
