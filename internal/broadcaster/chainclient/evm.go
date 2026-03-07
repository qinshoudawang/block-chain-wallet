package chainclient

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"

	"wallet-system/internal/chain/evm"
)

type evmClient struct {
	client *evm.Client
}

func NewEVMClient(client *evm.Client) Client {
	return &evmClient{client: client}
}

func (c *evmClient) BroadcastSignedTxHex(ctx context.Context, signedTxHex string) (string, error) {
	if c == nil || c.client == nil {
		return "", errors.New("evm client is required")
	}
	raw, err := hex.DecodeString(strings.TrimPrefix(strings.TrimSpace(signedTxHex), "0x"))
	if err != nil {
		return "", err
	}
	return c.client.Broadcast(ctx, raw)
}

func (c *evmClient) GetLatestHeight(ctx context.Context) (uint64, error) {
	if c == nil || c.client == nil {
		return 0, errors.New("evm client is required")
	}
	return c.client.LatestHeight(ctx)
}

func (c *evmClient) GetConfirmation(ctx context.Context, txHash string, amount string, latestHeight uint64) (*Confirmation, error) {
	_ = ctx
	_ = txHash
	_ = amount
	_ = latestHeight
	return nil, errors.New("evm confirmer path is disabled; use chain indexer")
}
