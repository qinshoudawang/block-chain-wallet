package evm

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func (c *Client) Broadcast(ctx context.Context, signedTxBytes []byte) (string, error) {
	if c == nil || c.cli == nil {
		return "", ErrClientNotConfigured
	}
	var tx types.Transaction
	if err := tx.UnmarshalBinary(signedTxBytes); err != nil {
		return "", err
	}
	if err := c.cli.SendTransaction(ctx, &tx); err != nil {
		return "", err
	}
	return tx.Hash().Hex(), nil
}

func (c *Client) LatestHeight(ctx context.Context) (uint64, error) {
	if c == nil || c.cli == nil {
		return 0, ErrClientNotConfigured
	}
	return c.cli.BlockNumber(ctx)
}

func (c *Client) TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	if c == nil || c.cli == nil {
		return nil, ErrClientNotConfigured
	}
	return c.cli.TransactionReceipt(ctx, txHash)
}

func (c *Client) TransactionByHash(ctx context.Context, txHash common.Hash) (*types.Transaction, bool, error) {
	if c == nil || c.cli == nil {
		return nil, false, ErrClientNotConfigured
	}
	return c.cli.TransactionByHash(ctx, txHash)
}

var ErrClientNotConfigured = errClientNotConfigured{}

type errClientNotConfigured struct{}

func (errClientNotConfigured) Error() string { return "evm client not configured" }
