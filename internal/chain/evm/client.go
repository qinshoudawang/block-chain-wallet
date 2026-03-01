package evm

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Client struct {
	cli *ethclient.Client
}

func NewClient(rpc string) (*Client, error) {
	cli, err := ethclient.Dial(rpc)
	if err != nil {
		return nil, err
	}
	return &Client{cli: cli}, nil
}

func NewClientWithEthClient(cli *ethclient.Client) *Client {
	return &Client{cli: cli}
}

func (c *Client) Close() error {
	if c == nil || c.cli == nil {
		return nil
	}
	c.cli.Close()
	return nil
}

func (c *Client) PendingNonceAt(ctx context.Context, account common.Address) (uint64, error) {
	if c == nil || c.cli == nil {
		return 0, ErrClientNotConfigured
	}
	return c.cli.PendingNonceAt(ctx, account)
}
