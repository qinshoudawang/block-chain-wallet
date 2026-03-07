package evm

import (
	"context"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/event"
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

func (c *Client) BalanceAt(ctx context.Context, account common.Address) (*big.Int, error) {
	if c == nil || c.cli == nil {
		return nil, ErrClientNotConfigured
	}
	return c.cli.BalanceAt(ctx, account, nil)
}

func (c *Client) SuggestGasTipCap(ctx context.Context) (*big.Int, error) {
	if c == nil || c.cli == nil {
		return nil, ErrClientNotConfigured
	}
	return c.cli.SuggestGasTipCap(ctx)
}

func (c *Client) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	if c == nil || c.cli == nil {
		return nil, ErrClientNotConfigured
	}
	return c.cli.SuggestGasPrice(ctx)
}

func (c *Client) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	if c == nil || c.cli == nil {
		return nil, ErrClientNotConfigured
	}
	return c.cli.HeaderByNumber(ctx, number)
}

func (c *Client) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	if c == nil || c.cli == nil {
		return nil, ErrClientNotConfigured
	}
	return c.cli.FilterLogs(ctx, q)
}

func (c *Client) SubscribeFilterLogs(ctx context.Context, q ethereum.FilterQuery, ch chan<- types.Log) (event.Subscription, error) {
	if c == nil || c.cli == nil {
		return nil, ErrClientNotConfigured
	}
	return c.cli.SubscribeFilterLogs(ctx, q, ch)
}

func (c *Client) CallContract(ctx context.Context, msg ethereum.CallMsg) ([]byte, error) {
	if c == nil || c.cli == nil {
		return nil, ErrClientNotConfigured
	}
	return c.cli.CallContract(ctx, msg, nil)
}

func (c *Client) TokenBalanceAt(ctx context.Context, token common.Address, owner common.Address) (*big.Int, error) {
	if token == (common.Address{}) || owner == (common.Address{}) {
		return nil, errors.New("invalid token or owner address")
	}
	// ERC20 balanceOf(address): 0x70a08231
	data := make([]byte, 4+32)
	copy(data[:4], []byte{0x70, 0xa0, 0x82, 0x31})
	copy(data[4:], common.LeftPadBytes(owner.Bytes(), 32))

	out, err := c.CallContract(ctx, ethereum.CallMsg{
		To:   &token,
		Data: data,
	})
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return big.NewInt(0), nil
	}
	if len(out) < 32 {
		return nil, errors.New("invalid balanceOf response")
	}
	return new(big.Int).SetBytes(out[len(out)-32:]), nil
}

func (c *Client) SuggestDynamicFeeCaps(ctx context.Context) (*big.Int, *big.Int, error) {
	tipCap, err := c.SuggestGasTipCap(ctx)
	if err != nil {
		return nil, nil, err
	}
	head, err := c.HeaderByNumber(ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	feeCap := new(big.Int).Set(tipCap)
	if head != nil && head.BaseFee != nil {
		feeCap = new(big.Int).Mul(head.BaseFee, big.NewInt(2))
		feeCap.Add(feeCap, tipCap)
	}
	return tipCap, feeCap, nil
}
