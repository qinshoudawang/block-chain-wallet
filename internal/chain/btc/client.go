package btc

import (
	"errors"

	"github.com/btcsuite/btcd/rpcclient"
)

type Config struct {
	Host       string
	User       string
	Pass       string
	DisableTLS bool
	Params     string
}

type Client struct {
	cli *rpcclient.Client
}

type UTXO struct {
	TxID          string
	Vout          uint32
	ValueSat      int64
	Confirmations int64
}

func NewClient(cfg Config) (*Client, error) {
	if cfg.Host == "" {
		return nil, errors.New("btc rpc host is required")
	}
	if cfg.User == "" {
		return nil, errors.New("btc rpc user is required")
	}
	if cfg.Pass == "" {
		return nil, errors.New("btc rpc pass is required")
	}
	if cfg.Params == "" {
		cfg.Params = "mainnet"
	}
	cli, err := rpcclient.New(&rpcclient.ConnConfig{
		Host:         cfg.Host,
		User:         cfg.User,
		Pass:         cfg.Pass,
		Params:       cfg.Params,
		HTTPPostMode: true,
		DisableTLS:   cfg.DisableTLS,
	}, nil)
	if err != nil {
		return nil, err
	}
	return &Client{cli: cli}, nil
}

func (c *Client) Close() {
	if c == nil || c.cli == nil {
		return
	}
	c.cli.Shutdown()
	c.cli.WaitForShutdown()
}
