package btc

import (
	"context"
	"errors"
	"math"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/btcutil"
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

func (c *Client) ListUnspentByAddress(ctx context.Context, addr btcutil.Address, minConf int) ([]UTXO, error) {
	if c == nil || c.cli == nil {
		return nil, errors.New("btc rpc client is required")
	}
	if addr == nil {
		return nil, errors.New("btc address is required")
	}
	if minConf < 0 {
		minConf = 0
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	items, err := c.cli.ListUnspentMinMaxAddresses(minConf, math.MaxInt32, []btcutil.Address{addr})
	if err != nil {
		return nil, err
	}
	utxos := make([]UTXO, 0, len(items))
	for _, item := range items {
		if !item.Spendable {
			continue
		}
		sats, err := btcutil.NewAmount(item.Amount)
		if err != nil || sats <= 0 {
			continue
		}
		utxos = append(utxos, UTXO{
			TxID:          item.TxID,
			Vout:          item.Vout,
			ValueSat:      int64(sats),
			Confirmations: item.Confirmations,
		})
	}
	return utxos, nil
}

func (c *Client) EstimateSatPerVByte(confTarget int64) (int64, error) {
	if c == nil || c.cli == nil {
		return 0, errors.New("btc rpc client is required")
	}
	if confTarget <= 0 {
		confTarget = 2
	}
	res, err := c.cli.EstimateSmartFee(confTarget, (*btcjson.EstimateSmartFeeMode)(nil))
	if err != nil {
		return 0, err
	}
	if res == nil || res.FeeRate == nil {
		return 0, errors.New("btc fee rate unavailable")
	}
	// bitcoind returns BTC/kvB. Convert to sat/vB.
	satPerVByte := int64((*res.FeeRate) * 1e8 / 1000)
	if satPerVByte <= 0 {
		return 0, errors.New("invalid btc fee rate")
	}
	return satPerVByte, nil
}
