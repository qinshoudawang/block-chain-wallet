package chainclient

import (
	"context"
	"errors"
	"strings"

	btcchain "wallet-system/internal/chain/btc"
)

type btcClient struct {
	rpc *btcchain.Client
}

func newBTCClient(rpc *btcchain.Client) Client { return &btcClient{rpc: rpc} }

func NewBTCClient(rpc *btcchain.Client) Client { return newBTCClient(rpc) }

func (c *btcClient) BroadcastSignedTxHex(ctx context.Context, signedTxHex string) (string, error) {
	if c == nil || c.rpc == nil {
		return "", errors.New("btc rpc client is required")
	}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}
	return c.rpc.BroadcastRawTxHex(strings.TrimSpace(signedTxHex))
}

func (c *btcClient) GetLatestHeight(ctx context.Context) (uint64, error) {
	if c == nil || c.rpc == nil {
		return 0, errors.New("btc rpc client is required")
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	return c.rpc.LatestHeight()
}

func (c *btcClient) GetConfirmation(ctx context.Context, txHash string, amount string, latestHeight uint64) (*Confirmation, error) {
	_ = amount
	if c == nil || c.rpc == nil {
		return nil, errors.New("btc rpc client is required")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	tx, err := c.rpc.GetRawTransactionVerbose(strings.TrimSpace(txHash))
	if err != nil || tx == nil {
		// Not found / not indexed yet: caller treats as pending.
		return nil, nil
	}
	if tx.Confirmations == 0 {
		return nil, nil
	}
	conf := int(tx.Confirmations)
	var bn uint64
	if latestHeight+1 >= uint64(conf) {
		bn = latestHeight - uint64(conf) + 1
	}
	return &Confirmation{
		BlockNumber:   bn,
		Confirmations: conf,
		Settlement:    nil,
	}, nil
}

