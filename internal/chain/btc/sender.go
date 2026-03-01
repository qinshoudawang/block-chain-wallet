package btc

import (
	"bytes"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
)

func (c *Client) BroadcastRawTxHex(rawTxHex string) (string, error) {
	if c == nil || c.cli == nil {
		return "", errors.New("btc rpc client is required")
	}
	raw, err := hex.DecodeString(strings.TrimPrefix(strings.TrimSpace(rawTxHex), "0x"))
	if err != nil {
		return "", err
	}
	tx := &wire.MsgTx{}
	if err := tx.Deserialize(bytes.NewReader(raw)); err != nil {
		return "", err
	}
	txHash, err := c.cli.SendRawTransaction(tx, false)
	if err != nil {
		return "", err
	}
	return txHash.String(), nil
}

func (c *Client) LatestHeight() (uint64, error) {
	if c == nil || c.cli == nil {
		return 0, errors.New("btc rpc client is required")
	}
	n, err := c.cli.GetBlockCount()
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, errors.New("invalid btc block height")
	}
	return uint64(n), nil
}

func (c *Client) GetRawTransactionVerbose(txHash string) (*btcjson.TxRawResult, error) {
	if c == nil || c.cli == nil {
		return nil, errors.New("btc rpc client is required")
	}
	h, err := chainhash.NewHashFromStr(strings.TrimSpace(txHash))
	if err != nil {
		return nil, err
	}
	return c.cli.GetRawTransactionVerbose(h)
}

