package btc

import (
	"context"
	"encoding/hex"
	"strings"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/btcutil"
)

func (c *Client) BroadcastRawTxHex(rawTxHex string) (string, error) {
	rawTxHex = strings.TrimPrefix(strings.TrimSpace(rawTxHex), "0x")
	if _, err := hex.DecodeString(rawTxHex); err != nil {
		return "", err
	}
	return c.postRawTransaction(context.Background(), rawTxHex)
}

func (c *Client) LatestHeight() (uint64, error) {
	return c.fetchTipHeight(context.Background())
}

func (c *Client) GetRawTransactionVerbose(txHash string) (*btcjson.TxRawResult, error) {
	row, err := c.fetchTransaction(context.Background(), txHash)
	if err != nil {
		return nil, err
	}
	out := &btcjson.TxRawResult{
		Txid:      row.TxID,
		Version:   row.Version,
		LockTime:  row.Locktime,
		Size:      row.Size,
		Weight:    row.Weight,
		Vsize:     int32((row.Weight + 3) / 4),
		BlockHash: row.Status.BlockHash,
		Blocktime: row.Status.BlockTime,
		Time:      row.Status.BlockTime,
		Vin:       make([]btcjson.Vin, 0, len(row.Vin)),
		Vout:      make([]btcjson.Vout, 0, len(row.Vout)),
	}
	if row.Status.Confirmed && row.Status.BlockHeight > 0 {
		if tip, err := c.LatestHeight(); err == nil && tip >= uint64(row.Status.BlockHeight) {
			out.Confirmations = tip - uint64(row.Status.BlockHeight) + 1
		}
	}
	for _, vin := range row.Vin {
		item := btcjson.Vin{
			Txid:     vin.TxID,
			Vout:     vin.Vout,
			Sequence: vin.Sequence,
			Witness:  vin.Witness,
		}
		if vin.IsCoinbase {
			if vin.Coinbase != "" {
				item.Coinbase = vin.Coinbase
			} else {
				item.Coinbase = "coinbase"
			}
		}
		out.Vin = append(out.Vin, item)
	}
	for i, vout := range row.Vout {
		out.Vout = append(out.Vout, btcjson.Vout{
			Value: btcutil.Amount(vout.Value).ToBTC(),
			N:     uint32(i),
			ScriptPubKey: btcjson.ScriptPubKeyResult{
				Asm:     vout.ScriptPubKeyAsm,
				Hex:     vout.ScriptPubKey,
				Type:    vout.ScriptPubKeyType,
				Address: vout.ScriptPubKeyAddr,
			},
		})
	}
	return out, nil
}
