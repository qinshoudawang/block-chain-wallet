package btc

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"math"
	"strconv"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
)

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

type UnsignedTxPrevout struct {
	Value    string `json:"value"`
	PkScript string `json:"pk_script"`
}

func (c *Client) BuildUnsignedWithdrawTx(fromAddr, toAddr btcutil.Address, selected []UTXO, amount, change int64) (string, []UnsignedTxPrevout, error) {
	if fromAddr == nil || toAddr == nil {
		return "", nil, errors.New("btc address is required")
	}
	if amount <= 0 {
		return "", nil, errors.New("invalid btc amount")
	}
	if change < 0 {
		return "", nil, errors.New("invalid btc change")
	}
	if len(selected) == 0 {
		return "", nil, errors.New("empty btc inputs")
	}

	fromPkScript, err := txscript.PayToAddrScript(fromAddr)
	if err != nil {
		return "", nil, err
	}
	toPkScript, err := txscript.PayToAddrScript(toAddr)
	if err != nil {
		return "", nil, err
	}

	tx := wire.NewMsgTx(1)
	for i := range selected {
		if selected[i].ValueSat <= 0 {
			return "", nil, errors.New("invalid btc input value")
		}
		h, err := chainhash.NewHashFromStr(selected[i].TxID)
		if err != nil {
			return "", nil, errors.New("invalid btc input txid")
		}
		tx.AddTxIn(&wire.TxIn{
			PreviousOutPoint: wire.OutPoint{Hash: *h, Index: selected[i].Vout},
			Sequence:         wire.MaxTxInSequenceNum,
		})
	}
	tx.AddTxOut(&wire.TxOut{Value: amount, PkScript: toPkScript})
	if change > 0 {
		tx.AddTxOut(&wire.TxOut{Value: change, PkScript: fromPkScript})
	}

	var buf bytes.Buffer
	buf.Grow(tx.SerializeSize())
	if err := tx.Serialize(&buf); err != nil {
		return "", nil, err
	}

	prevouts := make([]UnsignedTxPrevout, 0, len(selected))
	pkScriptHex := hex.EncodeToString(fromPkScript)
	for i := range selected {
		prevouts = append(prevouts, UnsignedTxPrevout{
			Value:    strconv.FormatInt(selected[i].ValueSat, 10),
			PkScript: pkScriptHex,
		})
	}
	return hex.EncodeToString(buf.Bytes()), prevouts, nil
}
