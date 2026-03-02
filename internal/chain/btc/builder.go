package btc

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
)

func (c *Client) ListUnspentByAddress(ctx context.Context, addr btcutil.Address, minConf int) ([]UTXO, error) {
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

	rows, err := c.fetchAddressUTXOs(ctx, addr.EncodeAddress())
	if err != nil {
		return nil, err
	}
	tip, _ := c.fetchTipHeight(ctx)
	utxos := make([]UTXO, 0, len(rows))
	for _, item := range rows {
		if item.Value <= 0 {
			continue
		}
		var conf int64
		if item.Status.Confirmed && item.Status.BlockHeight > 0 && tip >= uint64(item.Status.BlockHeight) {
			conf = int64(tip) - item.Status.BlockHeight + 1
		}
		if conf < int64(minConf) {
			continue
		}
		utxos = append(utxos, UTXO{
			TxID:          strings.TrimSpace(item.TxID),
			Vout:          item.Vout,
			ValueSat:      item.Value,
			Confirmations: conf,
		})
	}
	return utxos, nil
}

func (c *Client) EstimateSatPerVByte(confTarget int64) (int64, error) {
	if confTarget <= 0 {
		confTarget = 2
	}
	table, err := c.fetchFeeEstimates(context.Background())
	if err != nil {
		return 0, err
	}
	if len(table) == 0 {
		return 0, errors.New("btc fee rate unavailable")
	}
	targetKey := strconv.FormatInt(confTarget, 10)
	rate, ok := table[targetKey]
	if !ok || rate <= 0 {
		rate = 0
		for _, v := range table {
			if v <= 0 {
				continue
			}
			if rate == 0 || v < rate {
				rate = v
			}
		}
	}
	if rate <= 0 {
		return 0, errors.New("invalid btc fee rate")
	}
	return int64(math.Ceil(rate)), nil
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
