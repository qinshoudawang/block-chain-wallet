package btcprovider

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	rootprovider "wallet-system/internal/signer/provider"

	"github.com/btcsuite/btcd/wire"
)

type unsignedWithdrawTx struct {
	RawTx    string              `json:"raw_tx"`
	Prevouts []unsignedTxPrevout `json:"prevouts"`
}

type unsignedTxPrevout struct {
	Value    string `json:"value"`
	PkScript string `json:"pk_script"`
}

func parseUnsignedWithdrawTx(unsignedTx []byte) (unsignedWithdrawTx, error) {
	var req unsignedWithdrawTx
	if err := json.Unmarshal(unsignedTx, &req); err != nil {
		return unsignedWithdrawTx{}, err
	}
	if strings.TrimSpace(req.RawTx) == "" {
		return unsignedWithdrawTx{}, errors.New("empty btc raw tx")
	}
	if len(req.Prevouts) == 0 {
		return unsignedWithdrawTx{}, errors.New("empty btc prevouts")
	}
	return req, nil
}

func decodeUnsignedRawTx(rawTxHex string) (*wire.MsgTx, error) {
	raw, err := hex.DecodeString(rootprovider.Trim0x(strings.TrimSpace(rawTxHex)))
	if err != nil {
		return nil, errors.New("invalid btc raw tx")
	}
	tx := &wire.MsgTx{}
	if err := tx.Deserialize(bytes.NewReader(raw)); err != nil {
		return nil, errors.New("invalid btc raw tx")
	}
	if len(tx.TxIn) == 0 {
		return nil, errors.New("empty btc tx inputs")
	}
	return tx, nil
}

func parsePrevoutsForSign(tx *wire.MsgTx, prevouts []unsignedTxPrevout) ([]int64, [][]byte, error) {
	if len(prevouts) != len(tx.TxIn) {
		return nil, nil, errors.New("btc prevouts/input mismatch")
	}
	inputValues := make([]int64, len(prevouts))
	pkScripts := make([][]byte, len(prevouts))
	for i := range prevouts {
		v, err := strconv.ParseInt(strings.TrimSpace(prevouts[i].Value), 10, 64)
		if err != nil || v <= 0 {
			return nil, nil, errors.New("invalid btc prevout value")
		}
		script, err := hex.DecodeString(rootprovider.Trim0x(strings.TrimSpace(prevouts[i].PkScript)))
		if err != nil || len(script) == 0 {
			return nil, nil, errors.New("invalid btc prevout pk_script")
		}
		inputValues[i] = v
		pkScripts[i] = script
	}
	return inputValues, pkScripts, nil
}

func serializeTx(tx *wire.MsgTx) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(tx.SerializeSize())
	if err := tx.Serialize(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
