package provider

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
)

type BTCSigner struct {
	privKey *btcec.PrivateKey
}

type btcUnsignedWithdrawTx struct {
	RawTx    string                 `json:"raw_tx"`
	Prevouts []btcUnsignedTxPrevout `json:"prevouts"`
}

type btcUnsignedTxPrevout struct {
	Value    string `json:"value"`
	PkScript string `json:"pk_script"`
}

func NewBTCSigner(hexPriv string) (*BTCSigner, error) {
	if strings.TrimSpace(hexPriv) == "" {
		return nil, errors.New("empty BTC private key")
	}
	raw, err := hex.DecodeString(trim0x(hexPriv))
	if err != nil {
		return nil, err
	}
	priv, _ := btcec.PrivKeyFromBytes(raw)
	return &BTCSigner{
		privKey: priv,
	}, nil
}

func (s *BTCSigner) Sign(unsignedTx []byte) ([]byte, error) {
	if s == nil || s.privKey == nil {
		return nil, errors.New("btc private key is required")
	}
	req, err := parseBTCUnsignedWithdrawTx(unsignedTx)
	if err != nil {
		return nil, err
	}
	tx, err := decodeBTCUnsignedRawTx(req.RawTx)
	if err != nil {
		return nil, err
	}
	inputValues, pkScripts, err := parseBTCPrevoutsForSign(tx, req.Prevouts)
	if err != nil {
		return nil, err
	}
	if err := signBTCInputs(tx, inputValues, pkScripts, s.privKey); err != nil {
		return nil, err
	}
	raw, err := serializeBTCTx(tx)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func parseBTCUnsignedWithdrawTx(unsignedTx []byte) (btcUnsignedWithdrawTx, error) {
	var req btcUnsignedWithdrawTx
	if err := json.Unmarshal(unsignedTx, &req); err != nil {
		return btcUnsignedWithdrawTx{}, err
	}
	if strings.TrimSpace(req.RawTx) == "" {
		return btcUnsignedWithdrawTx{}, errors.New("empty btc raw tx")
	}
	if len(req.Prevouts) == 0 {
		return btcUnsignedWithdrawTx{}, errors.New("empty btc prevouts")
	}
	return req, nil
}

func decodeBTCUnsignedRawTx(rawTxHex string) (*wire.MsgTx, error) {
	raw, err := hex.DecodeString(trim0x(strings.TrimSpace(rawTxHex)))
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

func parseBTCPrevoutsForSign(tx *wire.MsgTx, prevouts []btcUnsignedTxPrevout) ([]int64, [][]byte, error) {
	if len(prevouts) != len(tx.TxIn) {
		return nil, nil, errors.New("btc prevouts/input mismatch")
	}
	inputValues := make([]int64, len(prevouts))
	pkScripts := make([][]byte, len(prevouts))
	for i := range prevouts {
		v, err := parseBTCSatoshis(prevouts[i].Value)
		if err != nil || v <= 0 {
			return nil, nil, errors.New("invalid btc prevout value")
		}
		script, err := hex.DecodeString(trim0x(strings.TrimSpace(prevouts[i].PkScript)))
		if err != nil || len(script) == 0 {
			return nil, nil, errors.New("invalid btc prevout pk_script")
		}
		inputValues[i] = v
		pkScripts[i] = script
	}
	return inputValues, pkScripts, nil
}

func signBTCInputs(tx *wire.MsgTx, inputValues []int64, pkScripts [][]byte, priv *btcec.PrivateKey) error {
	prevOuts := make(map[wire.OutPoint]*wire.TxOut, len(tx.TxIn))
	for i := range tx.TxIn {
		prevOuts[tx.TxIn[i].PreviousOutPoint] = &wire.TxOut{
			Value:    inputValues[i],
			PkScript: pkScripts[i],
		}
	}
	fetcher := txscript.NewMultiPrevOutFetcher(prevOuts)
	sigHashes := txscript.NewTxSigHashes(tx, fetcher)
	for i := range tx.TxIn {
		witness, err := txscript.WitnessSignature(
			tx, sigHashes, i, inputValues[i], pkScripts[i], txscript.SigHashAll, priv, true,
		)
		if err != nil {
			return err
		}
		tx.TxIn[i].Witness = witness
	}
	return nil
}

func serializeBTCTx(tx *wire.MsgTx) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(tx.SerializeSize())
	if err := tx.Serialize(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func parseBTCSatoshis(v string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(v), 10, 64)
}
