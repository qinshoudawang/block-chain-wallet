package btcprovider

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"

	rootprovider "wallet-system/internal/signer/provider"
	signpb "wallet-system/proto/signer"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
)

type Signer struct {
	privKey *btcec.PrivateKey
}

func NewSigner(hexPriv string) (*Signer, error) {
	if strings.TrimSpace(hexPriv) == "" {
		return nil, errors.New("empty BTC private key")
	}
	raw, err := hex.DecodeString(rootprovider.Trim0x(hexPriv))
	if err != nil {
		return nil, err
	}
	priv, _ := btcec.PrivKeyFromBytes(raw)
	return &Signer{privKey: priv}, nil
}

func (s *Signer) Sign(ctx context.Context, req *signpb.SignRequest) ([]byte, error) {
	_ = ctx
	if s == nil || s.privKey == nil {
		return nil, errors.New("btc private key is required")
	}
	if req == nil {
		return nil, errors.New("sign request is nil")
	}
	unsignedReq, err := parseUnsignedWithdrawTx(req.GetUnsignedTx())
	if err != nil {
		return nil, err
	}
	tx, err := decodeUnsignedRawTx(unsignedReq.RawTx)
	if err != nil {
		return nil, err
	}
	inputValues, pkScripts, err := parsePrevoutsForSign(tx, unsignedReq.Prevouts)
	if err != nil {
		return nil, err
	}
	if err := signInputs(tx, inputValues, pkScripts, s.privKey); err != nil {
		return nil, err
	}
	return serializeTx(tx)
}

func signInputs(tx *wire.MsgTx, inputValues []int64, pkScripts [][]byte, priv *btcec.PrivateKey) error {
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
