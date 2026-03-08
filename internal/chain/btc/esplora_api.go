package btc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type esploraUTXOStatus struct {
	Confirmed   bool  `json:"confirmed"`
	BlockHeight int64 `json:"block_height"`
}

type esploraUTXORow struct {
	TxID   string            `json:"txid"`
	Vout   uint32            `json:"vout"`
	Value  int64             `json:"value"`
	Status esploraUTXOStatus `json:"status"`
}

type esploraTxVin struct {
	TxID       string   `json:"txid"`
	Vout       uint32   `json:"vout"`
	IsCoinbase bool     `json:"is_coinbase"`
	Coinbase   string   `json:"coinbase"`
	Sequence   uint32   `json:"sequence"`
	Witness    []string `json:"witness"`
}

type esploraTxVout struct {
	Value            int64  `json:"value"`
	ScriptPubKey     string `json:"scriptpubkey"`
	ScriptPubKeyAsm  string `json:"scriptpubkey_asm"`
	ScriptPubKeyType string `json:"scriptpubkey_type"`
	ScriptPubKeyAddr string `json:"scriptpubkey_address"`
}

type esploraTxStatus struct {
	Confirmed   bool   `json:"confirmed"`
	BlockHeight int64  `json:"block_height"`
	BlockHash   string `json:"block_hash"`
	BlockTime   int64  `json:"block_time"`
}

type esploraBlockRow struct {
	ID                string `json:"id"`
	Height            int64  `json:"height"`
	PreviousBlockHash string `json:"previousblockhash"`
}

type esploraTxRow struct {
	TxID     string          `json:"txid"`
	Version  uint32          `json:"version"`
	Locktime uint32          `json:"locktime"`
	Size     int32           `json:"size"`
	Weight   int32           `json:"weight"`
	Vin      []esploraTxVin  `json:"vin"`
	Vout     []esploraTxVout `json:"vout"`
	Status   esploraTxStatus `json:"status"`
}

func (c *Client) fetchAddressUTXOs(ctx context.Context, addr string) ([]esploraUTXORow, error) {
	if strings.TrimSpace(addr) == "" {
		return nil, errors.New("btc address is required")
	}
	raw, err := c.httpRequest(ctx, http.MethodGet, "/address/"+url.PathEscape(addr)+"/utxo", "", nil)
	if err != nil {
		return nil, err
	}
	var rows []esploraUTXORow
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func (c *Client) fetchFeeEstimates(ctx context.Context) (map[string]float64, error) {
	raw, err := c.httpRequest(ctx, http.MethodGet, "/fee-estimates", "", nil)
	if err != nil {
		return nil, err
	}
	var table map[string]float64
	if err := json.Unmarshal(raw, &table); err != nil {
		return nil, err
	}
	return table, nil
}

func (c *Client) postRawTransaction(ctx context.Context, rawTxHex string) (string, error) {
	rawTxHex = strings.TrimPrefix(strings.TrimSpace(rawTxHex), "0x")
	if rawTxHex == "" {
		return "", errors.New("raw tx is required")
	}
	raw, err := c.httpRequest(ctx, http.MethodPost, "/tx", "text/plain", strings.NewReader(rawTxHex))
	if err != nil {
		return "", err
	}
	txid := strings.Trim(strings.TrimSpace(string(raw)), "\"")
	if txid == "" {
		return "", errors.New("empty btc txid")
	}
	return txid, nil
}

func (c *Client) fetchTipHeight(ctx context.Context) (uint64, error) {
	raw, err := c.httpRequest(ctx, http.MethodGet, "/blocks/tip/height", "", nil)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
}

func (c *Client) fetchBlockHashByHeight(ctx context.Context, height uint64) (string, error) {
	raw, err := c.httpRequest(ctx, http.MethodGet, "/block-height/"+strconv.FormatUint(height, 10), "", nil)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(raw)), nil
}

func (c *Client) fetchBlock(ctx context.Context, hash string) (*esploraBlockRow, error) {
	hash = strings.TrimSpace(hash)
	if hash == "" {
		return nil, errors.New("block hash is required")
	}
	raw, err := c.httpRequest(ctx, http.MethodGet, "/block/"+url.PathEscape(hash), "", nil)
	if err != nil {
		return nil, err
	}
	var row esploraBlockRow
	if err := json.Unmarshal(raw, &row); err != nil {
		return nil, err
	}
	return &row, nil
}

func (c *Client) fetchBlockTxIDs(ctx context.Context, hash string) ([]string, error) {
	hash = strings.TrimSpace(hash)
	if hash == "" {
		return nil, errors.New("block hash is required")
	}
	raw, err := c.httpRequest(ctx, http.MethodGet, "/block/"+url.PathEscape(hash)+"/txids", "", nil)
	if err != nil {
		return nil, err
	}
	var rows []string
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func (c *Client) fetchTransaction(ctx context.Context, txid string) (*esploraTxRow, error) {
	txid = strings.TrimSpace(txid)
	if txid == "" {
		return nil, errors.New("tx hash is required")
	}
	raw, err := c.httpRequest(ctx, http.MethodGet, "/tx/"+url.PathEscape(txid), "", nil)
	if err != nil {
		return nil, err
	}
	var row esploraTxRow
	if err := json.Unmarshal(raw, &row); err != nil {
		return nil, err
	}
	return &row, nil
}
