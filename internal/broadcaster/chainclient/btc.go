package chainclient

import (
	"context"
	"errors"
	"math/big"
	"strings"

	btcchain "wallet-system/internal/chain/btc"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/btcutil"
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

func (c *btcClient) GetConfirmation(ctx context.Context, txHash string, amount string, tokenContractAddress string, latestHeight uint64) (*Confirmation, error) {
	_ = tokenContractAddress
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
	settlement, err := c.calcSettlement(ctx, tx, amount)
	if err != nil {
		return nil, err
	}
	return &Confirmation{
		BlockNumber:   bn,
		Confirmations: conf,
		Settlement:    settlement,
	}, nil
}

func (c *btcClient) calcSettlement(ctx context.Context, tx *btcjson.TxRawResult, amountStr string) (*Settlement, error) {
	amount := new(big.Int)
	if _, ok := amount.SetString(strings.TrimSpace(amountStr), 10); !ok || amount.Sign() < 0 {
		return nil, errors.New("invalid amount")
	}
	totalInSat, err := c.sumInputsSat(ctx, tx)
	if err != nil {
		return nil, err
	}
	totalOutSat, err := sumOutputsSat(tx)
	if err != nil {
		return nil, err
	}
	if totalInSat < totalOutSat {
		return nil, errors.New("invalid btc tx: inputs less than outputs")
	}
	feeSat := totalInSat - totalOutSat
	feeAmount := big.NewInt(feeSat)
	return &Settlement{
		TransferAssetContractAddress:   "",
		TransferSpentAmount:            amount,
		NetworkFeeAssetContractAddress: "",
		NetworkFeeAmount:               feeAmount,
	}, nil
}

func (c *btcClient) sumInputsSat(ctx context.Context, tx *btcjson.TxRawResult) (int64, error) {
	prevTxCache := make(map[string]*btcjson.TxRawResult, len(tx.Vin))
	var total int64
	for i := range tx.Vin {
		vin := tx.Vin[i]
		if vin.IsCoinBase() {
			return 0, errors.New("coinbase vin is not supported")
		}
		txid := strings.TrimSpace(vin.Txid)
		if txid == "" {
			return 0, errors.New("invalid btc vin txid")
		}
		prevTx, ok := prevTxCache[txid]
		if !ok {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			default:
			}
			var err error
			prevTx, err = c.rpc.GetRawTransactionVerbose(txid)
			if err != nil || prevTx == nil {
				return 0, errors.New("failed to load btc prev tx")
			}
			prevTxCache[txid] = prevTx
		}
		if int(vin.Vout) >= len(prevTx.Vout) {
			return 0, errors.New("invalid btc vin vout")
		}
		sat, err := btcFloatToSat(prevTx.Vout[vin.Vout].Value)
		if err != nil {
			return 0, err
		}
		total += sat
	}
	return total, nil
}

func sumOutputsSat(tx *btcjson.TxRawResult) (int64, error) {
	var total int64
	for i := range tx.Vout {
		sat, err := btcFloatToSat(tx.Vout[i].Value)
		if err != nil {
			return 0, err
		}
		total += sat
	}
	return total, nil
}

func btcFloatToSat(v float64) (int64, error) {
	amt, err := btcutil.NewAmount(v)
	if err != nil || amt < 0 {
		return 0, errors.New("invalid btc amount")
	}
	return int64(amt), nil
}
