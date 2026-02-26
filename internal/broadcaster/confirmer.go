package broadcaster

import (
	"context"
	"log"
	"math/big"
	"time"

	"wallet-system/internal/storage/repo"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

const (
	maxRetry  = 4
	threshold = 5
)

func RunConfirmer(ctx context.Context, wr *repo.WithdrawRepo, lr *repo.LedgerRepo, eth *ethclient.Client) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			items, err := wr.ListBroadcastedToConfirm(ctx, 200)
			if err != nil {
				log.Printf("[confirmer] list broadcasted failed err=%v", err)
				continue
			}
			if len(items) == 0 {
				continue
			}
			latest, err := eth.BlockNumber(ctx)
			if err != nil {
				log.Printf("[confirmer] get latest block failed err=%v", err)
				continue
			}
			for _, o := range items {
				receipt, err := eth.TransactionReceipt(ctx, common.HexToHash(o.TxHash))
				if err != nil {
					// 未出块/未索引到，先跳过
					continue
				}

				bn := receipt.BlockNumber.Uint64()
				conf := int(latest - bn + 1)

				if conf >= threshold {
					gasPriceWei, gasFeeWei, actualSpentWei, err := calcSettlementWei(o.Amount, receipt)
					if err != nil {
						log.Printf("[confirmer] calc settlement failed withdraw_id=%s tx_hash=%s err=%v", o.WithdrawID, o.TxHash, err)
						continue
					}
					ok, err := wr.ConfirmWithSettlement(
						ctx,
						lr,
						o.WithdrawID,
						bn,
						conf,
						threshold,
						receipt.GasUsed,
						gasPriceWei,
						gasFeeWei,
						actualSpentWei,
					)
					if err != nil || !ok {
						log.Printf("[confirmer] confirm with settlement skipped withdraw_id=%s tx_hash=%s ok=%v err=%v", o.WithdrawID, o.TxHash, ok, err)
						continue
					}
					log.Printf("[confirmer] confirm settled withdraw_id=%s tx_hash=%s block=%d conf=%d threshold=%d", o.WithdrawID, o.TxHash, bn, conf, threshold)
					continue
				}

				ok, err := wr.UpdateConfirmations(ctx, o.WithdrawID, bn, conf, threshold)
				if err != nil || !ok {
					log.Printf("[confirmer] update confirmations skipped withdraw_id=%s tx_hash=%s block=%d conf=%d ok=%v err=%v", o.WithdrawID, o.TxHash, bn, conf, ok, err)
					continue
				}
			}
		}
	}
}

func calcSettlementWei(amountWei string, receipt *ethtypes.Receipt) (gasPriceWei, gasFeeWei, actualSpentWei *big.Int, err error) {
	amount := new(big.Int)
	if _, ok := amount.SetString(amountWei, 10); !ok || amount.Sign() < 0 {
		return nil, nil, nil, errInvalidAmountWei
	}
	gasPriceWei = receipt.EffectiveGasPrice
	if gasPriceWei == nil {
		gasPriceWei = big.NewInt(0)
	}
	gasFeeWei = new(big.Int).Mul(new(big.Int).SetUint64(receipt.GasUsed), gasPriceWei)
	actualSpentWei = new(big.Int).Add(amount, gasFeeWei)
	return gasPriceWei, gasFeeWei, actualSpentWei, nil
}

var errInvalidAmountWei = invalidAmountWeiError{}

type invalidAmountWeiError struct{}

func (invalidAmountWeiError) Error() string { return "invalid amount wei" }
