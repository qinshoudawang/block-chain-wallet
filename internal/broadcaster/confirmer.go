package broadcaster

import (
	"context"
	"log"
	"time"

	"wallet-system/internal/broadcaster/chainclient"
	"wallet-system/internal/helpers"
	"wallet-system/internal/storage/repo"
)

const (
	maxRetry = 4
)

type confirmThresholds struct {
	evm int
	btc int
	sol int
}

func RunConfirmer(ctx context.Context, wr *repo.WithdrawRepo, lr *repo.LedgerRepo, clients *chainclient.Registry) {
	thresholds := loadConfirmThresholds()
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
			latestByChain := make(map[string]uint64)
			for _, o := range items {
				chain, cli, err := clients.Resolve(o.Chain)
				if err != nil {
					log.Printf("[confirmer] resolve chain client failed withdraw_id=%s chain=%s err=%v", o.WithdrawID, o.Chain, err)
					continue
				}
				threshold := thresholds.forChain(chain)
				latest, ok := latestByChain[chain]
				if !ok {
					latest, err = cli.GetLatestHeight(ctx)
					if err != nil {
						log.Printf("[confirmer] get latest height failed chain=%s err=%v", chain, err)
						continue
					}
					latestByChain[chain] = latest
				}
				cf, err := cli.GetConfirmation(ctx, o.TxHash, o.Amount, latest)
				if err != nil {
					log.Printf("[confirmer] get confirmation failed withdraw_id=%s tx_hash=%s chain=%s err=%v", o.WithdrawID, o.TxHash, chain, err)
					continue
				}
				if cf == nil {
					// pending / not indexed yet
					continue
				}
				bn := cf.BlockNumber
				conf := cf.Confirmations

				if conf >= threshold {
					if cf.Settlement != nil {
						ok, err := wr.ConfirmWithSettlement(
							ctx,
							lr,
							o.WithdrawID,
							bn,
							conf,
							threshold,
							cf.Settlement.GasUsed,
							cf.Settlement.GasPriceWei,
							cf.Settlement.GasFeeWei,
							cf.Settlement.ActualSpentWei,
						)
						if err != nil || !ok {
							log.Printf("[confirmer] confirm with settlement skipped withdraw_id=%s tx_hash=%s chain=%s ok=%v err=%v", o.WithdrawID, o.TxHash, chain, ok, err)
							continue
						}
						log.Printf("[confirmer] confirm settled withdraw_id=%s tx_hash=%s chain=%s block=%d conf=%d threshold=%d", o.WithdrawID, o.TxHash, chain, bn, conf, threshold)
						continue
					}
					ok, err := wr.UpdateConfirmations(ctx, o.WithdrawID, bn, conf, threshold)
					if err != nil || !ok {
						log.Printf("[confirmer] confirm without settlement skipped withdraw_id=%s tx_hash=%s chain=%s block=%d conf=%d ok=%v err=%v", o.WithdrawID, o.TxHash, chain, bn, conf, ok, err)
						continue
					}
					continue
				}

				ok, err = wr.UpdateConfirmations(ctx, o.WithdrawID, bn, conf, threshold)
				if err != nil || !ok {
					log.Printf("[confirmer] update confirmations skipped withdraw_id=%s tx_hash=%s chain=%s block=%d conf=%d ok=%v err=%v", o.WithdrawID, o.TxHash, chain, bn, conf, ok, err)
					continue
				}
			}
		}
	}
}

func loadConfirmThresholds() confirmThresholds {
	return confirmThresholds{
		evm: normalizeThreshold(helpers.ParseIntEnv("EVM_CONFIRM_THRESHOLD", 5), 5),
		btc: normalizeThreshold(helpers.ParseIntEnv("BTC_CONFIRM_THRESHOLD", 2), 2),
		sol: normalizeThreshold(helpers.ParseIntEnv("SOL_CONFIRM_THRESHOLD", 5), 5),
	}
}

func (t confirmThresholds) forChain(chain string) int {
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return 5
	}
	switch spec.Family {
	case "btc":
		return t.btc
	case "evm":
		return t.evm
	case "sol":
		return t.sol
	default:
		return 5
	}
}

func normalizeThreshold(v int, fallback int) int {
	if v > 0 {
		return v
	}
	return fallback
}
