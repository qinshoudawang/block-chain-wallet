package broadcaster

import (
	"context"
	"log"
	"time"

	"wallet-system/internal/broadcaster/chainclient"
	"wallet-system/internal/storage/repo"
)

const (
	maxRetry  = 4
	threshold = 5
)

func RunConfirmer(ctx context.Context, wr *repo.WithdrawRepo, lr *repo.LedgerRepo, clients *chainclient.Registry) {
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
