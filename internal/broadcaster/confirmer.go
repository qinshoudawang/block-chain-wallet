package broadcaster

import (
	"context"
	"log"
	"time"

	"wallet-system/internal/broadcaster/chainclient"
	"wallet-system/internal/helpers"
	"wallet-system/internal/sequence/utxoreserve"
	"wallet-system/internal/storage/model"
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

type Confirmer struct {
	wr          *repo.WithdrawRepo
	lr          *repo.LedgerRepo
	clients     *chainclient.Registry
	utxoReserve *utxoreserve.Manager
	thresholds  confirmThresholds
}

func NewConfirmer(wr *repo.WithdrawRepo, lr *repo.LedgerRepo, clients *chainclient.Registry, utxoReserve *utxoreserve.Manager) *Confirmer {
	return &Confirmer{
		wr:          wr,
		lr:          lr,
		clients:     clients,
		utxoReserve: utxoReserve,
		thresholds:  loadConfirmThresholds(),
	}
}

func RunConfirmer(ctx context.Context, wr *repo.WithdrawRepo, lr *repo.LedgerRepo, clients *chainclient.Registry, utxoReserve *utxoreserve.Manager) {
	NewConfirmer(wr, lr, clients, utxoReserve).Run(ctx)
}

func (c *Confirmer) Run(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.tick(ctx)
		}
	}
}

func (c *Confirmer) tick(ctx context.Context) {
	items, err := c.wr.ListBroadcastedToConfirm(ctx, 200)
	if err != nil {
		log.Printf("[confirmer] list broadcasted failed err=%v", err)
		return
	}
	if len(items) == 0 {
		return
	}

	latestByChain := make(map[string]uint64)
	for _, o := range items {
		c.processOrder(ctx, latestByChain, o)
	}
}

func (c *Confirmer) processOrder(ctx context.Context, latestByChain map[string]uint64, o model.WithdrawOrder) {
	chain, cli, err := c.clients.Resolve(o.Chain)
	if err != nil {
		log.Printf("[confirmer] resolve chain client failed withdraw_id=%s chain=%s err=%v", o.WithdrawID, o.Chain, err)
		return
	}

	threshold := c.thresholds.forChain(chain)
	latest, ok := latestByChain[chain]
	if !ok {
		latest, err = cli.GetLatestHeight(ctx)
		if err != nil {
			log.Printf("[confirmer] get latest height failed chain=%s err=%v", chain, err)
			return
		}
		latestByChain[chain] = latest
	}

	cf, err := cli.GetConfirmation(ctx, o.TxHash, o.Amount, latest)
	if err != nil {
		log.Printf("[confirmer] get confirmation failed withdraw_id=%s tx_hash=%s chain=%s err=%v", o.WithdrawID, o.TxHash, chain, err)
		return
	}
	if cf == nil {
		// pending / not indexed yet
		return
	}

	if cf.Confirmations >= threshold {
		c.confirmFinal(ctx, chain, o, cf, threshold)
		return
	}

	ok, err = c.wr.UpdateConfirmations(ctx, o.WithdrawID, cf.BlockNumber, cf.Confirmations, threshold)
	if err != nil || !ok {
		log.Printf(
			"[confirmer] update confirmations skipped withdraw_id=%s tx_hash=%s chain=%s block=%d conf=%d ok=%v err=%v",
			o.WithdrawID, o.TxHash, chain, cf.BlockNumber, cf.Confirmations, ok, err,
		)
	}
}

func (c *Confirmer) confirmFinal(ctx context.Context, chain string, o model.WithdrawOrder, cf *chainclient.Confirmation, threshold int) {
	ok, err := c.wr.ConfirmWithSettlement(
		ctx,
		c.lr,
		o.WithdrawID,
		cf.BlockNumber,
		cf.Confirmations,
		threshold,
		cf.Settlement.NetworkFeeAmount,
		cf.Settlement.ActualSpentAmount,
	)
	if err != nil || !ok {
		log.Printf(
			"[confirmer] confirm with settlement skipped withdraw_id=%s tx_hash=%s chain=%s ok=%v err=%v",
			o.WithdrawID, o.TxHash, chain, ok, err,
		)
		return
	}

	log.Printf(
		"[confirmer] confirm settled withdraw_id=%s tx_hash=%s chain=%s block=%d conf=%d threshold=%d",
		o.WithdrawID, o.TxHash, chain, cf.BlockNumber, cf.Confirmations, threshold,
	)
	if c.utxoReserve != nil && helpers.NeedReservation(chain) {
		if err := c.utxoReserve.ReleaseByWithdrawID(ctx, o.WithdrawID); err != nil {
			log.Printf("[confirmer] release utxo reservation failed withdraw_id=%s chain=%s err=%v", o.WithdrawID, chain, err)
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
	case helpers.FamilyBTC:
		return t.btc
	case helpers.FamilyEVM:
		return t.evm
	case helpers.FamilySOL:
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
