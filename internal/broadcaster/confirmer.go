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
	withdrawpb "wallet-system/proto/withdraw"
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
	rbfStuckFor time.Duration
	withdrawRPC withdrawpb.WithdrawServiceClient
}

func NewConfirmer(
	wr *repo.WithdrawRepo,
	lr *repo.LedgerRepo,
	clients *chainclient.Registry,
	utxoReserve *utxoreserve.Manager,
	withdrawRPC withdrawpb.WithdrawServiceClient,
) *Confirmer {
	return &Confirmer{
		wr:          wr,
		lr:          lr,
		clients:     clients,
		utxoReserve: utxoReserve,
		thresholds:  loadConfirmThresholds(),
		rbfStuckFor: loadBTCRBFStuckThreshold(),
		withdrawRPC: withdrawRPC,
	}
}

func RunConfirmer(
	ctx context.Context,
	wr *repo.WithdrawRepo,
	lr *repo.LedgerRepo,
	clients *chainclient.Registry,
	utxoReserve *utxoreserve.Manager,
	withdrawRPC withdrawpb.WithdrawServiceClient,
) {
	NewConfirmer(wr, lr, clients, utxoReserve, withdrawRPC).Run(ctx)
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
		if c.shouldTriggerRBF(chain, o, 0) {
			c.tryRBF(ctx, o, "pending_not_indexed", 0)
		}
		return
	}

	if cf.Confirmations >= threshold {
		c.confirmFinal(ctx, chain, o, cf, threshold)
		return
	}
	if c.shouldTriggerRBF(chain, o, cf.Confirmations) {
		c.tryRBF(ctx, o, "stuck_unconfirmed", cf.Confirmations)
	}

	ok, err = c.wr.UpdateConfirmations(ctx, o.WithdrawID, cf.BlockNumber, cf.Confirmations, threshold)
	if err != nil || !ok {
		log.Printf(
			"[confirmer] update confirmations skipped withdraw_id=%s tx_hash=%s chain=%s block=%d conf=%d ok=%v err=%v",
			o.WithdrawID, o.TxHash, chain, cf.BlockNumber, cf.Confirmations, ok, err,
		)
	}
}

func (c *Confirmer) shouldTriggerRBF(chain string, o model.WithdrawOrder, conf int) bool {
	if c == nil {
		return false
	}
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil || spec.Family != helpers.FamilyBTC {
		return false
	}
	if conf > 0 || c.rbfStuckFor <= 0 {
		return false
	}
	return time.Since(o.UpdatedAt) >= c.rbfStuckFor
}

func (c *Confirmer) tryRBF(
	ctx context.Context,
	o model.WithdrawOrder,
	reason string,
	confirmations int,
) {
	if c.withdrawRPC == nil {
		return
	}
	resp, err := c.withdrawRPC.SubmitRBF(ctx, &withdrawpb.SubmitRBFRequest{
		WithdrawId: o.WithdrawID,
		OldTxHash:  o.TxHash,
	})
	if err != nil {
		log.Printf("[confirmer] rbf submit failed withdraw_id=%s tx_hash=%s err=%v", o.WithdrawID, o.TxHash, err)
		return
	}
	log.Printf(
		"[confirmer] rbf submitted withdraw_id=%s old_tx=%s request_id=%s status=%s reason=%s confirmations=%d stuck_for=%s old_rate=%d new_rate=%d old_fee=%d new_fee=%d",
		o.WithdrawID, o.TxHash, resp.GetRequestId(), resp.GetStatus(), reason, confirmations, c.rbfStuckFor.String(),
		resp.GetOldFeeRateSatPerVbyte(), resp.GetNewFeeRateSatPerVbyte(), resp.GetOldFeeSat(), resp.GetNewFeeSat(),
	)
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

func loadBTCRBFStuckThreshold() time.Duration {
	minutes := helpers.ParseIntEnv("BTC_RBF_STUCK_MINUTES", 30)
	if minutes <= 0 {
		return 0
	}
	return time.Duration(minutes) * time.Minute
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
