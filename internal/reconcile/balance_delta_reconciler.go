package reconcile

import (
	"context"
	"errors"
	"log"
	"math/big"
	"strings"
	"time"

	"wallet-system/internal/infra/redisx"
	reconcilemodel "wallet-system/internal/storage/model/reconcile"
	"wallet-system/internal/storage/repo"

	"github.com/redis/go-redis/v9"
)

type BalanceDeltaReconcilerConfig struct {
	Chain          string
	HotAddress     string
	TokenContracts []string
	PollInterval   time.Duration
	LockTTL        time.Duration
}

type BalanceDeltaReconciler struct {
	cfg          BalanceDeltaReconcilerConfig
	redis        *redis.Client
	reconRepo    *repo.ReconcileRepo
	withdrawRepo *repo.WithdrawRepo
	sweepRepo    *repo.SweepRepo
}

func NewBalanceDeltaReconciler(
	cfg BalanceDeltaReconcilerConfig,
	redisClient *redis.Client,
	reconRepo *repo.ReconcileRepo,
	withdrawRepo *repo.WithdrawRepo,
	sweepRepo *repo.SweepRepo,
) *BalanceDeltaReconciler {
	return &BalanceDeltaReconciler{
		cfg:          cfg,
		redis:        redisClient,
		reconRepo:    reconRepo,
		withdrawRepo: withdrawRepo,
		sweepRepo:    sweepRepo,
	}
}

func (r *BalanceDeltaReconciler) Run(ctx context.Context) {
	if r == nil || r.reconRepo == nil || r.withdrawRepo == nil || r.sweepRepo == nil {
		return
	}
	poll := r.cfg.PollInterval
	if poll <= 0 {
		poll = 30 * time.Second
	}
	tk := time.NewTicker(poll)
	defer tk.Stop()

	r.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			r.tick(ctx)
		}
	}
}

func (r *BalanceDeltaReconciler) tick(ctx context.Context) {
	unlock, ok, err := r.acquireLock(ctx)
	if err != nil {
		log.Printf("[reconciler-balance-delta] acquire lock failed chain=%s err=%v", r.cfg.Chain, err)
		return
	}
	if !ok {
		return
	}
	defer unlock()

	hot := strings.TrimSpace(r.cfg.HotAddress)
	if hot == "" {
		return
	}
	for _, asset := range balanceDeltaAssets(r.cfg.TokenContracts) {
		r.reconcileHotDelta(ctx, hot, asset)
	}
}

func (r *BalanceDeltaReconciler) acquireLock(ctx context.Context) (func(), bool, error) {
	if r.redis == nil {
		return func() {}, true, nil
	}
	ttl := r.cfg.LockTTL
	if ttl <= 0 {
		ttl = 90 * time.Second
	}
	key := "lock:reconcile:balance-delta:" + strings.ToLower(strings.TrimSpace(r.cfg.Chain)) + ":hot"
	unlock, err := redisx.Acquire(ctx, r.redis, key, ttl)
	if err != nil {
		if errors.Is(err, redisx.ErrLockNotAcquired) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return unlock, true, nil
}

func (r *BalanceDeltaReconciler) reconcileHotDelta(ctx context.Context, hotAddress string, asset string) {
	logs, err := r.reconRepo.ListLatestOnchainLogs(ctx, reconcilemodel.ScopeHot, r.cfg.Chain, hotAddress, asset, 2)
	if err != nil {
		log.Printf("[reconciler-balance-delta] load checkpoints failed chain=%s address=%s asset=%s err=%v", r.cfg.Chain, hotAddress, asset, err)
		return
	}
	if len(logs) < 2 {
		return
	}
	current := logs[0]
	previous := logs[1]
	currentBal, ok := new(big.Int).SetString(strings.TrimSpace(current.LedgerBalanceAmount), 10)
	if !ok {
		r.saveHotDeltaResult(ctx, hotAddress, asset, previous.ReconciledAt, current.ReconciledAt, previous.LedgerBalanceAmount, current.LedgerBalanceAmount, "0", "0", reconcilemodel.StatusError, false, "invalid current ledger balance")
		return
	}
	previousBal, ok := new(big.Int).SetString(strings.TrimSpace(previous.LedgerBalanceAmount), 10)
	if !ok {
		r.saveHotDeltaResult(ctx, hotAddress, asset, previous.ReconciledAt, current.ReconciledAt, previous.LedgerBalanceAmount, current.LedgerBalanceAmount, "0", "0", reconcilemodel.StatusError, false, "invalid previous ledger balance")
		return
	}
	actualDelta := new(big.Int).Sub(currentBal, previousBal)
	expectedDelta, err := r.expectedHotDelta(ctx, hotAddress, asset, previous.ReconciledAt, current.ReconciledAt)
	if err != nil {
		r.saveHotDeltaResult(ctx, hotAddress, asset, previous.ReconciledAt, current.ReconciledAt, previousBal.String(), currentBal.String(), "0", actualDelta.String(), reconcilemodel.StatusError, false, err.Error())
		return
	}
	status := reconcilemodel.StatusMatch
	diff := new(big.Int).Sub(actualDelta, expectedDelta)
	hasMismatch := diff.Sign() != 0
	lastErr := ""
	if hasMismatch {
		status = reconcilemodel.StatusMismatch
		lastErr = "hot wallet ledger delta mismatch"
	}
	r.saveHotDeltaResult(ctx, hotAddress, asset, previous.ReconciledAt, current.ReconciledAt, previousBal.String(), currentBal.String(), expectedDelta.String(), actualDelta.String(), status, hasMismatch, lastErr)
}

func (r *BalanceDeltaReconciler) expectedHotDelta(ctx context.Context, hotAddress string, asset string, from time.Time, to time.Time) (*big.Int, error) {
	out := big.NewInt(0)
	withdraws, err := r.withdrawRepo.ListConfirmedBetween(ctx, r.cfg.Chain, from, to)
	if err != nil {
		return nil, err
	}
	for i := range withdraws {
		rec := withdraws[i]
		if !strings.EqualFold(strings.TrimSpace(rec.FromAddr), strings.TrimSpace(hotAddress)) {
			continue
		}
		transferAsset := strings.TrimSpace(rec.TokenContractAddress)
		if strings.EqualFold(transferAsset, strings.TrimSpace(asset)) {
			out.Sub(out, parseBigOrZero(rec.ActualSpentAmount))
		}
		feeAsset := strings.TrimSpace(rec.NetworkFeeAssetContractAddress)
		if strings.EqualFold(feeAsset, strings.TrimSpace(asset)) {
			out.Sub(out, parseBigOrZero(rec.NetworkFeeAmount))
		}
	}
	sweeps, err := r.sweepRepo.ListConfirmedBetween(ctx, r.cfg.Chain, from, to)
	if err != nil {
		return nil, err
	}
	for i := range sweeps {
		rec := sweeps[i]
		if !strings.EqualFold(strings.TrimSpace(rec.ToAddress), strings.TrimSpace(hotAddress)) {
			continue
		}
		transferAsset := strings.TrimSpace(rec.TransferAssetContractAddress)
		if transferAsset == "" {
			transferAsset = strings.TrimSpace(rec.AssetContractAddress)
		}
		if strings.EqualFold(transferAsset, strings.TrimSpace(asset)) {
			out.Add(out, parseBigOrZero(rec.ActualSpentAmount))
		}
	}
	return out, nil
}

func (r *BalanceDeltaReconciler) saveHotDeltaResult(
	ctx context.Context,
	hotAddress string,
	asset string,
	windowStartedAt time.Time,
	windowEndedAt time.Time,
	startingBalance string,
	endingBalance string,
	expected string,
	actual string,
	status string,
	hasMismatch bool,
	lastErr string,
) {
	diff := new(big.Int).Sub(parseBigOrZero(actual), parseBigOrZero(expected))
	if err := r.reconRepo.UpsertBalanceDeltaReconciliation(ctx, repo.BalanceDeltaReconciliationUpsertInput{
		Scope:                       reconcilemodel.ScopeHot,
		UserID:                      "",
		Chain:                       r.cfg.Chain,
		Address:                     strings.TrimSpace(hotAddress),
		AssetContractAddress:        strings.TrimSpace(asset),
		WindowStartedAt:             windowStartedAt,
		WindowEndedAt:               windowEndedAt,
		StartingLedgerBalanceAmount: strings.TrimSpace(startingBalance),
		EndingLedgerBalanceAmount:   strings.TrimSpace(endingBalance),
		ExpectedDeltaAmount:         strings.TrimSpace(expected),
		ActualDeltaAmount:           strings.TrimSpace(actual),
		DeltaDiffAmount:             diff.String(),
		ReconciliationStatus:        status,
		HasMismatch:                 hasMismatch,
		LastErrorMessage:            strings.TrimSpace(lastErr),
		ReconciledAt:                time.Now(),
	}); err != nil {
		log.Printf("[reconciler-balance-delta] save result failed chain=%s hot=%s asset=%s err=%v", r.cfg.Chain, hotAddress, asset, err)
	}
}

func parseBigOrZero(v string) *big.Int {
	out, ok := new(big.Int).SetString(strings.TrimSpace(v), 10)
	if !ok || out.Sign() < 0 {
		return big.NewInt(0)
	}
	return out
}

func balanceDeltaAssets(tokenContracts []string) []string {
	seen := map[string]struct{}{"": {}}
	out := []string{""}
	for _, c := range tokenContracts {
		v := strings.TrimSpace(c)
		if v == "" {
			continue
		}
		k := strings.ToLower(v)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, v)
	}
	return out
}
