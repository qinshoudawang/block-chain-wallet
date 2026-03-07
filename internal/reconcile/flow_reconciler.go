package reconcile

import (
	"context"
	"errors"
	"log"
	"math/big"
	"strconv"
	"strings"
	"time"

	"wallet-system/internal/infra/redisx"
	depositmodel "wallet-system/internal/storage/model/deposit"
	ledgermodel "wallet-system/internal/storage/model/ledger"
	reconcilemodel "wallet-system/internal/storage/model/reconcile"
	sweepmodel "wallet-system/internal/storage/model/sweep"
	userledgermodel "wallet-system/internal/storage/model/userledger"
	withdrawmodel "wallet-system/internal/storage/model/withdraw"
	"wallet-system/internal/storage/repo"

	"github.com/redis/go-redis/v9"
)

type FlowReconcilerConfig struct {
	Chain        string
	PollInterval time.Duration
	LockTTL      time.Duration
	BatchSize    int
}

type FlowReconciler struct {
	cfg            FlowReconcilerConfig
	redis          *redis.Client
	depositRepo    *repo.DepositRepo
	withdrawRepo   *repo.WithdrawRepo
	sweepRepo      *repo.SweepRepo
	ledgerRepo     *repo.LedgerRepo
	userLedgerRepo *repo.UserLedgerRepo
	reconcileRepo  *repo.ReconcileRepo
}

func NewFlowReconciler(
	cfg FlowReconcilerConfig,
	redisClient *redis.Client,
	depositRepo *repo.DepositRepo,
	withdrawRepo *repo.WithdrawRepo,
	sweepRepo *repo.SweepRepo,
	ledgerRepo *repo.LedgerRepo,
	userLedgerRepo *repo.UserLedgerRepo,
	reconcileRepo *repo.ReconcileRepo,
) *FlowReconciler {
	return &FlowReconciler{
		cfg:            cfg,
		redis:          redisClient,
		depositRepo:    depositRepo,
		withdrawRepo:   withdrawRepo,
		sweepRepo:      sweepRepo,
		ledgerRepo:     ledgerRepo,
		userLedgerRepo: userLedgerRepo,
		reconcileRepo:  reconcileRepo,
	}
}

func (r *FlowReconciler) Run(ctx context.Context) {
	if r == nil || r.depositRepo == nil || r.withdrawRepo == nil || r.sweepRepo == nil || r.ledgerRepo == nil || r.userLedgerRepo == nil || r.reconcileRepo == nil {
		return
	}
	poll := r.cfg.PollInterval
	if poll <= 0 {
		poll = 60 * time.Second
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

func (r *FlowReconciler) tick(ctx context.Context) {
	unlock, ok, err := r.acquireLock(ctx)
	if err != nil {
		log.Printf("[reconciler-flow] acquire lock failed chain=%s err=%v", r.cfg.Chain, err)
		return
	}
	if !ok {
		return
	}
	defer unlock()

	r.reconcileDepositBatch(ctx)
	r.reconcileWithdrawBatch(ctx)
	r.reconcileTopUpBatch(ctx)
	r.reconcileSweepBatch(ctx)
}

func (r *FlowReconciler) reconcileDepositBatch(ctx context.Context) {
	lastID, err := r.loadCursor(ctx, reconcilemodel.FlowSourceDeposit)
	if err != nil {
		log.Printf("[reconciler-flow] load deposit cursor failed chain=%s err=%v", r.cfg.Chain, err)
		return
	}
	items, err := r.depositRepo.ListFinalizedAfterID(ctx, r.cfg.Chain, lastID, r.batchSize())
	if err != nil {
		log.Printf("[reconciler-flow] list finalized deposits failed chain=%s last_id=%d err=%v", r.cfg.Chain, lastID, err)
		return
	}
	if len(items) == 0 {
		return
	}
	maxID := lastID
	for i := range items {
		rec := items[i]
		if err := r.reconcileDepositRecord(ctx, rec); err != nil {
			log.Printf("[reconciler-flow] reconcile deposit failed chain=%s id=%d tx=%s err=%v", rec.Chain, rec.ID, rec.TxHash, err)
		}
		if rec.ID > maxID {
			maxID = rec.ID
		}
	}
	if maxID > lastID {
		if err := r.saveCursor(ctx, reconcilemodel.FlowSourceDeposit, maxID); err != nil {
			log.Printf("[reconciler-flow] save deposit cursor failed chain=%s id=%d err=%v", r.cfg.Chain, maxID, err)
		}
	}
}

func (r *FlowReconciler) reconcileWithdrawBatch(ctx context.Context) {
	lastID, err := r.loadCursor(ctx, reconcilemodel.FlowSourceWithdraw)
	if err != nil {
		log.Printf("[reconciler-flow] load withdraw cursor failed chain=%s err=%v", r.cfg.Chain, err)
		return
	}
	items, err := r.withdrawRepo.ListFinalizedAfterID(ctx, r.cfg.Chain, lastID, r.batchSize())
	if err != nil {
		log.Printf("[reconciler-flow] list finalized withdraws failed chain=%s last_id=%d err=%v", r.cfg.Chain, lastID, err)
		return
	}
	if len(items) == 0 {
		return
	}
	maxID := lastID
	for i := range items {
		rec := items[i]
		if isTopUpWithdraw(rec.RequestID) {
			if rec.ID > maxID {
				maxID = rec.ID
			}
			continue
		}
		if err := r.reconcileWithdrawRecord(ctx, rec); err != nil {
			log.Printf("[reconciler-flow] reconcile withdraw failed chain=%s id=%d withdraw_id=%s err=%v", rec.Chain, rec.ID, rec.WithdrawID, err)
		}
		if rec.ID > maxID {
			maxID = rec.ID
		}
	}
	if maxID > lastID {
		if err := r.saveCursor(ctx, reconcilemodel.FlowSourceWithdraw, maxID); err != nil {
			log.Printf("[reconciler-flow] save withdraw cursor failed chain=%s id=%d err=%v", r.cfg.Chain, maxID, err)
		}
	}
}

func (r *FlowReconciler) reconcileTopUpBatch(ctx context.Context) {
	lastID, err := r.loadCursor(ctx, reconcilemodel.FlowSourceTopUp)
	if err != nil {
		log.Printf("[reconciler-flow] load topup cursor failed chain=%s err=%v", r.cfg.Chain, err)
		return
	}
	items, err := r.withdrawRepo.ListFinalizedAfterID(ctx, r.cfg.Chain, lastID, r.batchSize())
	if err != nil {
		log.Printf("[reconciler-flow] list finalized topups failed chain=%s last_id=%d err=%v", r.cfg.Chain, lastID, err)
		return
	}
	if len(items) == 0 {
		return
	}
	maxID := lastID
	for i := range items {
		rec := items[i]
		if !isTopUpWithdraw(rec.RequestID) {
			if rec.ID > maxID {
				maxID = rec.ID
			}
			continue
		}
		if err := r.reconcileTopUpRecord(ctx, rec); err != nil {
			log.Printf("[reconciler-flow] reconcile topup failed chain=%s id=%d withdraw_id=%s err=%v", rec.Chain, rec.ID, rec.WithdrawID, err)
		}
		if rec.ID > maxID {
			maxID = rec.ID
		}
	}
	if maxID > lastID {
		if err := r.saveCursor(ctx, reconcilemodel.FlowSourceTopUp, maxID); err != nil {
			log.Printf("[reconciler-flow] save topup cursor failed chain=%s id=%d err=%v", r.cfg.Chain, maxID, err)
		}
	}
}

func (r *FlowReconciler) reconcileSweepBatch(ctx context.Context) {
	lastID, err := r.loadCursor(ctx, reconcilemodel.FlowSourceSweep)
	if err != nil {
		log.Printf("[reconciler-flow] load sweep cursor failed chain=%s err=%v", r.cfg.Chain, err)
		return
	}
	items, err := r.sweepRepo.ListFinalizedAfterID(ctx, r.cfg.Chain, lastID, r.batchSize())
	if err != nil {
		log.Printf("[reconciler-flow] list finalized sweeps failed chain=%s last_id=%d err=%v", r.cfg.Chain, lastID, err)
		return
	}
	if len(items) == 0 {
		return
	}
	maxID := lastID
	for i := range items {
		rec := items[i]
		if err := r.reconcileSweepRecord(ctx, rec); err != nil {
			log.Printf("[reconciler-flow] reconcile sweep failed chain=%s id=%d sweep_id=%s err=%v", rec.Chain, rec.ID, rec.SweepID, err)
		}
		if rec.ID > maxID {
			maxID = rec.ID
		}
	}
	if maxID > lastID {
		if err := r.saveCursor(ctx, reconcilemodel.FlowSourceSweep, maxID); err != nil {
			log.Printf("[reconciler-flow] save sweep cursor failed chain=%s id=%d err=%v", r.cfg.Chain, maxID, err)
		}
	}
}

func (r *FlowReconciler) reconcileDepositRecord(ctx context.Context, rec depositmodel.DepositRecord) error {
	bizType, expectedDelta, ok := expectedDepositBiz(rec)
	if !ok {
		return nil
	}
	bizID := depositBizID(rec)

	entry, found, err := r.userLedgerRepo.GetEntryByBiz(ctx, bizType, bizID)
	if err != nil {
		r.saveFlowResult(ctx, reconcilemodel.FlowSourceDeposit, bizType, bizID, rec.UserID, rec.Chain, rec.TokenContractAddress, expectedDelta, "0", reconcilemodel.FlowStatusError, false, err.Error())
		return err
	}
	if !found {
		r.saveFlowResult(ctx, reconcilemodel.FlowSourceDeposit, bizType, bizID, rec.UserID, rec.Chain, rec.TokenContractAddress, expectedDelta, "0", reconcilemodel.FlowStatusMissing, true, "user ledger entry missing")
		return nil
	}
	if strings.TrimSpace(entry.UserID) != strings.TrimSpace(rec.UserID) ||
		!strings.EqualFold(strings.TrimSpace(entry.Chain), strings.TrimSpace(rec.Chain)) ||
		strings.TrimSpace(entry.AssetContractAddress) != strings.TrimSpace(rec.TokenContractAddress) ||
		strings.TrimSpace(entry.DeltaAvailable) != expectedDelta {
		r.saveFlowResult(ctx, reconcilemodel.FlowSourceDeposit, bizType, bizID, rec.UserID, rec.Chain, rec.TokenContractAddress, expectedDelta, strings.TrimSpace(entry.DeltaAvailable), reconcilemodel.FlowStatusMismatch, true, "entry content mismatch")
		return nil
	}
	r.saveFlowResult(ctx, reconcilemodel.FlowSourceDeposit, bizType, bizID, rec.UserID, rec.Chain, rec.TokenContractAddress, expectedDelta, strings.TrimSpace(entry.DeltaAvailable), reconcilemodel.FlowStatusMatch, false, "")
	return nil
}

func (r *FlowReconciler) reconcileWithdrawRecord(ctx context.Context, rec withdrawmodel.WithdrawOrder) error {
	bizType := ledgermodel.LedgerBizTypeWithdraw
	bizID := strings.TrimSpace(rec.WithdrawID)
	if bizID == "" {
		return nil
	}
	freezes, err := r.ledgerRepo.ListFreezesByBiz(ctx, bizType, bizID)
	if err != nil {
		r.saveFlowResult(ctx, reconcilemodel.FlowSourceWithdraw, bizType, bizID, "", rec.Chain, rec.TokenContractAddress, strings.TrimSpace(rec.Amount), "0", reconcilemodel.FlowStatusError, false, err.Error())
		return err
	}
	expectedStatus, ok := expectedWithdrawFreezeStatus(rec.Status)
	if !ok {
		return nil
	}
	if len(freezes) == 0 {
		r.saveFlowResult(ctx, reconcilemodel.FlowSourceWithdraw, bizType, bizID, "", rec.Chain, rec.TokenContractAddress, strings.TrimSpace(rec.Amount), "0", reconcilemodel.FlowStatusMissing, true, "ledger freeze missing")
		return nil
	}
	total := big.NewInt(0)
	for i := range freezes {
		f := freezes[i]
		amt, ok := new(big.Int).SetString(strings.TrimSpace(f.Amount), 10)
		if !ok || amt.Sign() < 0 {
			r.saveFlowResult(ctx, reconcilemodel.FlowSourceWithdraw, bizType, bizID, "", rec.Chain, rec.TokenContractAddress, strings.TrimSpace(rec.Amount), "0", reconcilemodel.FlowStatusError, false, "invalid freeze amount")
			return nil
		}
		total.Add(total, amt)
		if f.Status != expectedStatus {
			r.saveFlowResult(ctx, reconcilemodel.FlowSourceWithdraw, bizType, bizID, "", rec.Chain, rec.TokenContractAddress, strings.TrimSpace(rec.Amount), total.String(), reconcilemodel.FlowStatusMismatch, true, "freeze status mismatch")
			return nil
		}
	}
	r.saveFlowResult(ctx, reconcilemodel.FlowSourceWithdraw, bizType, bizID, "", rec.Chain, rec.TokenContractAddress, strings.TrimSpace(rec.Amount), total.String(), reconcilemodel.FlowStatusMatch, false, "")
	return nil
}

func (r *FlowReconciler) reconcileSweepRecord(ctx context.Context, rec sweepmodel.SweepOrder) error {
	bizType := "SWEEP"
	bizID := strings.TrimSpace(rec.SweepID)
	if bizID == "" {
		return nil
	}
	expected := strings.TrimSpace(rec.Amount)
	actual := strings.TrimSpace(rec.ActualSpentAmount)
	if actual == "" {
		actual = expected
	}
	expectedAsset := strings.TrimSpace(rec.AssetContractAddress)
	settledAsset := strings.TrimSpace(rec.TransferAssetContractAddress)
	if settledAsset == "" {
		settledAsset = expectedAsset
	}
	switch rec.Status {
	case sweepmodel.SweepStatusConfirmed:
		if strings.TrimSpace(rec.TxHash) == "" {
			r.saveFlowResult(ctx, reconcilemodel.FlowSourceSweep, bizType, bizID, rec.UserID, rec.Chain, rec.AssetContractAddress, expected, "0", reconcilemodel.FlowStatusMismatch, true, "confirmed sweep missing tx_hash")
			return nil
		}
		expectedAmt, ok := new(big.Int).SetString(expected, 10)
		if !ok || expectedAmt.Sign() <= 0 {
			r.saveFlowResult(ctx, reconcilemodel.FlowSourceSweep, bizType, bizID, rec.UserID, rec.Chain, rec.AssetContractAddress, expected, actual, reconcilemodel.FlowStatusError, false, "invalid expected sweep amount")
			return nil
		}
		actualAmt, ok := new(big.Int).SetString(actual, 10)
		if !ok || actualAmt.Sign() <= 0 {
			r.saveFlowResult(ctx, reconcilemodel.FlowSourceSweep, bizType, bizID, rec.UserID, rec.Chain, rec.AssetContractAddress, expected, actual, reconcilemodel.FlowStatusError, false, "invalid actual sweep amount")
			return nil
		}
		if expectedAmt.Cmp(actualAmt) != 0 {
			r.saveFlowResult(ctx, reconcilemodel.FlowSourceSweep, bizType, bizID, rec.UserID, rec.Chain, rec.AssetContractAddress, expected, actual, reconcilemodel.FlowStatusMismatch, true, "sweep amount mismatch")
			return nil
		}
		if !equalAsset(expectedAsset, settledAsset) {
			r.saveFlowResult(ctx, reconcilemodel.FlowSourceSweep, bizType, bizID, rec.UserID, rec.Chain, rec.AssetContractAddress, expected, actual, reconcilemodel.FlowStatusMismatch, true, "sweep asset mismatch")
			return nil
		}
		r.saveFlowResult(ctx, reconcilemodel.FlowSourceSweep, bizType, bizID, rec.UserID, rec.Chain, rec.AssetContractAddress, expected, actual, reconcilemodel.FlowStatusMatch, false, "")
	case sweepmodel.SweepStatusFailed:
		if strings.TrimSpace(rec.TxHash) != "" {
			r.saveFlowResult(ctx, reconcilemodel.FlowSourceSweep, bizType, bizID, rec.UserID, rec.Chain, rec.AssetContractAddress, expected, actual, reconcilemodel.FlowStatusMismatch, true, "failed sweep should not keep tx_hash")
			return nil
		}
		r.saveFlowResult(ctx, reconcilemodel.FlowSourceSweep, bizType, bizID, rec.UserID, rec.Chain, rec.AssetContractAddress, expected, actual, reconcilemodel.FlowStatusMatch, false, "")
	}
	return nil
}

func equalAsset(a string, b string) bool {
	return strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b))
}

func (r *FlowReconciler) saveFlowResult(
	ctx context.Context,
	source string,
	bizType string,
	bizID string,
	userID string,
	chain string,
	asset string,
	expectedDelta string,
	actualDelta string,
	status string,
	isMismatch bool,
	lastErr string,
) {
	if r == nil || r.reconcileRepo == nil {
		return
	}
	if err := r.reconcileRepo.UpsertBusinessFlowReconciliation(ctx, repo.FlowReconciliationUpsertInput{
		FlowSource:           source,
		BusinessType:         bizType,
		BusinessID:           bizID,
		UserID:               strings.TrimSpace(userID),
		Chain:                strings.TrimSpace(chain),
		AssetContractAddress: strings.TrimSpace(asset),
		ExpectedChangeAmount: strings.TrimSpace(expectedDelta),
		ActualChangeAmount:   strings.TrimSpace(actualDelta),
		ReconciliationStatus: status,
		HasMismatch:          isMismatch,
		LastErrorMessage:     lastErr,
		ReconciledAt:         time.Now(),
	}); err != nil {
		log.Printf("[reconciler-flow] save result failed chain=%s source=%s biz=%s/%s err=%v", chain, source, bizType, bizID, err)
	}
}

func (r *FlowReconciler) acquireLock(ctx context.Context) (func(), bool, error) {
	if r.redis == nil {
		return func() {}, true, nil
	}
	ttl := r.cfg.LockTTL
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	key := "lock:reconcile:flow:" + strings.ToLower(strings.TrimSpace(r.cfg.Chain))
	unlock, err := redisx.Acquire(ctx, r.redis, key, ttl)
	if err != nil {
		if errors.Is(err, redisx.ErrLockNotAcquired) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return unlock, true, nil
}

func (r *FlowReconciler) loadCursor(ctx context.Context, source string) (uint64, error) {
	if r.redis == nil {
		return 0, nil
	}
	key := "cursor:reconcile:flow:" + strings.ToLower(strings.TrimSpace(r.cfg.Chain)) + ":" + strings.ToLower(strings.TrimSpace(source))
	v, err := r.redis.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, err
	}
	id, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
	if err != nil {
		return 0, nil
	}
	return id, nil
}

func (r *FlowReconciler) saveCursor(ctx context.Context, source string, id uint64) error {
	if r.redis == nil {
		return nil
	}
	key := "cursor:reconcile:flow:" + strings.ToLower(strings.TrimSpace(r.cfg.Chain)) + ":" + strings.ToLower(strings.TrimSpace(source))
	return r.redis.Set(ctx, key, strconv.FormatUint(id, 10), 0).Err()
}

func (r *FlowReconciler) batchSize() int {
	if r.cfg.BatchSize > 0 {
		return r.cfg.BatchSize
	}
	return 500
}

func expectedDepositBiz(rec depositmodel.DepositRecord) (bizType string, deltaAvailable string, ok bool) {
	switch rec.Status {
	case depositmodel.StatusConfirmed:
		return userledgermodel.BizTypeDepositConfirm, strings.TrimSpace(rec.Amount), true
	case depositmodel.StatusReverted:
		amt := strings.TrimSpace(rec.Amount)
		if strings.HasPrefix(amt, "-") {
			return userledgermodel.BizTypeDepositRevert, amt, true
		}
		return userledgermodel.BizTypeDepositRevert, "-" + amt, true
	default:
		return "", "", false
	}
}

func expectedWithdrawFreezeStatus(status withdrawmodel.WithdrawStatus) (ledgermodel.LedgerFreezeStatus, bool) {
	switch status {
	case withdrawmodel.StatusCONFIRMED:
		return ledgermodel.LedgerFreezeConsumed, true
	case withdrawmodel.StatusFAILED:
		return ledgermodel.LedgerFreezeReleased, true
	default:
		return "", false
	}
}

func isTopUpWithdraw(requestID string) bool {
	return strings.HasPrefix(strings.TrimSpace(requestID), "sweeper-topup:")
}

func (r *FlowReconciler) reconcileTopUpRecord(ctx context.Context, rec withdrawmodel.WithdrawOrder) error {
	bizType := "TOPUP"
	bizID := strings.TrimSpace(rec.WithdrawID)
	if bizID == "" {
		return nil
	}
	expected := strings.TrimSpace(rec.Amount)
	actual := strings.TrimSpace(rec.ActualSpentAmount)
	if actual == "" {
		actual = expected
	}
	switch rec.Status {
	case withdrawmodel.StatusCONFIRMED:
		if strings.TrimSpace(rec.TxHash) == "" {
			r.saveFlowResult(ctx, reconcilemodel.FlowSourceTopUp, bizType, bizID, "", rec.Chain, "", expected, "0", reconcilemodel.FlowStatusMismatch, true, "confirmed topup missing tx_hash")
			return nil
		}
		if _, ok := new(big.Int).SetString(expected, 10); !ok {
			r.saveFlowResult(ctx, reconcilemodel.FlowSourceTopUp, bizType, bizID, "", rec.Chain, "", expected, actual, reconcilemodel.FlowStatusError, false, "invalid expected topup amount")
			return nil
		}
		actualAmt, ok := new(big.Int).SetString(actual, 10)
		if !ok || actualAmt.Sign() <= 0 {
			r.saveFlowResult(ctx, reconcilemodel.FlowSourceTopUp, bizType, bizID, "", rec.Chain, "", expected, actual, reconcilemodel.FlowStatusError, false, "invalid actual topup amount")
			return nil
		}
		r.saveFlowResult(ctx, reconcilemodel.FlowSourceTopUp, bizType, bizID, "", rec.Chain, "", expected, actual, reconcilemodel.FlowStatusMatch, false, "")
	case withdrawmodel.StatusFAILED:
		r.saveFlowResult(ctx, reconcilemodel.FlowSourceTopUp, bizType, bizID, "", rec.Chain, "", expected, actual, reconcilemodel.FlowStatusMatch, false, "")
	}
	return nil
}

func depositBizID(rec depositmodel.DepositRecord) string {
	return strings.TrimSpace(rec.Chain) + ":" + strings.TrimSpace(rec.TxHash) + ":" + strconv.FormatUint(uint64(rec.LogIndex), 10)
}
