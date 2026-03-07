package reconcile

import (
	"context"
	"errors"
	"log"
	"math/big"
	"strings"
	"time"

	evmchain "wallet-system/internal/chain/evm"
	"wallet-system/internal/infra/redisx"
	reconcilemodel "wallet-system/internal/storage/model/reconcile"
	"wallet-system/internal/storage/repo"

	"github.com/ethereum/go-ethereum/common"
	"github.com/redis/go-redis/v9"
)

type EVMReconcilerConfig struct {
	Chain          string
	HotAddress     string
	TokenContracts []string
	HotPoll        time.Duration
	UserPoll       time.Duration
	HotLockTTL     time.Duration
	UserLockTTL    time.Duration
	Tolerance      *big.Int
}

type EVMReconciler struct {
	cfg         EVMReconcilerConfig
	evm         *evmchain.Client
	redis       *redis.Client
	addressRepo *repo.AddressRepo
	ledgerRepo  *repo.LedgerRepo
	reconRepo   *repo.ReconcileRepo
}

func NewEVMReconciler(
	cfg EVMReconcilerConfig,
	evm *evmchain.Client,
	redisClient *redis.Client,
	addressRepo *repo.AddressRepo,
	ledgerRepo *repo.LedgerRepo,
	reconRepo *repo.ReconcileRepo,
) *EVMReconciler {
	return &EVMReconciler{
		cfg:         cfg,
		evm:         evm,
		redis:       redisClient,
		addressRepo: addressRepo,
		ledgerRepo:  ledgerRepo,
		reconRepo:   reconRepo,
	}
}

func (r *EVMReconciler) Run(ctx context.Context) {
	if r == nil || r.evm == nil || r.addressRepo == nil || r.ledgerRepo == nil || r.reconRepo == nil {
		return
	}
	hotPoll := r.cfg.HotPoll
	if hotPoll <= 0 {
		hotPoll = 30 * time.Second
	}
	userPoll := r.cfg.UserPoll
	if userPoll <= 0 {
		userPoll = 5 * time.Minute
	}
	hotTicker := time.NewTicker(hotPoll)
	defer hotTicker.Stop()
	userTicker := time.NewTicker(userPoll)
	defer userTicker.Stop()

	r.tickHot(ctx)
	r.tickUser(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-hotTicker.C:
			r.tickHot(ctx)
		case <-userTicker.C:
			r.tickUser(ctx)
		}
	}
}

func (r *EVMReconciler) tickHot(ctx context.Context) {
	unlock, ok, err := r.acquireLock(ctx, "hot", r.cfg.HotLockTTL)
	if err != nil {
		log.Printf("[reconciler-evm] acquire hot lock failed chain=%s err=%v", r.cfg.Chain, err)
		return
	}
	if !ok {
		return
	}
	defer unlock()

	assets := reconcileAssets(r.cfg.TokenContracts)
	hot := strings.TrimSpace(r.cfg.HotAddress)
	if hot == "" {
		return
	}
	for _, asset := range assets {
		r.reconcileOne(ctx, "", hot, asset)
	}
}

func (r *EVMReconciler) tickUser(ctx context.Context) {
	unlock, ok, err := r.acquireLock(ctx, "user", r.cfg.UserLockTTL)
	if err != nil {
		log.Printf("[reconciler-evm] acquire user lock failed chain=%s err=%v", r.cfg.Chain, err)
		return
	}
	if !ok {
		return
	}
	defer unlock()

	addresses, err := r.addressRepo.ListByChain(ctx, strings.ToLower(strings.TrimSpace(r.cfg.Chain)))
	if err != nil {
		log.Printf("[reconciler-evm] list addresses failed chain=%s err=%v", r.cfg.Chain, err)
		return
	}
	assets := reconcileAssets(r.cfg.TokenContracts)
	for i := range addresses {
		addr := addresses[i]
		for _, asset := range assets {
			r.reconcileOne(ctx, addr.UserID, addr.Address, asset)
		}
	}
}

func (r *EVMReconciler) acquireLock(ctx context.Context, scope string, ttl time.Duration) (func(), bool, error) {
	if r.redis == nil {
		return func() {}, true, nil
	}
	if ttl <= 0 {
		ttl = 90 * time.Second
	}
	key := "lock:reconcile:evm:" + strings.ToLower(strings.TrimSpace(r.cfg.Chain)) + ":" + strings.ToLower(strings.TrimSpace(scope))
	unlock, err := redisx.Acquire(ctx, r.redis, key, ttl)
	if err != nil {
		if errors.Is(err, redisx.ErrLockNotAcquired) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return unlock, true, nil
}

func (r *EVMReconciler) reconcileOne(ctx context.Context, userID string, address string, asset string) {
	scope := resolveScope(strings.TrimSpace(userID))
	asset = strings.TrimSpace(asset)
	acct, ok, err := r.ledgerRepo.GetByChainAddressAsset(ctx, r.cfg.Chain, address, asset)
	if err != nil {
		log.Printf("[reconciler-evm] load ledger account failed chain=%s address=%s asset=%s err=%v", r.cfg.Chain, address, asset, err)
		r.saveResult(ctx, scope, userID, address, asset, "0", "0", "0", reconcilemodel.StatusError, false, err.Error())
		return
	}
	ledgerTotal := big.NewInt(0)
	if ok {
		avail, okAvail := new(big.Int).SetString(strings.TrimSpace(acct.AvailableAmount), 10)
		frozen, okFrozen := new(big.Int).SetString(strings.TrimSpace(acct.FrozenAmount), 10)
		if !okAvail || !okFrozen || avail.Sign() < 0 || frozen.Sign() < 0 {
			log.Printf("[reconciler-evm] invalid ledger amounts chain=%s address=%s asset=%s avail=%s frozen=%s",
				r.cfg.Chain, address, asset, acct.AvailableAmount, acct.FrozenAmount)
			r.saveResult(ctx, scope, userID, address, asset, "0", "0", "0", reconcilemodel.StatusError, false, "invalid ledger amounts")
			return
		}
		ledgerTotal = ledgerTotal.Add(avail, frozen)
	}

	chainBal, err := r.fetchOnchainBalance(ctx, address, asset)
	if err != nil {
		log.Printf("[reconciler-evm] fetch onchain balance failed chain=%s address=%s asset=%s err=%v", r.cfg.Chain, address, asset, err)
		r.saveResult(ctx, scope, userID, address, asset, "0", ledgerTotal.String(), "0", reconcilemodel.StatusError, false, err.Error())
		return
	}
	diff := new(big.Int).Sub(chainBal, ledgerTotal)
	tol := r.cfg.Tolerance
	if tol == nil || tol.Sign() < 0 {
		tol = big.NewInt(0)
	}
	isMismatch := abs(diff).Cmp(tol) > 0
	status := reconcilemodel.StatusMatch
	if isMismatch {
		status = reconcilemodel.StatusMismatch
	}
	r.saveResult(ctx, scope, userID, address, asset, chainBal.String(), ledgerTotal.String(), diff.String(), status, isMismatch, "")
	if !isMismatch {
		return
	}
	log.Printf("[reconciler-evm] mismatch chain=%s user=%s address=%s asset=%s onchain=%s ledger_total=%s diff=%s",
		r.cfg.Chain, strings.TrimSpace(userID), address, asset, chainBal.String(), ledgerTotal.String(), diff.String())
}

func (r *EVMReconciler) saveResult(
	ctx context.Context,
	scope string,
	userID string,
	address string,
	asset string,
	onchain string,
	ledger string,
	diff string,
	status string,
	isMismatch bool,
	lastError string,
) {
	if r == nil || r.reconRepo == nil {
		return
	}
	err := r.reconRepo.UpsertOnchainReconciliation(ctx, repo.OnchainReconciliationUpsertInput{
		Scope:                scope,
		UserID:               strings.TrimSpace(userID),
		Chain:                r.cfg.Chain,
		Address:              strings.TrimSpace(address),
		AssetContractAddress: strings.TrimSpace(asset),
		OnchainBalanceAmount: strings.TrimSpace(onchain),
		LedgerBalanceAmount:  strings.TrimSpace(ledger),
		BalanceDiffAmount:    strings.TrimSpace(diff),
		ReconciliationStatus: status,
		HasMismatch:          isMismatch,
		LastErrorMessage:     strings.TrimSpace(lastError),
		ReconciledAt:         time.Now(),
	})
	if err != nil {
		log.Printf("[reconciler-evm] save result failed chain=%s scope=%s user=%s address=%s asset=%s err=%v",
			r.cfg.Chain, scope, strings.TrimSpace(userID), strings.TrimSpace(address), strings.TrimSpace(asset), err)
	}
}

func (r *EVMReconciler) fetchOnchainBalance(ctx context.Context, address string, asset string) (*big.Int, error) {
	owner := common.HexToAddress(strings.TrimSpace(address))
	if strings.TrimSpace(asset) == "" {
		return r.evm.BalanceAt(ctx, owner)
	}
	return r.evm.TokenBalanceAt(ctx, common.HexToAddress(strings.TrimSpace(asset)), owner)
}

func reconcileAssets(tokenContracts []string) []string {
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

func abs(v *big.Int) *big.Int {
	if v == nil {
		return big.NewInt(0)
	}
	return new(big.Int).Abs(v)
}

func resolveScope(userID string) string {
	if strings.TrimSpace(userID) == "" {
		return reconcilemodel.ScopeHot
	}
	return reconcilemodel.ScopeUser
}
