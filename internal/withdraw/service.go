package withdraw

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"math/big"
	"strings"
	"time"

	auth "wallet-system/internal/auth"
	"wallet-system/internal/broadcaster"
	"wallet-system/internal/config"
	"wallet-system/internal/helpers"
	"wallet-system/internal/infra/kafka"
	"wallet-system/internal/infra/redisx"
	"wallet-system/internal/sequence/utxoreserve"
	withdrawmodel "wallet-system/internal/storage/model/withdraw"
	storagerepo "wallet-system/internal/storage/repo"
	"wallet-system/internal/withdraw/chainclient"
	flowpkg "wallet-system/internal/withdraw/flow"
	signpb "wallet-system/proto/signer"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Deps struct {
	Redis       *redis.Client
	ChainClient *chainclient.Registry
	Signer      signpb.SignerServiceClient
	Ledger      *storagerepo.LedgerRepo
	Withdraw    *storagerepo.WithdrawRepo
	UTXOReserve *utxoreserve.Manager
	Risk        RiskApprover
}

type ChainProfile = config.ChainProfile

type RiskApproveInput struct {
	WithdrawID string
	RequestID  string
	Chain      string
	From       string
	To         string
	Amount     string
}

type RiskApprover interface {
	ApproveWithdraw(ctx context.Context, in RiskApproveInput) error
}

type Service struct {
	profiles   map[string]ChainProfile
	authSecret []byte
	deps       Deps
	Producer   *kafka.Producer
}

type SequenceAllocation struct {
	Value  uint64
	Unlock func()
}

type WithdrawInput struct {
	Chain  string
	To     string
	Amount string // atomic-unit decimal
	Token  string // optional token contract (EVM)
}

type SubmitRBFResult struct {
	WithdrawID            string
	RequestID             string
	Status                string
	OldFeeRateSatPerVbyte int64
	NewFeeRateSatPerVbyte int64
	OldFeeSat             int64
	NewFeeSat             int64
}

// Lifecycle
func NewService(profiles map[string]ChainProfile, authSecret []byte, deps Deps, producer *kafka.Producer) *Service {
	if deps.Ledger == nil {
		panic("ledger repo is required")
	}
	if deps.Withdraw == nil {
		panic("withdraw repo is required")
	}
	if deps.ChainClient == nil {
		panic("chain client registry is required")
	}

	if len(profiles) == 0 {
		panic("at least one chain profile is required")
	}
	if len(authSecret) == 0 {
		panic("auth secret is required")
	}
	normalizedProfiles := make(map[string]ChainProfile, len(profiles))
	for chain, p := range profiles {
		spec, err := helpers.ResolveChainSpec(chain)
		if err != nil {
			panic(err)
		}
		if p.FromAddress == "" {
			panic("from address is required for chain: " + spec.CanonicalChain)
		}
		canonical := spec.CanonicalChain
		if _, exists := normalizedProfiles[canonical]; exists {
			panic("duplicate chain profile: " + canonical)
		}
		normalizedProfiles[canonical] = p
	}

	return &Service{
		profiles:   normalizedProfiles,
		authSecret: authSecret,
		deps:       deps,
		Producer:   producer,
	}
}

// Public API
func (s *Service) MatchRequestChain(chain string) (string, error) {
	reqSpec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return "", err
	}
	return reqSpec.CanonicalChain, nil
}

func (s *Service) CreateAndSignWithdraw(ctx context.Context, in WithdrawInput) (task *broadcaster.BroadcastTask, err error) {
	_, task, err = flowpkg.Run(ctx, flowpkg.CreateTemplate{
		Input:              flowpkg.CreateInput{Chain: in.Chain, To: in.To, Amount: in.Amount, Token: in.Token},
		CallerName:         "withdraw-api",
		SkipPrecheck:       false,
		InitFn:             s.initCreateFlowState,
		BeforeSignFn:       s.beforeSignCreateFlow,
		BuildTransactionFn: s.buildUnsignedCreateFlow,
		PersistFn:          s.persistSignedCreateFlow,
		BroadcastFn:        s.broadcastFlowTask,
		OnErrorFn:          s.onErrorCreateFlow,
	}, s.buildFlowRunnerDeps())
	return task, err
}

// CreateAndSignSystemWithdraw creates a withdraw order without ledger freeze/risk steps.
// It is used by internal system flows like token-gas topup.
func (s *Service) CreateAndSignSystemWithdraw(ctx context.Context, in WithdrawInput, caller string) (task *broadcaster.BroadcastTask, err error) {
	_, task, err = flowpkg.Run(ctx, flowpkg.CreateTemplate{
		Input:              flowpkg.CreateInput{Chain: in.Chain, To: in.To, Amount: in.Amount, Token: in.Token},
		CallerName:         "withdraw-system",
		SkipPrecheck:       true,
		InitFn:             s.initCreateFlowState,
		BeforeSignFn:       s.beforeSignCreateFlow,
		BuildTransactionFn: s.buildUnsignedCreateFlow,
		PersistFn:          s.persistSignedCreateFlow,
		BroadcastFn:        s.broadcastFlowTask,
		OnErrorFn:          s.onErrorCreateFlow,
	}, s.buildFlowRunnerDeps())
	return task, err
}

func (s *Service) CreateAndSignRBFWithdraw(ctx context.Context, withdrawID string, oldTxHash string) (*SubmitRBFResult, error) {
	if s == nil || s.deps.Withdraw == nil || s.deps.ChainClient == nil || s.deps.Signer == nil || len(s.authSecret) == 0 {
		return nil, errors.New("withdraw service not configured for rbf")
	}
	st, task, err := flowpkg.Run(ctx, flowpkg.RBFTemplate{
		WithdrawID:         withdrawID,
		OldTxHash:          oldTxHash,
		ValidateFn:         s.initRBFFlowState,
		BuildTransactionFn: s.buildUnsignedRBFFlow,
		PersistFn:          s.persistSignedRBFFlow,
		BroadcastFn:        s.broadcastFlowTask,
	}, s.buildFlowRunnerDeps())
	if err != nil {
		return nil, err
	}
	if task == nil {
		return nil, errors.New("rbf task is nil")
	}
	if st == nil || st.RBFBuild == nil {
		return nil, errors.New("rbf build result is missing")
	}

	return &SubmitRBFResult{
		WithdrawID:            task.WithdrawID,
		RequestID:             task.RequestID,
		Status:                "RBF_ENQUEUED",
		OldFeeRateSatPerVbyte: st.RBFBuild.OldFeeRate,
		NewFeeRateSatPerVbyte: st.RBFBuild.NewFeeRate,
		OldFeeSat:             st.RBFBuild.OldFee,
		NewFeeSat:             st.RBFBuild.NewFee,
	}, nil
}

func (s *Service) EnqueueBroadcastTask(ctx context.Context, canonicalChain string, task *broadcaster.BroadcastTask) error {
	if s == nil || s.Producer == nil {
		return errors.New("kafka producer not configured")
	}
	if task == nil {
		return errors.New("broadcast task is nil")
	}
	key := canonicalChain + ":" + task.From
	taskBytes, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return s.Producer.Publish(ctx, key, taskBytes)
}

// Flow assembly
func (s *Service) buildFlowRunnerDeps() flowpkg.RunnerDeps {
	return flowpkg.RunnerDeps{
		SignFn: func(ctx context.Context, st *flowpkg.State, caller string) ([]byte, error) {
			resp, err := s.signWithdraw(
				ctx,
				st.Chain,
				st.FromAddr,
				st.WithdrawID,
				st.RequestID,
				st.ToAddr,
				st.AmountText,
				st.UnsignedTx,
				caller,
			)
			if err != nil {
				return nil, err
			}
			return resp.SignedTx, nil
		},
		ReleaseUTXOFn: func(ctx context.Context, withdrawID string) error {
			if s.deps.UTXOReserve == nil {
				return nil
			}
			return s.deps.UTXOReserve.ReleaseByWithdrawID(ctx, withdrawID)
		},
	}
}

func (s *Service) broadcastFlowTask(ctx context.Context, st *flowpkg.State, task *broadcaster.BroadcastTask) error {
	_ = st
	if task == nil {
		return errors.New("broadcast task is nil")
	}
	return s.EnqueueBroadcastTask(ctx, task.Chain, task)
}

// Domain helpers
func (s *Service) validateWithdrawInput(
	chain string,
	in WithdrawInput,
	chainClient chainclient.Client,
) (toAddr string, amount *big.Int, tokenContract string, err error) {
	toAddr, amount, err = chainClient.ValidateWithdrawInput(chain, in.To, in.Amount)
	if err != nil {
		return "", nil, "", err
	}
	tokenContract, err = chainclient.NormalizeWithdrawTokenContract(chain, in.Token)
	if err != nil {
		return "", nil, "", err
	}
	return toAddr, amount, tokenContract, nil
}

func (s *Service) freezeAndApprove(
	ctx context.Context,
	chain string,
	profile ChainProfile,
	withdrawID string,
	requestID string,
	tokenContractAddress string,
	toAddr string,
	amountStr string,
	amount *big.Int,
) error {
	plans, err := buildFreezePlans(tokenContractAddress, amount, profile.FreezeReserve)
	if err != nil {
		return err
	}
	for _, p := range plans {
		if _, err := s.deps.Ledger.FreezeWithdraw(ctx, chain, profile.FromAddress, p.assetContractAddress, withdrawID, p.amount); err != nil {
			return err
		}
	}
	if s.deps.Risk == nil {
		return nil
	}
	return s.deps.Risk.ApproveWithdraw(ctx, RiskApproveInput{
		WithdrawID: withdrawID,
		RequestID:  requestID,
		Chain:      chain,
		From:       profile.FromAddress,
		To:         toAddr,
		Amount:     amountStr,
	})
}

func (s *Service) releaseFrozenOnError(ctx context.Context, withdrawID string) error {
	return s.deps.Ledger.ReleaseWithdrawFreeze(ctx, withdrawID)
}

// Persistence and encoding
func (s *Service) insertSignedOrder(
	ctx context.Context,
	chain string,
	fromAddr string,
	withdrawID string,
	requestID string,
	toAddr string,
	amount string,
	sequence uint64,
	signedTx []byte,
	tokenContractAddress string,
) (signedPayload string, signedPayloadEncoding string, err error) {
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return "", "", err
	}
	switch spec.Family {
	case helpers.FamilyEVM, helpers.FamilyBTC:
		signedPayload = hex.EncodeToString(signedTx)
		signedPayloadEncoding = broadcaster.SignedPayloadEncodingHex
	case helpers.FamilySOL:
		signedPayload = base64.StdEncoding.EncodeToString(signedTx)
		signedPayloadEncoding = broadcaster.SignedPayloadEncodingBase64
	default:
		return "", "", errors.New("unsupported chain family for signed payload encoding")
	}

	if err := s.deps.Withdraw.InsertSigned(ctx, &withdrawmodel.WithdrawOrder{
		WithdrawID:            withdrawID,
		RequestID:             requestID,
		Chain:                 chain,
		FromAddr:              fromAddr,
		ToAddr:                toAddr,
		Amount:                amount,
		Sequence:              sequence,
		SignedPayload:         signedPayload,
		SignedPayloadEncoding: signedPayloadEncoding,
		TokenContractAddress:  tokenContractAddress,
		TxHash:                "",
	}); err != nil {
		return "", "", err
	}
	return signedPayload, signedPayloadEncoding, nil
}

func expectedSignedPayloadEncodingForFamily(family string) (string, error) {
	switch family {
	case helpers.FamilyBTC, helpers.FamilyEVM:
		return broadcaster.SignedPayloadEncodingHex, nil
	case helpers.FamilySOL:
		return broadcaster.SignedPayloadEncodingBase64, nil
	default:
		return "", errors.New("unsupported chain family for signed payload encoding")
	}
}

func encodeSignedPayloadByFamily(family string, signedTx []byte) (string, string, error) {
	switch family {
	case helpers.FamilyBTC, helpers.FamilyEVM:
		return hex.EncodeToString(signedTx), broadcaster.SignedPayloadEncodingHex, nil
	case helpers.FamilySOL:
		return base64.StdEncoding.EncodeToString(signedTx), broadcaster.SignedPayloadEncodingBase64, nil
	default:
		return "", "", errors.New("unsupported chain family for signed payload encoding")
	}
}

// Chain runtime and sequencing
func (s *Service) chainRuntime(chain string, profile ChainProfile, withdrawID string, tokenContract string) chainclient.Runtime {
	return chainclient.Runtime{
		Chain:            chain,
		ChainID:          profile.ChainID,
		FromAddress:      profile.FromAddress,
		TokenContract:    tokenContract,
		WithdrawID:       withdrawID,
		MinConf:          profile.MinConf,
		FeeTarget:        profile.FeeTarget,
		FeeRate:          profile.FeeRate,
		ExcludedUTXOKeys: nil,
		ReserveUTXO:      nil,
		UTXOReserve:      s.deps.UTXOReserve,
	}
}

func (s *Service) prepareUnsignedTx(
	ctx context.Context,
	chainClient chainclient.Client,
	rt chainclient.Runtime,
	toAddr string,
	amount *big.Int,
	sequence uint64,
) ([]byte, bool, error) {
	unsignedTx, err := chainClient.BuildUnsignedWithdrawTx(ctx, rt, toAddr, amount, sequence)
	if err != nil {
		return nil, false, err
	}
	if rt.ReserveUTXO != nil {
		return unsignedTx, true, nil
	}
	return unsignedTx, false, nil
}

func (s *Service) allocateSequence(
	ctx context.Context,
	rt *chainclient.Runtime,
	chainClient chainclient.Client,
) (SequenceAllocation, error) {
	if rt == nil {
		return SequenceAllocation{}, errors.New("runtime is required")
	}
	fromAddr := rt.FromAddress
	return s.withSequenceLock(ctx, rt.Chain, fromAddr, func() (uint64, error) {
		return chainClient.AllocateSequence(ctx, s.deps.Redis, rt, func(ctx context.Context) (uint64, error) {
			return s.deps.Withdraw.NextSequenceFloor(ctx, rt.Chain, fromAddr)
		})
	})
}

func (s *Service) withSequenceLock(
	ctx context.Context,
	chain string,
	fromAddr string,
	allocate func() (uint64, error),
) (SequenceAllocation, error) {
	unlock, err := redisx.Acquire(ctx, s.deps.Redis, lockKeyForSequence(chain, fromAddr), 8*time.Second)
	if err != nil {
		log.Printf("[withdraw-service] sequence lock busy chain=%s from=%s err=%v", chain, fromAddr, err)
		return SequenceAllocation{}, errors.New("busy")
	}

	sequence, err := allocate()
	if err != nil {
		log.Printf("[withdraw-service] sequence allocate failed chain=%s from=%s err=%v", chain, fromAddr, err)
		unlock()
		return SequenceAllocation{}, errors.New("sequence allocate failed")
	}
	return SequenceAllocation{
		Value:  sequence,
		Unlock: unlock,
	}, nil
}

// Signer and chain context
func (s *Service) signWithdraw(
	ctx context.Context,
	chain string,
	fromAddr string,
	withdrawID string,
	requestID string,
	toAddr string,
	amount string,
	unsignedTx []byte,
	caller string,
) (*signpb.SignResponse, error) {
	log.Printf("[withdraw-service] signer rpc start chain=%s withdraw_id=%s request_id=%s from=%s to=%s amount=%s unsigned_size=%d", chain, withdrawID, requestID, fromAddr, toAddr, amount, len(unsignedTx))

	authToken := auth.MakeToken(s.authSecret, auth.TxPayload{
		WithdrawID: withdrawID,
		RequestID:  requestID,
		Chain:      chain,
		From:       fromAddr,
		To:         toAddr,
		Amount:     amount,
		UnsignedTx: unsignedTx,
	})

	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := s.deps.Signer.SignTransaction(sctx, &signpb.SignRequest{
		RequestId:   requestID,
		WithdrawId:  withdrawID,
		Chain:       chain,
		FromAddress: fromAddr,
		ToAddress:   toAddr,
		Amount:      amount,
		UnsignedTx:  unsignedTx,
		AuthToken:   authToken,
		Caller:      caller,
	})
	if err != nil {
		log.Printf("[withdraw-service] signer rpc failed chain=%s withdraw_id=%s request_id=%s err=%v", chain, withdrawID, requestID, err)
		return nil, err
	}
	log.Printf("[withdraw-service] signer rpc success chain=%s withdraw_id=%s request_id=%s signed_size=%d", chain, withdrawID, requestID, len(resp.SignedTx))
	return resp, nil
}

func (s *Service) resolveChainContext(chain string) (string, ChainProfile, chainclient.Client, error) {
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return "", ChainProfile{}, nil, err
	}
	profile, ok := s.profiles[spec.CanonicalChain]
	if !ok {
		return "", ChainProfile{}, nil, errors.New("chain profile not configured")
	}
	cli, err := s.deps.ChainClient.ResolveByChain(spec.CanonicalChain)
	if err != nil {
		return "", ChainProfile{}, nil, err
	}
	return spec.CanonicalChain, profile, cli, nil
}

// Small helpers
func buildFreezePlans(tokenContractAddress string, transferAmount *big.Int, feeReserve *big.Int) ([]freezePlan, error) {
	tokenContractAddress = strings.TrimSpace(tokenContractAddress)
	if tokenContractAddress == "" {
		freezeAmount := new(big.Int).Set(transferAmount)
		if feeReserve != nil && feeReserve.Sign() > 0 {
			freezeAmount.Add(freezeAmount, feeReserve)
		}
		return []freezePlan{{assetContractAddress: "", amount: freezeAmount}}, nil
	}
	if feeReserve == nil || feeReserve.Sign() <= 0 {
		return nil, errors.New("freeze reserve is required for token withdraw")
	}
	return []freezePlan{
		{assetContractAddress: tokenContractAddress, amount: new(big.Int).Set(transferAmount)},
		{assetContractAddress: "", amount: new(big.Int).Set(feeReserve)},
	}, nil
}

type freezePlan struct {
	assetContractAddress string
	amount               *big.Int
}

func sequenceForLog(v uint64) string {
	return big.NewInt(0).SetUint64(v).String()
}

func buildRequestID(caller string) string {
	caller = strings.TrimSpace(caller)
	if caller == "" {
		return uuid.NewString()
	}
	return caller + ":" + uuid.NewString()
}

func lockKeyForSequence(chain string, fromAddr string) string {
	return "lock:sequence:" + chain + ":" + strings.ToLower(strings.TrimSpace(fromAddr))
}
