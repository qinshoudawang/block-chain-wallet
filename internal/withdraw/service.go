package withdraw

import (
	"context"
	"encoding/base64"
	"encoding/hex"
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
	"wallet-system/internal/storage/model"
	storagerepo "wallet-system/internal/storage/repo"
	"wallet-system/internal/withdraw/chainclient"
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

func (s *Service) MatchRequestChain(chain string) (string, error) {
	reqSpec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return "", err
	}
	return reqSpec.CanonicalChain, nil
}

type WithdrawInput struct {
	Chain  string
	To     string
	Amount string // atomic-unit decimal
}

func (s *Service) CreateAndSignWithdraw(ctx context.Context, in WithdrawInput) (task *broadcaster.BroadcastTask, err error) {
	chain, profile, chainClient, err := s.resolveChainContext(in.Chain)
	if err != nil {
		return nil, err
	}
	fromAddr := profile.FromAddress
	log.Printf("[withdraw-service] create/sign start chain=%s from=%s to=%s amount=%s", chain, fromAddr, in.To, in.Amount)

	// 1) Validate chain-specific input and normalize address/amount.
	toAddr, amt, err := chainClient.ValidateWithdrawInput(chain, in.To, in.Amount)
	if err != nil {
		log.Printf("[withdraw-service] validate input failed chain=%s from=%s to=%s amount=%s err=%v", chain, fromAddr, in.To, in.Amount, err)
		return nil, err
	}

	withdrawID := uuid.NewString()
	requestID := uuid.NewString()
	rt := s.chainRuntime(chain, profile, withdrawID)

	// 2) Freeze ledger balance first and run risk approval before signing.
	if err := s.freezeAndApprove(ctx, chain, profile, withdrawID, requestID, toAddr, in.Amount, amt); err != nil {
		log.Printf("[withdraw-service] precheck failed chain=%s withdraw_id=%s request_id=%s err=%v", chain, withdrawID, requestID, err)
		return nil, err
	}
	utxoReserved := false
	defer func() {
		if err == nil {
			return
		}
		if utxoReserved && s.deps.UTXOReserve != nil {
			if rerr := s.deps.UTXOReserve.ReleaseByWithdrawID(context.Background(), withdrawID); rerr != nil {
				log.Printf("[withdraw-service] release utxo reservation failed chain=%s withdraw_id=%s err=%v", chain, withdrawID, rerr)
			}
		}
		if rerr := s.releaseFrozenOnError(context.Background(), withdrawID); rerr != nil {
			log.Printf("[withdraw-service] release freeze failed chain=%s withdraw_id=%s err=%v", chain, withdrawID, rerr)
		}
	}()

	// 3) Allocate sequence for chains that need ordered issuance.
	sequenceAlloc, err := s.allocateSequence(ctx, &rt, chainClient)
	if err != nil {
		return nil, err
	}
	if sequenceAlloc.Unlock != nil {
		defer sequenceAlloc.Unlock()
	}
	logSequence := sequenceForLog(sequenceAlloc.Value)

	// 4) Build chain-specific unsigned transaction payload.
	var unsignedTx []byte
	unsignedTx, utxoReserved, err = s.prepareUnsignedTx(
		ctx,
		chainClient,
		rt,
		toAddr,
		amt,
		sequenceAlloc.Value,
	)
	if err != nil {
		log.Printf("[withdraw-service] build unsigned tx failed chain=%s from=%s to=%s sequence=%s err=%v", chain, fromAddr, toAddr, logSequence, err)
		return nil, err
	}

	// 5) Request signer service to sign the payload.
	sresp, err := s.signWithdraw(ctx, chain, profile, withdrawID, requestID, toAddr, in.Amount, unsignedTx)
	if err != nil {
		log.Printf("[withdraw-service] sign withdraw failed chain=%s withdraw_id=%s request_id=%s sequence=%s err=%v", chain, withdrawID, requestID, logSequence, err)
		return nil, err
	}
	log.Printf("[withdraw-service] sign withdraw success chain=%s withdraw_id=%s request_id=%s sequence=%s signed_size=%d", chain, withdrawID, requestID, logSequence, len(sresp.SignedTx))

	// 6) Persist signed order and return task for broadcaster enqueue.
	signedPayload, signedPayloadEncoding, err := s.insertSignedOrder(
		ctx,
		chain,
		profile,
		withdrawID,
		requestID,
		toAddr,
		in.Amount,
		sequenceAlloc.Value,
		sresp.SignedTx,
		"",
	)
	if err != nil {
		log.Printf("[withdraw-service] insert signed order failed chain=%s withdraw_id=%s request_id=%s sequence=%s err=%v", chain, withdrawID, requestID, logSequence, err)
		return nil, err
	}
	log.Printf("[withdraw-service] insert signed order success chain=%s withdraw_id=%s request_id=%s sequence=%s", chain, withdrawID, requestID, logSequence)

	return &broadcaster.BroadcastTask{
		Version:               1,
		WithdrawID:            withdrawID,
		RequestID:             requestID,
		Chain:                 chain,
		From:                  fromAddr,
		To:                    toAddr,
		Amount:                in.Amount,
		Sequence:              sequenceAlloc.Value,
		SignedPayload:         signedPayload,
		SignedPayloadEncoding: signedPayloadEncoding,
		ChainMetaJSON:         "",
		CreatedAt:             time.Now().Unix(),
		Attempt:               0,
	}, nil
}

func (s *Service) freezeAndApprove(
	ctx context.Context,
	chain string,
	profile ChainProfile,
	withdrawID string,
	requestID string,
	toAddr string,
	amountStr string,
	amount *big.Int,
) error {
	freezeAmount := new(big.Int).Set(amount)
	if profile.FreezeReserve != nil && profile.FreezeReserve.Sign() > 0 {
		freezeAmount.Add(freezeAmount, profile.FreezeReserve)
	}
	if _, err := s.deps.Ledger.FreezeWithdraw(ctx, chain, profile.FromAddress, withdrawID, freezeAmount); err != nil {
		return err
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

func (s *Service) insertSignedOrder(
	ctx context.Context,
	chain string,
	profile ChainProfile,
	withdrawID string,
	requestID string,
	toAddr string,
	amount string,
	sequence uint64,
	signedTx []byte,
	chainMetaJSON string,
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

	if err := s.deps.Withdraw.InsertSigned(ctx, &model.WithdrawOrder{
		WithdrawID:            withdrawID,
		RequestID:             requestID,
		Chain:                 chain,
		FromAddr:              profile.FromAddress,
		ToAddr:                toAddr,
		Amount:                amount,
		Sequence:              sequence,
		SignedPayload:         signedPayload,
		SignedPayloadEncoding: signedPayloadEncoding,
		ChainMetaJSON:         chainMetaJSON,
		TxHash:                "",
	}); err != nil {
		return "", "", err
	}
	return signedPayload, signedPayloadEncoding, nil
}

func (s *Service) chainRuntime(chain string, profile ChainProfile, withdrawID string) chainclient.Runtime {
	return chainclient.Runtime{
		Chain:            chain,
		ChainID:          profile.ChainID,
		FromAddress:      profile.FromAddress,
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

func (s *Service) signWithdraw(
	ctx context.Context,
	chain string,
	profile ChainProfile,
	withdrawID string,
	requestID string,
	toAddr string,
	amount string,
	unsignedTx []byte,
) (*signpb.SignResponse, error) {
	log.Printf("[withdraw-service] signer rpc start chain=%s withdraw_id=%s request_id=%s from=%s to=%s amount=%s unsigned_size=%d", chain, withdrawID, requestID, profile.FromAddress, toAddr, amount, len(unsignedTx))

	authToken := auth.MakeToken(s.authSecret, auth.TxPayload{
		WithdrawID: withdrawID,
		RequestID:  requestID,
		Chain:      chain,
		From:       profile.FromAddress,
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
		FromAddress: profile.FromAddress,
		ToAddress:   toAddr,
		Amount:      amount,
		UnsignedTx:  unsignedTx,
		AuthToken:   authToken,
		Caller:      "withdraw-api",
	})
	if err != nil {
		log.Printf("[withdraw-service] signer rpc failed chain=%s withdraw_id=%s request_id=%s err=%v", chain, withdrawID, requestID, err)
		return nil, err
	}
	log.Printf("[withdraw-service] signer rpc success chain=%s withdraw_id=%s request_id=%s signed_size=%d", chain, withdrawID, requestID, len(resp.SignedTx))
	return resp, nil
}

func sequenceForLog(v uint64) string {
	return big.NewInt(0).SetUint64(v).String()
}

func lockKeyForSequence(chain string, fromAddr string) string {
	return "lock:sequence:" + chain + ":" + strings.ToLower(strings.TrimSpace(fromAddr))
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
