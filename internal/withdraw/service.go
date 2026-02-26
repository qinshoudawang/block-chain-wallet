package withdraw

import (
	"context"
	"encoding/hex"
	"errors"
	"log"
	"math/big"
	"time"

	"wallet-system/internal/helpers"
	"wallet-system/internal/infra/kafka"
	"wallet-system/internal/infra/redisx"
	"wallet-system/internal/storage/model"
	storagerepo "wallet-system/internal/storage/repo"
	"wallet-system/internal/withdraw/chainclient"
	signpb "wallet-system/proto/signer"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Deps struct {
	Redis       *redis.Client
	ChainClient *chainclient.Registry
	Signer      signpb.SignerServiceClient
	Ledger      *storagerepo.LedgerRepo
	Withdraw    *storagerepo.WithdrawRepo
	Risk        RiskApprover
}

type ChainProfile struct {
	From          common.Address
	ChainID       *big.Int
	FreezeReserve *big.Int // optional extra reserve in smallest unit; nil/0 means none
}

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
	profiles map[string]ChainProfile
	auth     *AuthSigner
	deps     Deps
	Producer *kafka.Producer
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
		canonical := spec.CanonicalChain
		if _, exists := normalizedProfiles[canonical]; exists {
			panic("duplicate chain profile: " + canonical)
		}
		normalizedProfiles[canonical] = p
	}

	return &Service{
		profiles: normalizedProfiles,
		auth:     NewAuthSigner(authSecret),
		deps:     deps,
		Producer: producer,
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
	Amount string // wei decimal
}

func (s *Service) CreateAndSignWithdraw(ctx context.Context, in WithdrawInput) (task *BroadcastTask, err error) {
	chain, profile, chainClient, err := s.resolveChainContext(in.Chain)
	if err != nil {
		return nil, err
	}
	log.Printf("[withdraw-service] create/sign start chain=%s from=%s to=%s amount=%s", chain, profile.From.Hex(), in.To, in.Amount)

	to, amt, err := validateWithdrawInput(in)
	if err != nil {
		log.Printf("[withdraw-service] validate input failed chain=%s from=%s to=%s amount=%s err=%v", chain, profile.From.Hex(), in.To, in.Amount, err)
		return nil, err
	}

	withdrawID := uuid.NewString()
	requestID := uuid.NewString()

	if err := s.freezeAndApprove(ctx, chain, profile, withdrawID, requestID, to, in.Amount, amt); err != nil {
		log.Printf("[withdraw-service] precheck failed chain=%s withdraw_id=%s request_id=%s err=%v", chain, withdrawID, requestID, err)
		return nil, err
	}
	defer func() {
		if err == nil {
			return
		}
		if rerr := s.releaseFrozenOnError(context.Background(), withdrawID); rerr != nil {
			log.Printf("[withdraw-service] release freeze failed chain=%s withdraw_id=%s err=%v", chain, withdrawID, rerr)
		}
	}()

	lockKey := "lock:nonce:" + chain + ":" + profile.From.Hex()
	lock := redisx.NewLock(s.deps.Redis, lockKey, 8*time.Second)
	if err := lock.TryLock(ctx); err != nil {
		log.Printf("[withdraw-service] nonce lock busy chain=%s from=%s err=%v", chain, profile.From.Hex(), err)
		return nil, errors.New("busy")
	}
	defer func() { _ = lock.Unlock(context.Background()) }()

	nv, err := chainClient.AllocateNonce(ctx, s.chainRuntime(chain, profile), func(ctx context.Context) (uint64, error) {
		return s.deps.Withdraw.NextNonceFloor(ctx, chain, profile.From.Hex())
	})
	if err != nil {
		log.Printf("[withdraw-service] nonce allocate failed chain=%s from=%s err=%v", chain, profile.From.Hex(), err)
		return nil, errors.New("nonce allocate failed")
	}

	unsignedTx, err := chainClient.BuildUnsignedWithdrawTx(ctx, s.chainRuntime(chain, profile), to, amt, nv)
	if err != nil {
		log.Printf("[withdraw-service] build unsigned tx failed chain=%s from=%s to=%s nonce=%d err=%v", chain, profile.From.Hex(), to.Hex(), nv, err)
		return nil, err
	}

	sresp, err := s.signWithdraw(ctx, chain, profile, withdrawID, requestID, to, in.Amount, unsignedTx)
	if err != nil {
		log.Printf("[withdraw-service] sign withdraw failed chain=%s withdraw_id=%s request_id=%s nonce=%d err=%v", chain, withdrawID, requestID, nv, err)
		return nil, err
	}
	log.Printf("[withdraw-service] sign withdraw success chain=%s withdraw_id=%s request_id=%s nonce=%d signed_size=%d", chain, withdrawID, requestID, nv, len(sresp.SignedTx))

	signedTxHex := hex.EncodeToString(sresp.SignedTx)
	if err := s.insertSignedOrder(ctx, chain, profile, withdrawID, requestID, to, in.Amount, nv, signedTxHex); err != nil {
		log.Printf("[withdraw-service] insert signed order failed chain=%s withdraw_id=%s request_id=%s nonce=%d err=%v", chain, withdrawID, requestID, nv, err)
		return nil, err
	}
	log.Printf("[withdraw-service] insert signed order success chain=%s withdraw_id=%s request_id=%s nonce=%d", chain, withdrawID, requestID, nv)

	return &BroadcastTask{
		Version:     1,
		WithdrawID:  withdrawID,
		RequestID:   requestID,
		Chain:       chain,
		From:        profile.From.Hex(),
		To:          to.Hex(),
		Amount:      in.Amount,
		Nonce:       nv,
		SignedTxHex: signedTxHex,
		CreatedAt:   time.Now().Unix(),
		Attempt:     0,
	}, nil
}

func (s *Service) freezeAndApprove(
	ctx context.Context,
	chain string,
	profile ChainProfile,
	withdrawID string,
	requestID string,
	to common.Address,
	amountStr string,
	amount *big.Int,
) error {
	freezeAmount := new(big.Int).Set(amount)
	if profile.FreezeReserve != nil && profile.FreezeReserve.Sign() > 0 {
		freezeAmount.Add(freezeAmount, profile.FreezeReserve)
	}
	if _, err := s.deps.Ledger.FreezeWithdraw(ctx, chain, profile.From.Hex(), withdrawID, freezeAmount); err != nil {
		return err
	}
	if s.deps.Risk == nil {
		return nil
	}
	return s.deps.Risk.ApproveWithdraw(ctx, RiskApproveInput{
		WithdrawID: withdrawID,
		RequestID:  requestID,
		Chain:      chain,
		From:       profile.From.Hex(),
		To:         to.Hex(),
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
	to common.Address,
	amount string,
	nonce uint64,
	signedTxHex string,
) error {
	return s.deps.Withdraw.InsertSigned(ctx, &model.WithdrawOrder{
		WithdrawID:   withdrawID,
		RequestID:    requestID,
		Chain:        chain,
		FromAddr:     profile.From.Hex(),
		ToAddr:       to.Hex(),
		Amount:       amount,
		Nonce:        nonce,
		SignedTxHex:  signedTxHex,
		SignedTxHash: "",
		TxHash:       "",
	})
}

func validateWithdrawInput(in WithdrawInput) (common.Address, *big.Int, error) {
	to := common.HexToAddress(in.To)
	if to == (common.Address{}) {
		return common.Address{}, nil, errors.New("invalid to address")
	}

	amt := new(big.Int)
	if _, ok := amt.SetString(in.Amount, 10); !ok || amt.Sign() <= 0 {
		return common.Address{}, nil, errors.New("invalid amount")
	}
	return to, amt, nil
}

func (s *Service) chainRuntime(chain string, profile ChainProfile) chainclient.Runtime {
	return chainclient.Runtime{
		Redis:   s.deps.Redis,
		Chain:   chain,
		ChainID: profile.ChainID,
		From:    profile.From,
	}
}

func (s *Service) signWithdraw(
	ctx context.Context,
	chain string,
	profile ChainProfile,
	withdrawID string,
	requestID string,
	to common.Address,
	amount string,
	unsignedTx []byte,
) (*signpb.SignResponse, error) {
	log.Printf("[withdraw-service] signer rpc start chain=%s withdraw_id=%s request_id=%s from=%s to=%s amount=%s unsigned_size=%d", chain, withdrawID, requestID, profile.From.Hex(), to.Hex(), amount, len(unsignedTx))

	authToken, _ := s.auth.MakeAuthToken(
		withdrawID,
		requestID,
		chain,
		profile.From.Hex(),
		to.Hex(),
		amount,
		unsignedTx,
	)

	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := s.deps.Signer.SignTransaction(sctx, &signpb.SignRequest{
		RequestId:   requestID,
		WithdrawId:  withdrawID,
		Chain:       chain,
		FromAddress: profile.From.Hex(),
		ToAddress:   to.Hex(),
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
