package withdraw

import (
	"context"
	"encoding/hex"
	"errors"
	"log"
	"math/big"
	"time"

	"wallet-system/internal/chain/evm"
	"wallet-system/internal/infra/kafka"
	"wallet-system/internal/infra/redisx"
	signpb "wallet-system/proto/signer"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Config struct {
	Chain      string
	ChainID    *big.Int
	From       common.Address
	AuthSecret []byte
}

type Deps struct {
	Redis   *redis.Client
	Eth     *ethclient.Client
	Builder *evm.EVMBuilder
	Signer  signpb.SignerServiceClient
}

type Service struct {
	cfg      Config
	deps     Deps
	auth     *AuthSigner
	Producer *kafka.Producer
}

func NewService(cfg Config, deps Deps, producer *kafka.Producer) *Service {
	return &Service{
		cfg:      cfg,
		deps:     deps,
		auth:     NewAuthSigner(cfg.AuthSecret),
		Producer: producer,
	}
}

type WithdrawInput struct {
	To     string
	Amount string // wei decimal
}

func (s *Service) CreateAndSignWithdraw(ctx context.Context, in WithdrawInput) (*BroadcastTask, error) {
	log.Printf("[withdraw-service] create/sign start chain=%s from=%s to=%s amount=%s", s.cfg.Chain, s.cfg.From.Hex(), in.To, in.Amount)

	to, amt, err := validateWithdrawInput(in)
	if err != nil {
		log.Printf("[withdraw-service] validate input failed chain=%s from=%s to=%s amount=%s err=%v", s.cfg.Chain, s.cfg.From.Hex(), in.To, in.Amount, err)
		return nil, err
	}

	lockKey := "lock:nonce:" + s.cfg.Chain + ":" + s.cfg.From.Hex()
	lock := redisx.NewLock(s.deps.Redis, lockKey, 8*time.Second)
	if err := lock.TryLock(ctx); err != nil {
		log.Printf("[withdraw-service] nonce lock busy chain=%s from=%s err=%v", s.cfg.Chain, s.cfg.From.Hex(), err)
		return nil, errors.New("busy")
	}
	defer func() { _ = lock.Unlock(context.Background()) }()

	// TODO: 这里应该先做：账本冻结 + 风控审批

	nv, err := s.allocateNonce(ctx)
	if err != nil {
		log.Printf("[withdraw-service] allocate nonce failed chain=%s from=%s err=%v", s.cfg.Chain, s.cfg.From.Hex(), err)
		return nil, err
	}

	unsignedTx, err := s.deps.Builder.BuildUnsignedTx(ctx, s.cfg.From, to, amt, nil, s.cfg.ChainID, nv)
	if err != nil {
		log.Printf("[withdraw-service] build unsigned tx failed chain=%s from=%s to=%s nonce=%d err=%v", s.cfg.Chain, s.cfg.From.Hex(), to.Hex(), nv, err)
		return nil, err
	}

	withdrawID := uuid.NewString()
	requestID := uuid.NewString()

	sresp, err := s.signWithdraw(ctx, withdrawID, requestID, to, in.Amount, unsignedTx)
	if err != nil {
		log.Printf("[withdraw-service] sign withdraw failed chain=%s withdraw_id=%s request_id=%s nonce=%d err=%v", s.cfg.Chain, withdrawID, requestID, nv, err)
		return nil, err
	}
	log.Printf("[withdraw-service] sign withdraw success chain=%s withdraw_id=%s request_id=%s nonce=%d signed_size=%d", s.cfg.Chain, withdrawID, requestID, nv, len(sresp.SignedTx))

	return &BroadcastTask{
		Version:     1,
		WithdrawID:  withdrawID,
		RequestID:   requestID,
		Chain:       s.cfg.Chain,
		From:        s.cfg.From.Hex(),
		To:          to.Hex(),
		Amount:      in.Amount,
		Nonce:       nv,
		SignedTxHex: hex.EncodeToString(sresp.SignedTx),
		CreatedAt:   time.Now().Unix(),
		Attempt:     0,
	}, nil
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

func (s *Service) allocateNonce(ctx context.Context) (uint64, error) {
	nm := evm.NewNonceManager(s.deps.Redis, s.deps.Eth, s.cfg.Chain, s.cfg.From)
	_ = nm.EnsureInitialized(ctx)

	nv, err := nm.Allocate(ctx)
	if err == nil {
		return nv, nil
	}
	log.Printf("[withdraw-service] allocate nonce retry after init chain=%s from=%s err=%v", s.cfg.Chain, s.cfg.From.Hex(), err)
	if err2 := nm.EnsureInitialized(ctx); err2 != nil {
		log.Printf("[withdraw-service] nonce ensure init failed chain=%s from=%s err=%v", s.cfg.Chain, s.cfg.From.Hex(), err2)
		return 0, errors.New("nonce init failed")
	}

	nv, err = nm.Allocate(ctx)
	if err != nil {
		log.Printf("[withdraw-service] nonce allocate failed after retry chain=%s from=%s err=%v", s.cfg.Chain, s.cfg.From.Hex(), err)
		return 0, errors.New("nonce allocate failed")
	}
	return nv, nil
}

func (s *Service) signWithdraw(
	ctx context.Context,
	withdrawID string,
	requestID string,
	to common.Address,
	amount string,
	unsignedTx []byte,
) (*signpb.SignResponse, error) {
	log.Printf("[withdraw-service] signer rpc start chain=%s withdraw_id=%s request_id=%s from=%s to=%s amount=%s unsigned_size=%d", s.cfg.Chain, withdrawID, requestID, s.cfg.From.Hex(), to.Hex(), amount, len(unsignedTx))

	authToken, _ := s.auth.MakeAuthToken(
		withdrawID,
		requestID,
		s.cfg.Chain,
		s.cfg.From.Hex(),
		to.Hex(),
		amount,
		unsignedTx,
	)

	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := s.deps.Signer.SignTransaction(sctx, &signpb.SignRequest{
		RequestId:   requestID,
		WithdrawId:  withdrawID,
		Chain:       s.cfg.Chain,
		FromAddress: s.cfg.From.Hex(),
		ToAddress:   to.Hex(),
		Amount:      amount,
		UnsignedTx:  unsignedTx,
		AuthToken:   authToken,
		Caller:      "withdraw-api",
	})
	if err != nil {
		log.Printf("[withdraw-service] signer rpc failed chain=%s withdraw_id=%s request_id=%s err=%v", s.cfg.Chain, withdrawID, requestID, err)
		return nil, err
	}
	log.Printf("[withdraw-service] signer rpc success chain=%s withdraw_id=%s request_id=%s signed_size=%d", s.cfg.Chain, withdrawID, requestID, len(resp.SignedTx))
	return resp, nil
}
