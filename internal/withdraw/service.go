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
	"wallet-system/internal/storage/model"
	storagerepo "wallet-system/internal/storage/repo"
	signpb "wallet-system/proto/signer"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Config struct {
	Chain         string
	ChainID       *big.Int
	From          common.Address
	AuthSecret    []byte
	GasReserveWei *big.Int
}

type Deps struct {
	Redis    *redis.Client
	Eth      *ethclient.Client
	Builder  *evm.EVMBuilder
	Signer   signpb.SignerServiceClient
	Ledger   *storagerepo.LedgerRepo
	Withdraw *storagerepo.WithdrawRepo
	Risk     RiskApprover
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
	cfg      Config
	deps     Deps
	auth     *AuthSigner
	Producer *kafka.Producer
}

func NewService(cfg Config, deps Deps, producer *kafka.Producer) *Service {
	if deps.Ledger == nil {
		panic("ledger repo is required")
	}
	if deps.Withdraw == nil {
		panic("withdraw repo is required")
	}
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

func (s *Service) CreateAndSignWithdraw(ctx context.Context, in WithdrawInput) (task *BroadcastTask, err error) {
	log.Printf("[withdraw-service] create/sign start chain=%s from=%s to=%s amount=%s", s.cfg.Chain, s.cfg.From.Hex(), in.To, in.Amount)

	to, amt, err := validateWithdrawInput(in)
	if err != nil {
		log.Printf("[withdraw-service] validate input failed chain=%s from=%s to=%s amount=%s err=%v", s.cfg.Chain, s.cfg.From.Hex(), in.To, in.Amount, err)
		return nil, err
	}

	withdrawID := uuid.NewString()
	requestID := uuid.NewString()

	if err := s.freezeAndApprove(ctx, withdrawID, requestID, to, in.Amount, amt); err != nil {
		log.Printf("[withdraw-service] precheck failed chain=%s withdraw_id=%s request_id=%s err=%v", s.cfg.Chain, withdrawID, requestID, err)
		return nil, err
	}
	defer func() {
		if err == nil {
			return
		}
		if rerr := s.releaseFrozenOnError(context.Background(), withdrawID); rerr != nil {
			log.Printf("[withdraw-service] release freeze failed chain=%s withdraw_id=%s err=%v", s.cfg.Chain, withdrawID, rerr)
		}
	}()

	lockKey := "lock:nonce:" + s.cfg.Chain + ":" + s.cfg.From.Hex()
	lock := redisx.NewLock(s.deps.Redis, lockKey, 8*time.Second)
	if err := lock.TryLock(ctx); err != nil {
		log.Printf("[withdraw-service] nonce lock busy chain=%s from=%s err=%v", s.cfg.Chain, s.cfg.From.Hex(), err)
		return nil, errors.New("busy")
	}
	defer func() { _ = lock.Unlock(context.Background()) }()

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

	sresp, err := s.signWithdraw(ctx, withdrawID, requestID, to, in.Amount, unsignedTx)
	if err != nil {
		log.Printf("[withdraw-service] sign withdraw failed chain=%s withdraw_id=%s request_id=%s nonce=%d err=%v", s.cfg.Chain, withdrawID, requestID, nv, err)
		return nil, err
	}
	log.Printf("[withdraw-service] sign withdraw success chain=%s withdraw_id=%s request_id=%s nonce=%d signed_size=%d", s.cfg.Chain, withdrawID, requestID, nv, len(sresp.SignedTx))

	signedTxHex := hex.EncodeToString(sresp.SignedTx)
	if err := s.insertSignedOrder(ctx, withdrawID, requestID, to, in.Amount, nv, signedTxHex); err != nil {
		log.Printf("[withdraw-service] insert signed order failed chain=%s withdraw_id=%s request_id=%s nonce=%d err=%v", s.cfg.Chain, withdrawID, requestID, nv, err)
		return nil, err
	}
	log.Printf("[withdraw-service] insert signed order success chain=%s withdraw_id=%s request_id=%s nonce=%d", s.cfg.Chain, withdrawID, requestID, nv)

	return &BroadcastTask{
		Version:     1,
		WithdrawID:  withdrawID,
		RequestID:   requestID,
		Chain:       s.cfg.Chain,
		From:        s.cfg.From.Hex(),
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
	withdrawID string,
	requestID string,
	to common.Address,
	amountStr string,
	amount *big.Int,
) error {
	freezeAmount := new(big.Int).Set(amount)
	if s.cfg.GasReserveWei != nil && s.cfg.GasReserveWei.Sign() > 0 {
		freezeAmount.Add(freezeAmount, s.cfg.GasReserveWei)
	}
	if _, err := s.deps.Ledger.FreezeWithdraw(ctx, s.cfg.Chain, s.cfg.From.Hex(), withdrawID, freezeAmount); err != nil {
		return err
	}
	if s.deps.Risk == nil {
		return nil
	}
	return s.deps.Risk.ApproveWithdraw(ctx, RiskApproveInput{
		WithdrawID: withdrawID,
		RequestID:  requestID,
		Chain:      s.cfg.Chain,
		From:       s.cfg.From.Hex(),
		To:         to.Hex(),
		Amount:     amountStr,
	})
}

func (s *Service) releaseFrozenOnError(ctx context.Context, withdrawID string) error {
	return s.deps.Ledger.ReleaseWithdrawFreeze(ctx, withdrawID)
}

func (s *Service) insertSignedOrder(
	ctx context.Context,
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
		Chain:        s.cfg.Chain,
		FromAddr:     s.cfg.From.Hex(),
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

func (s *Service) allocateNonce(ctx context.Context) (uint64, error) {
	nm := evm.NewNonceManager(s.deps.Redis, s.deps.Eth, s.cfg.Chain, s.cfg.From)
	nm.WithNonceFloorProvider(func(ctx context.Context) (uint64, error) {
		return s.deps.Withdraw.NextNonceFloor(ctx, s.cfg.Chain, s.cfg.From.Hex())
	})
	if err := nm.EnsureInitialized(ctx); err != nil {
		log.Printf("[withdraw-service] nonce ensure init failed chain=%s from=%s err=%v", s.cfg.Chain, s.cfg.From.Hex(), err)
		return 0, errors.New("nonce init failed")
	}

	nv, err := nm.Allocate(ctx)
	if err != nil {
		log.Printf("[withdraw-service] nonce allocate failed chain=%s from=%s err=%v", s.cfg.Chain, s.cfg.From.Hex(), err)
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
