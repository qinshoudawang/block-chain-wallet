package sweep

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"math/big"
	"strings"
	"time"

	auth "wallet-system/internal/auth"
	"wallet-system/internal/broadcaster"
	evmchain "wallet-system/internal/chain/evm"
	"wallet-system/internal/clients"
	"wallet-system/internal/infra/kafka"
	"wallet-system/internal/infra/redisx"
	"wallet-system/internal/storage/repo"
	signpb "wallet-system/proto/signer"
	withdrawpb "wallet-system/proto/withdraw"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type EVMSweeperConfig struct {
	Chain          string
	ChainID        *big.Int
	HotAddress     string
	TokenContract  string
	MinNativeWei   *big.Int
	MinTokenAmount *big.Int
	GasReserveWei  *big.Int
	TokenTopUpWei  *big.Int
	PollInterval   time.Duration
	LockTTL        time.Duration
	CandidateLimit int
}

type EVMSweeper struct {
	cfg         EVMSweeperConfig
	evm         *evmchain.Client
	signer      signpb.SignerServiceClient
	auth        auth.Provider
	producer    *kafka.Producer
	redis       *redis.Client
	sweepRepo   *repo.SweepRepo
	withdrawRPC withdrawpb.WithdrawServiceClient
}

func NewEVMSweeper(
	cfg EVMSweeperConfig,
	evm *evmchain.Client,
	signerCli *clients.SignerClient,
	authProvider auth.Provider,
	producer *kafka.Producer,
	redisClient *redis.Client,
	sweepRepo *repo.SweepRepo,
	withdrawCli *clients.WithdrawClient,
) *EVMSweeper {
	var signer signpb.SignerServiceClient
	if signerCli != nil {
		signer = signerCli.Client
	}
	var withdrawRPC withdrawpb.WithdrawServiceClient
	if withdrawCli != nil {
		withdrawRPC = withdrawCli.Client
	}
	return &EVMSweeper{
		cfg:         cfg,
		evm:         evm,
		signer:      signer,
		auth:        authProvider,
		producer:    producer,
		redis:       redisClient,
		sweepRepo:   sweepRepo,
		withdrawRPC: withdrawRPC,
	}
}

func (s *EVMSweeper) Run(ctx context.Context) {
	if s == nil || s.evm == nil || s.signer == nil || s.auth == nil || s.sweepRepo == nil || s.producer == nil {
		return
	}
	poll := s.cfg.PollInterval
	if poll <= 0 {
		poll = 30 * time.Second
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()

	s.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *EVMSweeper) tick(ctx context.Context) {
	asset := strings.TrimSpace(s.cfg.TokenContract)
	unlock, locked, err := s.acquireWorkerLock(ctx, asset)
	if err != nil {
		log.Printf("[sweeper-evm] lock acquire failed chain=%s asset=%s err=%v", s.cfg.Chain, asset, err)
		return
	}
	if !locked {
		return
	}
	defer unlock()

	candidates, err := s.sweepRepo.ListCandidates(ctx, s.cfg.Chain, asset, s.cfg.CandidateLimit)
	if err != nil {
		log.Printf("[sweeper-evm] list candidates failed chain=%s err=%v", s.cfg.Chain, err)
		return
	}
	for i := range candidates {
		s.handleCandidate(ctx, asset, candidates[i])
	}
}

func (s *EVMSweeper) acquireWorkerLock(ctx context.Context, asset string) (func(), bool, error) {
	if s == nil || s.redis == nil {
		return func() {}, true, nil
	}
	ttl := s.cfg.LockTTL
	if ttl <= 0 {
		ttl = 90 * time.Second
	}
	assetKey := strings.ToLower(strings.TrimSpace(asset))
	if assetKey == "" {
		assetKey = "native"
	}
	key := "lock:sweep:worker:" + strings.ToLower(strings.TrimSpace(s.cfg.Chain)) + ":" + assetKey
	unlock, err := redisx.Acquire(ctx, s.redis, key, ttl)
	if err != nil {
		if errors.Is(err, redisx.ErrLockNotAcquired) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return unlock, true, nil
}

func (s *EVMSweeper) handleCandidate(ctx context.Context, asset string, c repo.SweepCandidate) {
	amount, ok := s.resolveSweepAmount(asset, c.AvailableAmount)
	if !ok {
		return
	}
	hasActive, err := s.sweepRepo.HasActiveSweep(ctx, c.Chain, c.Address, asset)
	if err != nil {
		log.Printf("[sweeper-evm] check active failed chain=%s from=%s err=%v", c.Chain, c.Address, err)
		return
	}
	if hasActive {
		return
	}
	if ready, err := s.ensureTokenGas(ctx, c.Address); err != nil {
		log.Printf("[sweeper-evm] ensure token gas failed chain=%s from=%s err=%v", c.Chain, c.Address, err)
		return
	} else if !ready {
		return
	}
	sweepID, created, err := s.createInitSweep(ctx, asset, c, amount)
	if err != nil {
		log.Printf("[sweeper-evm] create sweep failed sweep_id=%s err=%v", sweepID, err)
		return
	}
	if !created {
		return
	}
	if err := s.executeSweep(ctx, sweepID, c, amount); err != nil {
		_ = s.sweepRepo.MarkFailed(ctx, sweepID, err.Error())
		log.Printf("[sweeper-evm] execute failed sweep_id=%s from=%s amount=%s err=%v", sweepID, c.Address, amount.String(), err)
	}
}

func (s *EVMSweeper) ensureTokenGas(ctx context.Context, toAddress string) (bool, error) {
	if strings.TrimSpace(s.cfg.TokenContract) == "" {
		return true, nil
	}
	if s.cfg.GasReserveWei == nil || s.cfg.GasReserveWei.Sign() <= 0 {
		return true, nil
	}
	bal, err := s.evm.BalanceAt(ctx, common.HexToAddress(toAddress))
	if err != nil {
		return false, err
	}
	if bal.Cmp(s.cfg.GasReserveWei) >= 0 {
		return true, nil
	}
	if s.cfg.TokenTopUpWei == nil || s.cfg.TokenTopUpWei.Sign() <= 0 {
		return false, errors.New("token gas is insufficient and topup amount is not configured")
	}
	reqID, err := s.sendGasTopUp(ctx, toAddress, s.cfg.TokenTopUpWei)
	if err != nil {
		return false, err
	}
	log.Printf("[sweeper-evm] gas topup enqueued chain=%s to=%s amount=%s req=%s", s.cfg.Chain, toAddress, s.cfg.TokenTopUpWei.String(), reqID)
	return false, nil
}

func (s *EVMSweeper) sendGasTopUp(ctx context.Context, toAddress string, amountWei *big.Int) (string, error) {
	if s.withdrawRPC == nil {
		return "", errors.New("withdraw rpc not configured")
	}
	resp, err := s.withdrawRPC.SubmitTopUp(ctx, &withdrawpb.SubmitTopUpRequest{
		Chain:     s.cfg.Chain,
		ToAddress: strings.TrimSpace(toAddress),
		Amount:    amountWei.String(),
	})
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(resp.GetWithdrawId()), nil
}

func (s *EVMSweeper) resolveSweepAmount(asset string, availableRaw string) (*big.Int, bool) {
	available, ok := new(big.Int).SetString(strings.TrimSpace(availableRaw), 10)
	if !ok || available.Sign() <= 0 {
		return nil, false
	}
	amount := new(big.Int).Set(available)
	// Native sweep needs to leave gas reserve for future txs from this address.
	if asset == "" && s.cfg.GasReserveWei != nil && s.cfg.GasReserveWei.Sign() > 0 {
		amount.Sub(amount, s.cfg.GasReserveWei)
	}
	if amount.Sign() <= 0 {
		return nil, false
	}
	minAmount := s.minAmountByAsset(asset)
	if minAmount != nil && amount.Cmp(minAmount) < 0 {
		return nil, false
	}
	return amount, true
}

func (s *EVMSweeper) minAmountByAsset(asset string) *big.Int {
	if strings.TrimSpace(asset) == "" {
		return s.cfg.MinNativeWei
	}
	return s.cfg.MinTokenAmount
}

func (s *EVMSweeper) createInitSweep(
	ctx context.Context,
	asset string,
	c repo.SweepCandidate,
	amount *big.Int,
) (string, bool, error) {
	// Persist INIT order first so recovery/retry has a durable business key.
	sweepID := "swp:" + uuid.NewString()
	created, err := s.sweepRepo.CreateInit(ctx, repo.SweepCreateInput{
		SweepID:              sweepID,
		Chain:                c.Chain,
		UserID:               c.UserID,
		FromAddress:          c.Address,
		ToAddress:            s.cfg.HotAddress,
		AssetContractAddress: asset,
		Amount:               amount.String(),
	})
	return sweepID, created, err
}

func (s *EVMSweeper) executeSweep(ctx context.Context, sweepID string, c repo.SweepCandidate, amount *big.Int) error {
	from := common.HexToAddress(c.Address)
	// Pull latest pending nonce to keep per-address tx sequence monotonic.
	nonce, err := s.evm.PendingNonceAt(ctx, from)
	if err != nil {
		return err
	}

	to := common.HexToAddress(s.cfg.HotAddress)
	target := to
	value := amount
	var data []byte
	tokenContract := strings.TrimSpace(s.cfg.TokenContract)
	if tokenContract != "" {
		if !common.IsHexAddress(tokenContract) {
			return errors.New("invalid sweep token contract")
		}
		// ERC20 sweep uses token transfer calldata, native value remains zero.
		data, err = encodeERC20TransferData(to, amount)
		if err != nil {
			return err
		}
		target = common.HexToAddress(tokenContract)
		value = big.NewInt(0)
	}

	unsigned, err := s.evm.BuildUnsignedTx(ctx, from, target, value, data, s.cfg.ChainID, nonce)
	if err != nil {
		return err
	}
	// Signing is delegated to signer service; sweeper does not handle private keys.
	raw, err := s.signSweepTx(ctx, sweepID, c, amount, unsigned)
	if err != nil {
		return err
	}
	task := broadcaster.BroadcastTask{
		TaskType:              broadcaster.TaskTypeSweep,
		Version:               1,
		SweepID:               sweepID,
		RequestID:             sweepID,
		Chain:                 c.Chain,
		From:                  c.Address,
		To:                    s.cfg.HotAddress,
		Amount:                amount.String(),
		Sequence:              nonce,
		SignedPayload:         hex.EncodeToString(raw),
		SignedPayloadEncoding: broadcaster.SignedPayloadEncodingHex,
		TokenContractAddress:  tokenContract,
		CreatedAt:             time.Now().Unix(),
		Attempt:               0,
	}
	b, err := json.Marshal(task)
	if err != nil {
		return err
	}
	key := strings.ToLower(strings.TrimSpace(c.Chain)) + ":" + strings.ToLower(strings.TrimSpace(c.Address))
	return s.producer.Publish(ctx, key, b)
}

func (s *EVMSweeper) signSweepTx(
	ctx context.Context,
	sweepID string,
	c repo.SweepCandidate,
	amount *big.Int,
	unsignedTx []byte,
) ([]byte, error) {
	return s.signTx(ctx, sweepID, c.Address, s.cfg.HotAddress, amount.String(), "sweeper", unsignedTx)
}

func (s *EVMSweeper) signTx(
	ctx context.Context,
	bizID string,
	fromAddress string,
	toAddress string,
	amount string,
	caller string,
	unsignedTx []byte,
) ([]byte, error) {
	requestID := "sweep-sign:" + uuid.NewString()
	// HMAC binds key fields + unsigned payload hash to prevent payload tampering in transit.
	authToken, err := auth.MakeTokenWithProvider(ctx, s.auth, auth.TxPayload{
		WithdrawID: bizID,
		RequestID:  requestID,
		Chain:      s.cfg.Chain,
		From:       fromAddress,
		To:         toAddress,
		Amount:     amount,
		UnsignedTx: unsignedTx,
	})
	if err != nil {
		return nil, err
	}
	sctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	resp, err := s.signer.SignTransaction(sctx, &signpb.SignRequest{
		RequestId:   requestID,
		WithdrawId:  bizID,
		Chain:       s.cfg.Chain,
		FromAddress: fromAddress,
		ToAddress:   toAddress,
		Amount:      amount,
		UnsignedTx:  unsignedTx,
		AuthToken:   authToken,
		Caller:      caller,
	})
	if err != nil {
		return nil, err
	}
	if len(resp.GetSignedTx()) == 0 {
		return nil, errors.New("empty signed tx from signer")
	}
	return resp.GetSignedTx(), nil
}

func encodeERC20TransferData(to common.Address, amount *big.Int) ([]byte, error) {
	if amount == nil || amount.Sign() <= 0 {
		return nil, errors.New("invalid sweep amount")
	}
	methodID := []byte{0xa9, 0x05, 0x9c, 0xbb}
	paddedTo := common.LeftPadBytes(to.Bytes(), 32)
	paddedAmount := common.LeftPadBytes(amount.Bytes(), 32)
	out := make([]byte, 0, 4+32+32)
	out = append(out, methodID...)
	out = append(out, paddedTo...)
	out = append(out, paddedAmount...)
	return out, nil
}
