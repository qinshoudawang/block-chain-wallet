package withdraw

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	auth "wallet-system/internal/auth"
	"wallet-system/internal/broadcaster"
	"wallet-system/internal/helpers"
	"wallet-system/internal/infra/kafka"
	"wallet-system/internal/storage/model"
	"wallet-system/internal/storage/repo"
	withdrawchain "wallet-system/internal/withdraw/chainclient"
	signpb "wallet-system/proto/signer"
	withdrawpb "wallet-system/proto/withdraw"

	"github.com/google/uuid"
)

type RBFServer struct {
	withdrawpb.UnimplementedWithdrawServiceServer
	withdrawRepo  *repo.WithdrawRepo
	chainRegistry *withdrawchain.Registry
	signer        signpb.SignerServiceClient
	producer      *kafka.Producer
	authSecret    []byte
}

type btcRBFOrderContext struct {
	WithdrawID string
	OldTxHash  string
	Order      *model.WithdrawOrder
}

func NewRBFServer(
	withdrawRepo *repo.WithdrawRepo,
	chainRegistry *withdrawchain.Registry,
	signer signpb.SignerServiceClient,
	producer *kafka.Producer,
	authSecret []byte,
) *RBFServer {
	return &RBFServer{
		withdrawRepo:  withdrawRepo,
		chainRegistry: chainRegistry,
		signer:        signer,
		producer:      producer,
		authSecret:    authSecret,
	}
}

func (s *RBFServer) SubmitBTCRBF(ctx context.Context, req *withdrawpb.SubmitBTCRBFRequest) (*withdrawpb.SubmitBTCRBFResponse, error) {
	if err := s.validateRBFServerConfig(); err != nil {
		return nil, err
	}
	rbfCtx, err := s.loadAndValidateBTCRBFOrder(ctx, req)
	if err != nil {
		return nil, err
	}
	order := rbfCtx.Order

	chainCli, err := s.chainRegistry.ResolveByChain(order.Chain)
	if err != nil {
		return nil, err
	}
	feeTarget := normalizeBTCFeeTarget(req.GetFeeTargetBlocks())
	minDelta := normalizeMinRBFDelta(req.GetMinDeltaSatPerVbyte())
	build, err := chainCli.BuildRBFUnsignedWithdrawTx(
		ctx,
		order.Chain,
		order.FromAddr,
		order.ToAddr,
		order.Amount,
		order.SignedPayload,
		feeTarget,
		minDelta,
	)
	if err != nil {
		return nil, err
	}

	requestID := uuid.NewString()
	token := auth.MakeToken(s.authSecret, auth.TxPayload{
		WithdrawID: order.WithdrawID,
		RequestID:  requestID,
		Chain:      order.Chain,
		From:       order.FromAddr,
		To:         order.ToAddr,
		Amount:     order.Amount,
		UnsignedTx: build.UnsignedPayload,
	})
	signResp, err := s.signer.SignTransaction(ctx, &signpb.SignRequest{
		RequestId:   requestID,
		WithdrawId:  order.WithdrawID,
		Chain:       order.Chain,
		FromAddress: order.FromAddr,
		ToAddress:   order.ToAddr,
		Amount:      order.Amount,
		UnsignedTx:  build.UnsignedPayload,
		AuthToken:   token,
		Caller:      "withdraw-rbf",
	})
	if err != nil {
		return nil, err
	}
	newSignedHex := hex.EncodeToString(signResp.SignedTx)
	if ok, err := s.withdrawRepo.SaveRBFSignedPayload(
		ctx, order.WithdrawID, rbfCtx.OldTxHash, newSignedHex, broadcaster.SignedPayloadEncodingHex,
	); err != nil || !ok {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("save rbf signed payload failed")
	}

	task := broadcaster.BroadcastTask{
		Version:               1,
		WithdrawID:            order.WithdrawID,
		RequestID:             requestID,
		Chain:                 order.Chain,
		From:                  order.FromAddr,
		To:                    order.ToAddr,
		Amount:                order.Amount,
		Sequence:              order.Sequence,
		SignedPayload:         newSignedHex,
		SignedPayloadEncoding: broadcaster.SignedPayloadEncodingHex,
		ChainMetaJSON:         order.ChainMetaJSON,
		CreatedAt:             time.Now().Unix(),
		Attempt:               0,
	}
	taskBytes, err := json.Marshal(task)
	if err != nil {
		return nil, err
	}
	key := order.Chain + ":" + order.FromAddr
	if err := s.producer.Publish(ctx, key, taskBytes); err != nil {
		return nil, err
	}

	return &withdrawpb.SubmitBTCRBFResponse{
		WithdrawId:            order.WithdrawID,
		RequestId:             requestID,
		Status:                "RBF_ENQUEUED",
		OldFeeRateSatPerVbyte: build.OldFeeRate,
		NewFeeRateSatPerVbyte: build.NewFeeRate,
		OldFeeSat:             build.OldFee,
		NewFeeSat:             build.NewFee,
	}, nil
}

func (s *RBFServer) validateRBFServerConfig() error {
	if s == nil || s.withdrawRepo == nil || s.chainRegistry == nil || s.signer == nil || s.producer == nil || len(s.authSecret) == 0 {
		return errors.New("withdraw rbf server not configured")
	}
	return nil
}

func (s *RBFServer) loadAndValidateBTCRBFOrder(ctx context.Context, req *withdrawpb.SubmitBTCRBFRequest) (*btcRBFOrderContext, error) {
	withdrawID := strings.TrimSpace(req.GetWithdrawId())
	oldTxHash := strings.TrimSpace(req.GetOldTxHash())
	if withdrawID == "" || oldTxHash == "" {
		return nil, errors.New("withdraw_id and old_tx_hash are required")
	}

	order, err := s.withdrawRepo.GetByWithdrawID(ctx, withdrawID)
	if err != nil {
		return nil, err
	}
	if order.Status != model.StatusBROADCASTED {
		return nil, errors.New("withdraw is not in BROADCASTED status")
	}
	if !strings.EqualFold(strings.TrimSpace(order.TxHash), oldTxHash) {
		return nil, errors.New("old tx hash mismatch")
	}
	spec, err := helpers.ResolveChainSpec(order.Chain)
	if err != nil {
		return nil, err
	}
	if spec.Family != helpers.FamilyBTC {
		return nil, errors.New("only btc rbf is supported")
	}
	if strings.TrimSpace(order.SignedPayload) == "" {
		return nil, errors.New("signed payload is empty")
	}
	if v := strings.TrimSpace(order.SignedPayloadEncoding); v != "" && !strings.EqualFold(v, broadcaster.SignedPayloadEncodingHex) {
		return nil, errors.New("btc signed payload encoding must be hex")
	}
	return &btcRBFOrderContext{
		WithdrawID: withdrawID,
		OldTxHash:  oldTxHash,
		Order:      order,
	}, nil
}

func normalizeBTCFeeTarget(v int64) int64 {
	if v > 0 {
		return v
	}
	return 2
}

func normalizeMinRBFDelta(v int64) int64 {
	if v > 0 {
		return v
	}
	return 1
}
