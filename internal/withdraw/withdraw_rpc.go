package withdraw

import (
	"context"
	"errors"
	"strings"

	withdrawpb "wallet-system/proto/withdraw"
)

type RBFServer struct {
	withdrawpb.UnimplementedWithdrawServiceServer
	withdrawSvc *Service
}

func NewRBFServer(withdrawSvc *Service) *RBFServer {
	return &RBFServer{
		withdrawSvc: withdrawSvc,
	}
}

func (s *RBFServer) SubmitTopUp(ctx context.Context, req *withdrawpb.SubmitTopUpRequest) (*withdrawpb.SubmitTopUpResponse, error) {
	if s == nil || s.withdrawSvc == nil {
		return nil, errors.New("withdraw topup server not configured")
	}
	task, err := s.withdrawSvc.CreateAndSignSystemWithdraw(ctx, WithdrawInput{
		Chain:  strings.TrimSpace(req.GetChain()),
		To:     strings.TrimSpace(req.GetToAddress()),
		Amount: strings.TrimSpace(req.GetAmount()),
		Token:  "",
	}, "sweeper-topup")
	if err != nil {
		return nil, err
	}
	return &withdrawpb.SubmitTopUpResponse{
		WithdrawId: task.WithdrawID,
		RequestId:  task.RequestID,
		Status:     "TOPUP_ENQUEUED",
	}, nil
}

func (s *RBFServer) SubmitRBF(ctx context.Context, req *withdrawpb.SubmitRBFRequest) (*withdrawpb.SubmitRBFResponse, error) {
	if s == nil || s.withdrawSvc == nil {
		return nil, errors.New("withdraw rbf server not configured")
	}
	res, err := s.withdrawSvc.CreateAndSignRBFWithdraw(
		ctx,
		strings.TrimSpace(req.GetWithdrawId()),
		strings.TrimSpace(req.GetOldTxHash()),
	)
	if err != nil {
		return nil, err
	}
	return &withdrawpb.SubmitRBFResponse{
		WithdrawId:            res.WithdrawID,
		RequestId:             res.RequestID,
		Status:                res.Status,
		OldFeeRateSatPerVbyte: res.OldFeeRateSatPerVbyte,
		NewFeeRateSatPerVbyte: res.NewFeeRateSatPerVbyte,
		OldFeeSat:             res.OldFeeSat,
		NewFeeSat:             res.NewFeeSat,
	}, nil
}
