package withdraw

import (
	"context"
	"errors"
	"strings"
	"time"

	"wallet-system/internal/broadcaster"
	"wallet-system/internal/helpers"
	withdrawmodel "wallet-system/internal/storage/model/withdraw"
	flowpkg "wallet-system/internal/withdraw/flow"
)

type rbfOrderContext struct {
	WithdrawID string
	OldTxHash  string
	Order      *withdrawmodel.WithdrawOrder
	Spec       helpers.ChainSpec
}

func (s *Service) initRBFFlowState(ctx context.Context, withdrawID string, oldTxHash string) (*flowpkg.State, error) {
	rbfCtx, err := s.loadAndValidateRBFOrder(ctx, withdrawID, oldTxHash)
	if err != nil {
		return nil, err
	}
	o := rbfCtx.Order
	return &flowpkg.State{
		Chain:         o.Chain,
		Spec:          rbfCtx.Spec,
		FromAddr:      o.FromAddr,
		ToAddr:        o.ToAddr,
		TokenContract: o.TokenContractAddress,
		AmountText:    o.Amount,
		WithdrawID:    o.WithdrawID,
		RequestID:     buildRequestID("withdraw-rbf"),
		Sequence:      o.Sequence,
		SignedPayload: o.SignedPayload,
		RBFOldTxHash:  rbfCtx.OldTxHash,
	}, nil
}

func (s *Service) buildUnsignedRBFFlow(ctx context.Context, st *flowpkg.State) (string, error) {
	chainCli, err := s.deps.ChainClient.ResolveByChain(st.Chain)
	if err != nil {
		return "", err
	}
	build, err := chainCli.BuildRBFUnsignedWithdrawTx(ctx, st.Chain, st.FromAddr, st.ToAddr, st.AmountText, st.SignedPayload)
	if err != nil {
		return "", err
	}
	st.RBFBuild = build
	st.UnsignedTx = build.UnsignedPayload
	return sequenceForLog(st.Sequence), nil
}

func (s *Service) persistSignedRBFFlow(ctx context.Context, st *flowpkg.State, signedTx []byte) (*broadcaster.BroadcastTask, error) {
	signedPayload, encoding, err := encodeSignedPayloadByFamily(st.Spec.Family, signedTx)
	if err != nil {
		return nil, err
	}
	ok, err := s.deps.Withdraw.SaveRBFSignedPayload(ctx, st.WithdrawID, st.RBFOldTxHash, signedPayload, encoding)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("save rbf signed payload failed")
	}
	return &broadcaster.BroadcastTask{
		TaskType:              broadcaster.TaskTypeWithdraw,
		Version:               1,
		WithdrawID:            st.WithdrawID,
		RequestID:             st.RequestID,
		Chain:                 st.Chain,
		From:                  st.FromAddr,
		To:                    st.ToAddr,
		Amount:                st.AmountText,
		Sequence:              st.Sequence,
		SignedPayload:         signedPayload,
		SignedPayloadEncoding: encoding,
		TokenContractAddress:  st.TokenContract,
		CreatedAt:             time.Now().Unix(),
		Attempt:               0,
	}, nil
}

func (s *Service) loadAndValidateRBFOrder(ctx context.Context, withdrawID string, oldTxHash string) (*rbfOrderContext, error) {
	withdrawID = strings.TrimSpace(withdrawID)
	oldTxHash = strings.TrimSpace(oldTxHash)
	if withdrawID == "" || oldTxHash == "" {
		return nil, errors.New("withdraw_id and old_tx_hash are required")
	}

	order, err := s.deps.Withdraw.GetByWithdrawID(ctx, withdrawID)
	if err != nil {
		return nil, err
	}
	if order.Status != withdrawmodel.StatusBROADCASTED {
		return nil, errors.New("withdraw is not in BROADCASTED status")
	}
	if !strings.EqualFold(strings.TrimSpace(order.TxHash), oldTxHash) {
		return nil, errors.New("old tx hash mismatch")
	}
	spec, err := helpers.ResolveChainSpec(order.Chain)
	if err != nil {
		return nil, err
	}
	switch spec.Family {
	case helpers.FamilyBTC, helpers.FamilyEVM, helpers.FamilySOL:
	default:
		return nil, errors.New("rbf is not supported for this chain family")
	}
	if strings.TrimSpace(order.SignedPayload) == "" {
		return nil, errors.New("signed payload is empty")
	}
	if strings.TrimSpace(order.TokenContractAddress) != "" {
		return nil, errors.New("rbf is not supported for token withdraw yet")
	}
	expectedEncoding, err := expectedSignedPayloadEncodingForFamily(spec.Family)
	if err != nil {
		return nil, err
	}
	if v := strings.TrimSpace(order.SignedPayloadEncoding); v != "" && !strings.EqualFold(v, expectedEncoding) {
		return nil, errors.New("rbf signed payload encoding mismatch")
	}
	return &rbfOrderContext{
		WithdrawID: withdrawID,
		OldTxHash:  oldTxHash,
		Order:      order,
		Spec:       spec,
	}, nil
}
