package withdraw

import (
	"context"
	"errors"
	"log"
	"time"

	"wallet-system/internal/broadcaster"
	"wallet-system/internal/withdraw/chainclient"
	flowpkg "wallet-system/internal/withdraw/flow"

	"github.com/google/uuid"
)

type createFlowData struct {
	profile        ChainProfile
	chainClient    chainclient.Client
	runtime        chainclient.Runtime
	sequenceUnlock func()
}

func (s *Service) initCreateFlowState(in flowpkg.CreateInput, caller string) (*flowpkg.State, error) {
	chain, profile, chainClient, err := s.resolveChainContext(in.Chain)
	if err != nil {
		return nil, err
	}
	toAddr, amt, tokenContract, err := s.validateWithdrawInput(chain, WithdrawInput{
		Chain: in.Chain, To: in.To, Amount: in.Amount, Token: in.Token,
	}, chainClient)
	if err != nil {
		return nil, err
	}
	st := &flowpkg.State{
		Chain:         chain,
		FromAddr:      profile.FromAddress,
		ToAddr:        toAddr,
		TokenContract: tokenContract,
		AmountText:    in.Amount,
		AmountValue:   amt,
		WithdrawID:    uuid.NewString(),
		RequestID:     buildRequestID(caller),
	}
	st.Data = &createFlowData{
		profile:     profile,
		chainClient: chainClient,
		runtime:     s.chainRuntime(chain, profile, st.WithdrawID, tokenContract),
	}
	return st, nil
}

func (s *Service) beforeSignCreateFlow(ctx context.Context, st *flowpkg.State) error {
	d, ok := st.Data.(*createFlowData)
	if !ok || d == nil {
		return errors.New("invalid create flow state")
	}
	return s.freezeAndApprove(ctx, st.Chain, d.profile, st.WithdrawID, st.RequestID, st.TokenContract, st.ToAddr, st.AmountText, st.AmountValue)
}

func (s *Service) buildUnsignedCreateFlow(ctx context.Context, st *flowpkg.State) (string, error) {
	d, ok := st.Data.(*createFlowData)
	if !ok || d == nil {
		return "", errors.New("invalid create flow state")
	}
	alloc, err := s.allocateSequence(ctx, &d.runtime, d.chainClient)
	if err != nil {
		return "", err
	}
	st.Sequence = alloc.Value
	unsignedTx, utxoReserved, err := s.prepareUnsignedTx(ctx, d.chainClient, d.runtime, st.ToAddr, st.AmountValue, st.Sequence)
	if err != nil {
		if alloc.Unlock != nil {
			alloc.Unlock()
		}
		return "", err
	}
	st.UnsignedTx = unsignedTx
	st.UTXOReserved = utxoReserved
	d.sequenceUnlock = alloc.Unlock
	return sequenceForLog(st.Sequence), nil
}

func (s *Service) persistSignedCreateFlow(ctx context.Context, st *flowpkg.State, signedTx []byte) (*broadcaster.BroadcastTask, error) {
	d, ok := st.Data.(*createFlowData)
	if !ok || d == nil {
		return nil, errors.New("invalid create flow state")
	}
	if d.sequenceUnlock != nil {
		defer d.sequenceUnlock()
	}
	signedPayload, signedPayloadEncoding, err := s.insertSignedOrder(
		ctx,
		st.Chain,
		st.FromAddr,
		st.WithdrawID,
		st.RequestID,
		st.ToAddr,
		st.AmountText,
		st.Sequence,
		signedTx,
		st.TokenContract,
	)
	if err != nil {
		return nil, err
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
		SignedPayloadEncoding: signedPayloadEncoding,
		TokenContractAddress:  st.TokenContract,
		CreatedAt:             time.Now().Unix(),
		Attempt:               0,
	}, nil
}

func (s *Service) onErrorCreateFlow(ctx context.Context, st *flowpkg.State) {
	_ = ctx
	if err := s.releaseFrozenOnError(context.Background(), st.WithdrawID); err != nil {
		log.Printf("[withdraw-service] release freeze failed chain=%s withdraw_id=%s err=%v", st.Chain, st.WithdrawID, err)
	}
}
