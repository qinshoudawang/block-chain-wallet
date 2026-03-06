package flow

import (
	"context"

	"wallet-system/internal/broadcaster"
)

type RBFTemplate struct {
	WithdrawID         string
	OldTxHash          string
	ValidateFn         func(ctx context.Context, withdrawID string, oldTxHash string) (*State, error)
	BuildTransactionFn func(ctx context.Context, st *State) (string, error)
	PersistFn          func(ctx context.Context, st *State, signedTx []byte) (*broadcaster.BroadcastTask, error)
	BroadcastFn        func(ctx context.Context, st *State, task *broadcaster.BroadcastTask) error
}

func (RBFTemplate) Caller() string { return "withdraw-rbf" }

func (t RBFTemplate) Validate(ctx context.Context) (*State, error) {
	return t.ValidateFn(ctx, t.WithdrawID, t.OldTxHash)
}

func (t RBFTemplate) BuildTransaction(ctx context.Context, st *State) (string, error) {
	return t.BuildTransactionFn(ctx, st)
}

func (t RBFTemplate) Persist(ctx context.Context, st *State, signedTx []byte) (*broadcaster.BroadcastTask, error) {
	return t.PersistFn(ctx, st, signedTx)
}

func (t RBFTemplate) Broadcast(ctx context.Context, st *State, task *broadcaster.BroadcastTask) error {
	if t.BroadcastFn == nil {
		return nil
	}
	return t.BroadcastFn(ctx, st, task)
}

func (RBFTemplate) OnError(context.Context, *State) {}
