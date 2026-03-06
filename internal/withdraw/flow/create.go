package flow

import (
	"context"

	"wallet-system/internal/broadcaster"
)

type CreateInput struct {
	Chain  string
	To     string
	Amount string
	Token  string
}

type CreateTemplate struct {
	Input              CreateInput
	CallerName         string
	SkipPrecheck       bool
	InitFn             func(in CreateInput, caller string) (*State, error)
	BeforeSignFn       func(ctx context.Context, st *State) error
	BuildTransactionFn func(ctx context.Context, st *State) (string, error)
	PersistFn          func(ctx context.Context, st *State, signedTx []byte) (*broadcaster.BroadcastTask, error)
	BroadcastFn        func(ctx context.Context, st *State, task *broadcaster.BroadcastTask) error
	OnErrorFn          func(ctx context.Context, st *State)
}

func (t CreateTemplate) Caller() string { return t.CallerName }

func (t CreateTemplate) Validate(ctx context.Context) (*State, error) {
	st, err := t.InitFn(t.Input, t.CallerName)
	if err != nil {
		return nil, err
	}
	if t.SkipPrecheck || t.BeforeSignFn == nil {
		return st, nil
	}
	if err := t.BeforeSignFn(ctx, st); err != nil {
		return nil, err
	}
	return st, nil
}

func (t CreateTemplate) BuildTransaction(ctx context.Context, st *State) (string, error) {
	return t.BuildTransactionFn(ctx, st)
}

func (t CreateTemplate) Persist(ctx context.Context, st *State, signedTx []byte) (*broadcaster.BroadcastTask, error) {
	return t.PersistFn(ctx, st, signedTx)
}

func (t CreateTemplate) Broadcast(ctx context.Context, st *State, task *broadcaster.BroadcastTask) error {
	if t.BroadcastFn == nil {
		return nil
	}
	return t.BroadcastFn(ctx, st, task)
}

func (t CreateTemplate) OnError(ctx context.Context, st *State) {
	if t.SkipPrecheck || t.OnErrorFn == nil {
		return
	}
	t.OnErrorFn(ctx, st)
}
