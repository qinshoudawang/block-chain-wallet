package flow

import (
	"context"
	"errors"
	"math/big"
	"strings"

	"wallet-system/internal/broadcaster"
	"wallet-system/internal/helpers"
	"wallet-system/internal/withdraw/chainclient"
)

type State struct {
	Chain         string
	Spec          helpers.ChainSpec
	FromAddr      string
	ToAddr        string
	TokenContract string
	AmountText    string
	AmountValue   *big.Int
	WithdrawID    string
	RequestID     string
	Sequence      uint64
	UnsignedTx    []byte
	SignedPayload string
	RBFOldTxHash  string
	RBFBuild      *chainclient.RBFUnsignedBuildResult
	UTXOReserved  bool
	Data          any
}

type Template interface {
	Caller() string
	Validate(ctx context.Context) (*State, error)
	BuildTransaction(ctx context.Context, st *State) (string, error)
	Persist(ctx context.Context, st *State, signedTx []byte) (*broadcaster.BroadcastTask, error)
	Broadcast(ctx context.Context, st *State, task *broadcaster.BroadcastTask) error
	OnError(ctx context.Context, st *State)
}

type RunnerDeps struct {
	SignFn        func(ctx context.Context, st *State, caller string) ([]byte, error)
	ReleaseUTXOFn func(ctx context.Context, withdrawID string) error
}

func Run(ctx context.Context, tpl Template, deps RunnerDeps) (*State, *broadcaster.BroadcastTask, error) {
	if tpl == nil || strings.TrimSpace(tpl.Caller()) == "" {
		return nil, nil, errors.New("withdraw template caller is required")
	}
	if deps.SignFn == nil {
		return nil, nil, errors.New("sign function is required")
	}

	// Step 1: validate
	st, err := tpl.Validate(ctx)
	if err != nil {
		return nil, nil, err
	}
	succeeded := false
	rollback := func(st *State) {
		if st != nil && st.UTXOReserved && deps.ReleaseUTXOFn != nil {
			_ = deps.ReleaseUTXOFn(context.Background(), st.WithdrawID)
		}
		tpl.OnError(context.Background(), st)
	}
	defer func() {
		if succeeded {
			return
		}
		rollback(st)
	}()

	// Step 2: build transaction
	if _, err := tpl.BuildTransaction(ctx, st); err != nil {
		return nil, nil, err
	}

	// Step 3: sign
	signedTx, err := deps.SignFn(ctx, st, tpl.Caller())
	if err != nil {
		return nil, nil, err
	}
	// Step 4: persist
	task, err := tpl.Persist(ctx, st, signedTx)
	if err != nil {
		return nil, nil, err
	}
	// Step 5: broadcast
	if err := tpl.Broadcast(ctx, st, task); err != nil {
		return nil, nil, err
	}
	succeeded = true
	return st, task, nil
}
