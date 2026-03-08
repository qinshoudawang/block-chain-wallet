package sol

import (
	"context"
	"errors"
	"math/big"
	"strings"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

type SignatureStatus struct {
	Slot               uint64
	Confirmations      *uint64
	Err                any
	ConfirmationStatus string
}

func (c *Client) Broadcast(ctx context.Context, signedTx []byte) (string, error) {
	if err := c.ensureRPC(); err != nil {
		return "", err
	}
	if len(signedTx) == 0 {
		return "", errors.New("empty solana signed tx")
	}
	tx, err := solana.TransactionFromBytes(signedTx)
	if err != nil {
		return "", errors.New("invalid solana signed tx")
	}
	maxRetries := uint(3)
	sig, err := c.rpc.SendTransactionWithOpts(ctx, tx, rpc.TransactionOpts{
		SkipPreflight:       false,
		PreflightCommitment: rpc.CommitmentConfirmed,
		MaxRetries:          &maxRetries,
	})
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(sig.String()) == "" {
		return "", errors.New("empty solana tx signature")
	}
	return sig.String(), nil
}

func (c *Client) LatestHeight(ctx context.Context) (uint64, error) {
	if err := c.ensureRPC(); err != nil {
		return 0, err
	}
	return c.rpc.GetSlot(ctx, rpc.CommitmentConfirmed)
}

func (c *Client) GetSignatureStatus(ctx context.Context, signature string) (*SignatureStatus, error) {
	if err := c.ensureRPC(); err != nil {
		return nil, err
	}
	sigStr := strings.TrimSpace(signature)
	if sigStr == "" {
		return nil, errors.New("empty solana signature")
	}
	sig, err := solana.SignatureFromBase58(sigStr)
	if err != nil {
		return nil, errors.New("invalid solana signature")
	}
	resp, err := c.rpc.GetSignatureStatuses(ctx, true, sig)
	if err != nil {
		if errors.Is(err, rpc.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if resp == nil || len(resp.Value) == 0 || resp.Value[0] == nil {
		return nil, nil
	}
	v := resp.Value[0]
	return &SignatureStatus{
		Slot:               v.Slot,
		Confirmations:      v.Confirmations,
		Err:                v.Err,
		ConfirmationStatus: string(v.ConfirmationStatus),
	}, nil
}

func (c *Client) GetTransactionFee(ctx context.Context, signature string) (*big.Int, error) {
	if err := c.ensureRPC(); err != nil {
		return nil, err
	}
	sigStr := strings.TrimSpace(signature)
	if sigStr == "" {
		return nil, errors.New("empty solana signature")
	}
	sig, err := solana.SignatureFromBase58(sigStr)
	if err != nil {
		return nil, errors.New("invalid solana signature")
	}
	zero := uint64(0)
	tx, err := c.rpc.GetTransaction(ctx, sig, &rpc.GetTransactionOpts{
		Encoding:                       solana.EncodingBase64,
		Commitment:                     rpc.CommitmentConfirmed,
		MaxSupportedTransactionVersion: &zero,
	})
	if err != nil {
		if errors.Is(err, rpc.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if tx == nil || tx.Meta == nil {
		return nil, nil
	}
	if tx.Meta.Err != nil {
		return nil, errors.New("solana transaction execution failed")
	}
	return new(big.Int).SetUint64(tx.Meta.Fee), nil
}
