package sol

import (
	"context"
	"errors"
	"strings"

	"github.com/gagliardetto/solana-go/rpc"
)

func (c *Client) GetBlock(ctx context.Context, slot uint64) (*rpc.GetBlockResult, error) {
	if err := c.ensureRPC(); err != nil {
		return nil, err
	}
	zero := uint64(0)
	rewards := false
	return c.rpc.GetBlockWithOpts(ctx, slot, &rpc.GetBlockOpts{
		Commitment:                     rpc.CommitmentConfirmed,
		TransactionDetails:             rpc.TransactionDetailsFull,
		Rewards:                        &rewards,
		MaxSupportedTransactionVersion: &zero,
	})
}

func BlockHashString(v interface{ String() string }) string {
	return strings.TrimSpace(v.String())
}

func ParentHashFromBlock(block *rpc.GetBlockResult) (string, error) {
	if block == nil {
		return "", errors.New("solana block is required")
	}
	return strings.TrimSpace(block.PreviousBlockhash.String()), nil
}
