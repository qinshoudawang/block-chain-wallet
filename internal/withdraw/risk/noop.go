package risk

import (
	"context"

	"wallet-system/internal/withdraw"
)

// NoopApprover is a placeholder risk implementation that always allows withdrawals.
type NoopApprover struct{}

func NewNoopApprover() *NoopApprover {
	return &NoopApprover{}
}

func (n *NoopApprover) ApproveWithdraw(ctx context.Context, in withdraw.RiskApproveInput) error {
	return nil
}
