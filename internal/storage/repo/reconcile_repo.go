package repo

import (
	"context"
	"errors"
	"strings"
	"time"

	reconcilemodel "wallet-system/internal/storage/model/reconcile"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type OnchainReconciliationUpsertInput struct {
	Scope                string
	UserID               string
	Chain                string
	Address              string
	AssetContractAddress string
	OnchainBalanceAmount string
	LedgerBalanceAmount  string
	BalanceDiffAmount    string
	ReconciliationStatus string
	HasMismatch          bool
	LastErrorMessage     string
	ReconciledAt         time.Time
}

type FlowReconciliationUpsertInput struct {
	FlowSource           string
	BusinessType         string
	BusinessID           string
	UserID               string
	Chain                string
	AssetContractAddress string
	ExpectedChangeAmount string
	ActualChangeAmount   string
	ReconciliationStatus string
	HasMismatch          bool
	LastErrorMessage     string
	ReconciledAt         time.Time
}

type ReconcileRepo struct {
	db *gorm.DB
}

func NewReconcileRepo(db *gorm.DB) *ReconcileRepo {
	return &ReconcileRepo{db: db}
}

func (r *ReconcileRepo) UpsertOnchainReconciliation(ctx context.Context, in OnchainReconciliationUpsertInput) error {
	if r == nil || r.db == nil {
		return errors.New("reconcile repo not configured")
	}
	scope := strings.ToUpper(strings.TrimSpace(in.Scope))
	if scope == "" {
		return errors.New("scope is required")
	}
	rec := reconcilemodel.OnchainLedgerReconciliation{
		Scope:                scope,
		UserID:               strings.TrimSpace(in.UserID),
		Chain:                strings.ToLower(strings.TrimSpace(in.Chain)),
		Address:              strings.TrimSpace(in.Address),
		AssetContractAddress: strings.TrimSpace(in.AssetContractAddress),
		OnchainBalanceAmount: strings.TrimSpace(in.OnchainBalanceAmount),
		LedgerBalanceAmount:  strings.TrimSpace(in.LedgerBalanceAmount),
		BalanceDiffAmount:    strings.TrimSpace(in.BalanceDiffAmount),
		ReconciliationStatus: strings.ToUpper(strings.TrimSpace(in.ReconciliationStatus)),
		HasMismatch:          in.HasMismatch,
		LastErrorMessage:     strings.TrimSpace(in.LastErrorMessage),
		ReconciledAt:         &in.ReconciledAt,
	}
	logRec := reconcilemodel.OnchainLedgerReconciliationLog{
		Scope:                scope,
		UserID:               strings.TrimSpace(in.UserID),
		Chain:                strings.ToLower(strings.TrimSpace(in.Chain)),
		Address:              strings.TrimSpace(in.Address),
		AssetContractAddress: strings.TrimSpace(in.AssetContractAddress),
		OnchainBalanceAmount: strings.TrimSpace(in.OnchainBalanceAmount),
		LedgerBalanceAmount:  strings.TrimSpace(in.LedgerBalanceAmount),
		BalanceDiffAmount:    strings.TrimSpace(in.BalanceDiffAmount),
		ReconciliationStatus: strings.ToUpper(strings.TrimSpace(in.ReconciliationStatus)),
		HasMismatch:          in.HasMismatch,
		LastErrorMessage:     strings.TrimSpace(in.LastErrorMessage),
		ReconciledAt:         in.ReconciledAt,
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.
			Clauses(clause.OnConflict{
				Columns: []clause.Column{
					{Name: "scope"},
					{Name: "chain"},
					{Name: "address"},
					{Name: "asset_contract_address"},
				},
				DoUpdates: clause.AssignmentColumns([]string{
					"user_id",
					"onchain_balance_amount",
					"ledger_balance_amount",
					"balance_diff_amount",
					"reconciliation_status",
					"has_mismatch",
					"last_error_message",
					"reconciled_at",
					"updated_at",
				}),
			}).
			Create(&rec).Error; err != nil {
			return err
		}
		return tx.Create(&logRec).Error
	})
}

func (r *ReconcileRepo) UpsertBusinessFlowReconciliation(ctx context.Context, in FlowReconciliationUpsertInput) error {
	if r == nil || r.db == nil {
		return errors.New("reconcile repo not configured")
	}
	rec := reconcilemodel.BusinessFlowReconciliation{
		FlowSource:           strings.ToUpper(strings.TrimSpace(in.FlowSource)),
		BusinessType:         strings.ToUpper(strings.TrimSpace(in.BusinessType)),
		BusinessID:           strings.TrimSpace(in.BusinessID),
		UserID:               strings.TrimSpace(in.UserID),
		Chain:                strings.ToLower(strings.TrimSpace(in.Chain)),
		AssetContractAddress: strings.TrimSpace(in.AssetContractAddress),
		ExpectedChangeAmount: strings.TrimSpace(in.ExpectedChangeAmount),
		ActualChangeAmount:   strings.TrimSpace(in.ActualChangeAmount),
		ReconciliationStatus: strings.ToUpper(strings.TrimSpace(in.ReconciliationStatus)),
		HasMismatch:          in.HasMismatch,
		LastErrorMessage:     strings.TrimSpace(in.LastErrorMessage),
		ReconciledAt:         &in.ReconciledAt,
	}
	logRec := reconcilemodel.BusinessFlowReconciliationLog{
		FlowSource:           strings.ToUpper(strings.TrimSpace(in.FlowSource)),
		BusinessType:         strings.ToUpper(strings.TrimSpace(in.BusinessType)),
		BusinessID:           strings.TrimSpace(in.BusinessID),
		UserID:               strings.TrimSpace(in.UserID),
		Chain:                strings.ToLower(strings.TrimSpace(in.Chain)),
		AssetContractAddress: strings.TrimSpace(in.AssetContractAddress),
		ExpectedChangeAmount: strings.TrimSpace(in.ExpectedChangeAmount),
		ActualChangeAmount:   strings.TrimSpace(in.ActualChangeAmount),
		ReconciliationStatus: strings.ToUpper(strings.TrimSpace(in.ReconciliationStatus)),
		HasMismatch:          in.HasMismatch,
		LastErrorMessage:     strings.TrimSpace(in.LastErrorMessage),
		ReconciledAt:         in.ReconciledAt,
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.
			Clauses(clause.OnConflict{
				Columns: []clause.Column{
					{Name: "flow_source"},
					{Name: "business_type"},
					{Name: "business_id"},
				},
				DoUpdates: clause.AssignmentColumns([]string{
					"user_id",
					"chain",
					"asset_contract_address",
					"expected_change_amount",
					"actual_change_amount",
					"reconciliation_status",
					"has_mismatch",
					"last_error_message",
					"reconciled_at",
					"updated_at",
				}),
			}).
			Create(&rec).Error; err != nil {
			return err
		}
		return tx.Create(&logRec).Error
	})
}
