package evmindex

import (
	sweepmodel "wallet-system/internal/storage/model/sweep"
	withdrawmodel "wallet-system/internal/storage/model/withdraw"
)

type withdrawExecutionTarget struct {
	rec withdrawmodel.WithdrawOrder
}

func (t withdrawExecutionTarget) GetSourceAddress() string        { return t.rec.FromAddr }
func (t withdrawExecutionTarget) GetTargetAddress() string        { return t.rec.ToAddr }
func (t withdrawExecutionTarget) GetAssetContractAddress() string { return t.rec.TokenContractAddress }
func (t withdrawExecutionTarget) GetAmount() string               { return t.rec.Amount }

type sweepExecutionTarget struct {
	rec sweepmodel.SweepOrder
}

func (t sweepExecutionTarget) GetSourceAddress() string        { return t.rec.FromAddress }
func (t sweepExecutionTarget) GetTargetAddress() string        { return t.rec.ToAddress }
func (t sweepExecutionTarget) GetAssetContractAddress() string { return t.rec.AssetContractAddress }
func (t sweepExecutionTarget) GetAmount() string               { return t.rec.Amount }
