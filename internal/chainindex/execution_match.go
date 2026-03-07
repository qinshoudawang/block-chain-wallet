package chainindex

import (
	"strings"

	chainmodel "wallet-system/internal/storage/model/chain"
)

type executionMatchTarget interface {
	GetSourceAddress() string
	GetTargetAddress() string
	GetAssetContractAddress() string
	GetAmount() string
}

func matchesExecutionTarget(target executionMatchTarget, ev chainmodel.ChainEvent) bool {
	if !strings.EqualFold(strings.TrimSpace(target.GetSourceAddress()), strings.TrimSpace(ev.FromAddress)) {
		return false
	}
	if to := strings.TrimSpace(target.GetTargetAddress()); to != "" && !strings.EqualFold(to, strings.TrimSpace(ev.ToAddress)) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(target.GetAssetContractAddress()), strings.TrimSpace(ev.AssetContractAddress)) {
		return false
	}
	if ev.Success && strings.TrimSpace(target.GetAmount()) != strings.TrimSpace(ev.Amount) {
		return false
	}
	return true
}
