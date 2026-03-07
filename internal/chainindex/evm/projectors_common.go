package evmindex

import (
	"context"
	"log"

	evmchain "wallet-system/internal/chain/evm"
	chainmodel "wallet-system/internal/storage/model/chain"
	"wallet-system/internal/storage/repo"
)

func finalizedHeight(ctx context.Context, evm *evmchain.Client, confirmations uint64) (uint64, bool) {
	if evm == nil {
		return 0, false
	}
	latest, err := evm.LatestHeight(ctx)
	if err != nil {
		log.Printf("[evm-tx-projector] latest height failed err=%v", err)
		return 0, false
	}
	if latest < confirmations {
		return 0, false
	}
	return latest - confirmations, true
}

func chainEventToModel(in repo.ChainEventInput) chainmodel.ChainEvent {
	amount := "0"
	if in.Amount != nil {
		amount = in.Amount.String()
	}
	fee := "0"
	if in.FeeAmount != nil {
		fee = in.FeeAmount.String()
	}
	return chainmodel.ChainEvent{
		Chain:                   in.Chain,
		EventType:               in.EventType,
		Action:                  chainmodel.EventActionApply,
		BlockNumber:             in.BlockNumber,
		BlockHash:               in.BlockHash,
		TxHash:                  in.TxHash,
		TxIndex:                 in.TxIndex,
		LogIndex:                in.LogIndex,
		AssetContractAddress:    in.AssetContractAddress,
		FromAddress:             in.FromAddress,
		ToAddress:               in.ToAddress,
		Amount:                  amount,
		FeeAssetContractAddress: in.FeeAssetContractAddress,
		FeeAmount:               fee,
		Success:                 in.Success,
	}
}
