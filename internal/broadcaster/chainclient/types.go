package chainclient

import (
	"context"
	"math/big"
)

type Settlement struct {
	TransferAssetContractAddress   string
	TransferSpentAmount            *big.Int
	NetworkFeeAssetContractAddress string
	NetworkFeeAmount               *big.Int
}

type Confirmation struct {
	BlockNumber   uint64
	Confirmations int
	Settlement    *Settlement
}

type Client interface {
	BroadcastSignedTxHex(ctx context.Context, signedTxHex string) (string, error)
	GetLatestHeight(ctx context.Context) (uint64, error)
	GetConfirmation(ctx context.Context, txHash string, amount string, latestHeight uint64) (*Confirmation, error)
}
