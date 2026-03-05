package chainclient

import (
	"context"
	"encoding/hex"
	"errors"
	"math/big"
	"strings"

	solchain "wallet-system/internal/chain/sol"
)

type solanaClient struct {
	sol *solchain.Client
}

func NewSOLClient(rpc *solchain.Client) Client { return &solanaClient{sol: rpc} }

func (c *solanaClient) BroadcastSignedTxHex(ctx context.Context, signedTxHex string) (string, error) {
	if c == nil || c.sol == nil {
		return "", errors.New("solana rpc client is required")
	}
	raw, err := hex.DecodeString(strings.TrimSpace(strings.TrimPrefix(signedTxHex, "0x")))
	if err != nil {
		return "", errors.New("invalid solana signed tx")
	}
	return c.sol.Broadcast(ctx, raw)
}

func (c *solanaClient) GetLatestHeight(ctx context.Context) (uint64, error) {
	if c == nil || c.sol == nil {
		return 0, errors.New("solana rpc client is required")
	}
	return c.sol.LatestHeight(ctx)
}

func (c *solanaClient) GetConfirmation(ctx context.Context, txHash string, amount string, tokenContractAddress string, latestHeight uint64) (*Confirmation, error) {
	_ = tokenContractAddress
	if c == nil || c.sol == nil {
		return nil, errors.New("solana rpc client is required")
	}
	status, err := c.sol.GetSignatureStatus(ctx, txHash)
	if err != nil {
		return nil, err
	}
	if status == nil {
		return nil, nil
	}
	if status.Err != nil {
		return nil, errors.New("solana transaction failed")
	}

	conf := 0
	if status.Confirmations != nil {
		conf = int(*status.Confirmations)
	} else if status.Slot > 0 && latestHeight >= status.Slot {
		conf = int(latestHeight-status.Slot) + 1
	}
	if conf <= 0 {
		switch status.ConfirmationStatus {
		case "confirmed", "finalized":
			conf = 1
		default:
			return nil, nil
		}
	}

	fee, err := c.sol.GetTransactionFee(ctx, txHash)
	if err != nil {
		return nil, err
	}
	if fee == nil {
		fee = big.NewInt(0)
	}
	amt := new(big.Int)
	if _, ok := amt.SetString(strings.TrimSpace(amount), 10); !ok || amt.Sign() < 0 {
		return nil, errors.New("invalid amount")
	}

	return &Confirmation{
		BlockNumber:   status.Slot,
		Confirmations: conf,
		Settlement: &Settlement{
			TransferAssetContractAddress:   "",
			TransferSpentAmount:            amt,
			NetworkFeeAssetContractAddress: "",
			NetworkFeeAmount:               fee,
		},
	}, nil
}
