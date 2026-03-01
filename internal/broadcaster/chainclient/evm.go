package chainclient

import (
	"context"
	"encoding/hex"
	"errors"
	"math/big"
	"strings"

	"wallet-system/internal/chain/evm"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
)

type evmClient struct {
	client *evm.Client
}

func NewEVMClient(client *evm.Client) Client {
	return &evmClient{client: client}
}

func (c *evmClient) BroadcastSignedTxHex(ctx context.Context, signedTxHex string) (string, error) {
	if c == nil || c.client == nil {
		return "", errors.New("evm client is required")
	}
	raw, err := hex.DecodeString(strings.TrimPrefix(signedTxHex, "0x"))
	if err != nil {
		return "", err
	}
	return c.client.Broadcast(ctx, raw)
}

func (c *evmClient) GetLatestHeight(ctx context.Context) (uint64, error) {
	if c == nil || c.client == nil {
		return 0, errors.New("evm client is required")
	}
	return c.client.LatestHeight(ctx)
}

func (c *evmClient) GetConfirmation(ctx context.Context, txHash string, amount string, latestHeight uint64) (*Confirmation, error) {
	if c == nil || c.client == nil {
		return nil, errors.New("evm client is required")
	}
	receipt, err := c.client.TransactionReceipt(ctx, common.HexToHash(txHash))
	if err != nil {
		// Not found / not indexed yet: caller treats as pending.
		return nil, nil
	}

	bn := receipt.BlockNumber.Uint64()
	conf := int(latestHeight - bn + 1)
	settlement, err := calcSettlementWei(amount, receipt)
	if err != nil {
		return nil, err
	}

	return &Confirmation{
		BlockNumber:   bn,
		Confirmations: conf,
		Settlement: &Settlement{
			GasUsed:        receipt.GasUsed,
			GasPriceWei:    settlement.GasPriceWei,
			GasFeeWei:      settlement.GasFeeWei,
			ActualSpentWei: settlement.ActualSpentWei,
		},
	}, nil
}

type evmSettlement struct {
	GasPriceWei    *big.Int
	GasFeeWei      *big.Int
	ActualSpentWei *big.Int
}

func calcSettlementWei(amountWei string, receipt *ethtypes.Receipt) (*evmSettlement, error) {
	amount := new(big.Int)
	if _, ok := amount.SetString(amountWei, 10); !ok || amount.Sign() < 0 {
		return nil, errInvalidAmountWei
	}
	gasPriceWei := receipt.EffectiveGasPrice
	if gasPriceWei == nil {
		gasPriceWei = big.NewInt(0)
	}
	gasFeeWei := new(big.Int).Mul(new(big.Int).SetUint64(receipt.GasUsed), gasPriceWei)
	actualSpentWei := new(big.Int).Add(amount, gasFeeWei)
	return &evmSettlement{
		GasPriceWei:    gasPriceWei,
		GasFeeWei:      gasFeeWei,
		ActualSpentWei: actualSpentWei,
	}, nil
}

var errInvalidAmountWei = invalidAmountWeiError{}

type invalidAmountWeiError struct{}

func (invalidAmountWeiError) Error() string { return "invalid amount wei" }
