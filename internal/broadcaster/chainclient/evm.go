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
	sender *evm.EVMSender
}

func NewEVMClient(sender *evm.EVMSender) Client {
	return &evmClient{sender: sender}
}

func (c *evmClient) BroadcastSignedTxHex(ctx context.Context, signedTxHex string) (string, error) {
	if c == nil || c.sender == nil {
		return "", errors.New("evm sender is required")
	}
	raw, err := hex.DecodeString(strings.TrimPrefix(signedTxHex, "0x"))
	if err != nil {
		return "", err
	}
	return c.sender.Broadcast(ctx, raw)
}

func (c *evmClient) GetLatestHeight(ctx context.Context) (uint64, error) {
	if c == nil || c.sender == nil || c.sender.EthClient() == nil {
		return 0, errors.New("evm eth client is required")
	}
	eth := c.sender.EthClient()
	return eth.BlockNumber(ctx)
}

func (c *evmClient) GetConfirmation(ctx context.Context, txHash string, amount string, latestHeight uint64) (*Confirmation, error) {
	if c == nil || c.sender == nil || c.sender.EthClient() == nil {
		return nil, errors.New("evm eth client is required")
	}
	eth := c.sender.EthClient()

	receipt, err := eth.TransactionReceipt(ctx, common.HexToHash(txHash))
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
