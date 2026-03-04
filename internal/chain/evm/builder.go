package evm

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// 构造一个“未签名”的 EIP-1559 交易，并返回 RLP bytes
func (c *Client) BuildUnsignedTx(
	ctx context.Context,
	from common.Address,
	to common.Address,
	value *big.Int,
	data []byte,
	chainID *big.Int,
	nonce uint64,
) ([]byte, error) {
	if c == nil || c.cli == nil {
		return nil, ErrClientNotConfigured
	}

	tipCap, feeCap, err := c.SuggestDynamicFeeCaps(ctx)
	if err != nil {
		return nil, err
	}

	// 估 gas
	msg := ethereumCallMsg(from, to, value, data, feeCap, tipCap)
	gasLimit, err := c.cli.EstimateGas(ctx, msg)
	if err != nil {
		return nil, err
	}

	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   chainID,
		Nonce:     nonce,
		GasTipCap: tipCap,
		GasFeeCap: feeCap,
		Gas:       gasLimit,
		To:        &to,
		Value:     value,
		Data:      data,
	})

	return tx.MarshalBinary() // RLP 编码
}

func ethereumCallMsg(from, to common.Address, value *big.Int, data []byte, feeCap, tipCap *big.Int) ethereum.CallMsg {
	return ethereum.CallMsg{
		From:      from,
		To:        &to,
		Value:     value,
		Data:      data,
		GasFeeCap: feeCap,
		GasTipCap: tipCap,
	}
}
