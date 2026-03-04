package chainclient

import (
	"context"
	"encoding/hex"
	"errors"
	"math/big"
	"strings"

	"wallet-system/internal/chain/evm"
	"wallet-system/internal/helpers"
	"wallet-system/internal/sequence/nonce"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/redis/go-redis/v9"
)

type evmClient struct {
	evm *evm.Client
}

type evmRBFContext struct {
	Tx          *ethtypes.Transaction
	From        common.Address
	To          common.Address
	Amount      *big.Int
	MinDeltaWei *big.Int
	ChainID     *big.Int
	GasLimit    uint64
}

func newEVMClient(cli *evm.Client) Client {
	return &evmClient{evm: cli}
}

func (c *evmClient) ValidateWithdrawInput(chain string, to string, amount string) (string, *big.Int, error) {
	addr := common.HexToAddress(to)
	if addr == (common.Address{}) {
		return "", nil, errors.New("invalid to address")
	}
	amt := new(big.Int)
	if _, ok := amt.SetString(amount, 10); !ok || amt.Sign() <= 0 {
		return "", nil, errors.New("invalid amount")
	}
	return addr.Hex(), amt, nil
}

func (c *evmClient) AllocateSequence(ctx context.Context, redisClient *redis.Client, rt *Runtime, sequenceFloorProvider SequenceFloorProvider) (uint64, error) {
	if c == nil || c.evm == nil {
		return 0, errors.New("evm client is required")
	}
	if redisClient == nil {
		return 0, errors.New("redis is required")
	}
	if sequenceFloorProvider == nil {
		return 0, errors.New("sequence floor provider is required")
	}
	if rt == nil {
		return 0, errors.New("runtime is required")
	}
	if !common.IsHexAddress(rt.FromAddress) {
		return 0, errors.New("invalid evm from address")
	}
	from := common.HexToAddress(rt.FromAddress)

	nm := nonce.NewManager(redisClient, rt.Chain, from.Hex(), func(ctx context.Context) (uint64, error) {
		return c.evm.PendingNonceAt(ctx, from)
	})
	nm.WithNonceFloorProvider(sequenceFloorProvider)
	if err := nm.EnsureInitialized(ctx); err != nil {
		return 0, err
	}
	return nm.Allocate(ctx)
}

func (c *evmClient) BuildUnsignedWithdrawTx(
	ctx context.Context,
	rt Runtime,
	toAddr string,
	amount *big.Int,
	nonce uint64,
) ([]byte, error) {
	if c == nil || c.evm == nil {
		return nil, errors.New("evm client is required")
	}
	if rt.ChainID == nil {
		return nil, errors.New("chain id is required")
	}
	if !common.IsHexAddress(rt.FromAddress) {
		return nil, errors.New("invalid evm from address")
	}
	if !common.IsHexAddress(toAddr) {
		return nil, errors.New("invalid evm to address")
	}
	if amount == nil || amount.Sign() <= 0 {
		return nil, errors.New("invalid evm amount")
	}
	from := common.HexToAddress(rt.FromAddress)
	to := common.HexToAddress(toAddr)
	return c.evm.BuildUnsignedTx(ctx, from, to, amount, nil, rt.ChainID, nonce)
}

func (c *evmClient) BuildRBFUnsignedWithdrawTx(
	ctx context.Context,
	chain string,
	fromAddr string,
	toAddr string,
	amount string,
	signedPayload string,
) (*RBFUnsignedBuildResult, error) {
	if c == nil || c.evm == nil {
		return nil, errors.New("evm client is required")
	}
	rbfCtx, err := buildEVMRBFContext(chain, fromAddr, toAddr, amount, signedPayload)
	if err != nil {
		return nil, err
	}
	switch rbfCtx.Tx.Type() {
	case ethtypes.DynamicFeeTxType:
		return c.buildEIP1559RBF(ctx, rbfCtx)
	case ethtypes.LegacyTxType, ethtypes.AccessListTxType:
		return c.buildLegacyLikeRBF(ctx, rbfCtx)
	default:
		return nil, errors.New("unsupported evm tx type for rbf")
	}
}

func buildEVMRBFContext(
	chain string,
	fromAddr string,
	toAddr string,
	amount string,
	signedPayload string,
) (*evmRBFContext, error) {
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return nil, err
	}
	if spec.Family != helpers.FamilyEVM {
		return nil, errors.New("invalid evm chain for rbf")
	}
	if !common.IsHexAddress(fromAddr) {
		return nil, errors.New("invalid evm from address")
	}
	if !common.IsHexAddress(toAddr) {
		return nil, errors.New("invalid evm to address")
	}
	amt := new(big.Int)
	if _, ok := amt.SetString(strings.TrimSpace(amount), 10); !ok || amt.Sign() <= 0 {
		return nil, errors.New("invalid evm amount")
	}
	tx, err := decodeSignedEVMTx(signedPayload)
	if err != nil {
		return nil, err
	}
	txChainID := tx.ChainId()
	if txChainID == nil || txChainID.Sign() <= 0 {
		return nil, errors.New("invalid evm tx chain id")
	}
	if tx.To() == nil {
		return nil, errors.New("evm contract creation rbf is not supported")
	}
	from := common.HexToAddress(fromAddr)
	to := common.HexToAddress(toAddr)
	if *tx.To() != to {
		return nil, errors.New("evm tx to mismatch")
	}
	if tx.Value().Cmp(amt) != 0 {
		return nil, errors.New("evm tx amount mismatch")
	}
	if err := ensureEVMTxSender(tx, from); err != nil {
		return nil, err
	}
	minDeltaWei := normalizeEVMMinDeltaWei()
	return &evmRBFContext{
		Tx:          tx,
		From:        from,
		To:          to,
		Amount:      amt,
		MinDeltaWei: minDeltaWei,
		ChainID:     txChainID,
		GasLimit:    tx.Gas(),
	}, nil
}

func decodeSignedEVMTx(signedPayload string) (*ethtypes.Transaction, error) {
	raw, err := hex.DecodeString(strings.TrimSpace(strings.TrimPrefix(signedPayload, "0x")))
	if err != nil {
		return nil, errors.New("invalid evm signed tx payload")
	}
	var tx ethtypes.Transaction
	if err := tx.UnmarshalBinary(raw); err != nil {
		return nil, errors.New("invalid evm signed tx payload")
	}
	return &tx, nil
}

func ensureEVMTxSender(tx *ethtypes.Transaction, expected common.Address) error {
	if tx == nil {
		return errors.New("invalid evm tx")
	}
	sender, err := ethtypes.Sender(ethtypes.LatestSignerForChainID(tx.ChainId()), tx)
	if err != nil {
		return errors.New("invalid evm signed tx payload")
	}
	if sender != expected {
		return errors.New("evm tx from mismatch")
	}
	return nil
}

func normalizeEVMMinDeltaWei() *big.Int {
	return big.NewInt(1)
}

func (c *evmClient) buildEIP1559RBF(ctx context.Context, rbfCtx *evmRBFContext) (*RBFUnsignedBuildResult, error) {
	tx := rbfCtx.Tx
	oldTip := tx.GasTipCap()
	oldFeeCap := tx.GasFeeCap()
	if oldTip == nil || oldFeeCap == nil || oldTip.Sign() <= 0 || oldFeeCap.Sign() <= 0 {
		return nil, errors.New("invalid evm 1559 fee fields")
	}
	netTip, netFeeCap, err := c.evm.SuggestDynamicFeeCaps(ctx)
	if err != nil {
		return nil, err
	}
	newTip := maxBigInt(netTip, new(big.Int).Add(oldTip, rbfCtx.MinDeltaWei))
	newFeeCap := maxBigInt(netFeeCap, new(big.Int).Add(oldFeeCap, rbfCtx.MinDeltaWei))
	newFeeCap = maxBigInt(newFeeCap, newTip)
	replacement := ethtypes.NewTx(&ethtypes.DynamicFeeTx{
		ChainID:    rbfCtx.ChainID,
		Nonce:      tx.Nonce(),
		GasTipCap:  newTip,
		GasFeeCap:  newFeeCap,
		Gas:        rbfCtx.GasLimit,
		To:         &rbfCtx.To,
		Value:      new(big.Int).Set(rbfCtx.Amount),
		Data:       append([]byte(nil), tx.Data()...),
		AccessList: tx.AccessList(),
	})
	raw, err := replacement.MarshalBinary()
	if err != nil {
		return nil, err
	}
	oldFee, err := mulToInt64(tx.Gas(), oldFeeCap)
	if err != nil {
		return nil, err
	}
	newFee, err := mulToInt64(tx.Gas(), newFeeCap)
	if err != nil {
		return nil, err
	}
	oldRate, err := bigToInt64(oldFeeCap)
	if err != nil {
		return nil, err
	}
	newRate, err := bigToInt64(newFeeCap)
	if err != nil {
		return nil, err
	}
	return &RBFUnsignedBuildResult{
		UnsignedPayload: raw,
		OldFeeRate:      oldRate,
		NewFeeRate:      newRate,
		OldFee:          oldFee,
		NewFee:          newFee,
	}, nil
}

func (c *evmClient) buildLegacyLikeRBF(ctx context.Context, rbfCtx *evmRBFContext) (*RBFUnsignedBuildResult, error) {
	tx := rbfCtx.Tx
	oldGasPrice := tx.GasPrice()
	if oldGasPrice == nil || oldGasPrice.Sign() <= 0 {
		return nil, errors.New("invalid evm gas price")
	}
	netGasPrice, err := c.evm.SuggestGasPrice(ctx)
	if err != nil {
		return nil, err
	}
	newGasPrice := maxBigInt(netGasPrice, new(big.Int).Add(oldGasPrice, rbfCtx.MinDeltaWei))
	var replacement *ethtypes.Transaction
	switch tx.Type() {
	case ethtypes.LegacyTxType:
		replacement = ethtypes.NewTx(&ethtypes.LegacyTx{
			Nonce:    tx.Nonce(),
			GasPrice: newGasPrice,
			Gas:      rbfCtx.GasLimit,
			To:       &rbfCtx.To,
			Value:    new(big.Int).Set(rbfCtx.Amount),
			Data:     append([]byte(nil), tx.Data()...),
		})
	case ethtypes.AccessListTxType:
		replacement = ethtypes.NewTx(&ethtypes.AccessListTx{
			ChainID:    rbfCtx.ChainID,
			Nonce:      tx.Nonce(),
			GasPrice:   newGasPrice,
			Gas:        rbfCtx.GasLimit,
			To:         &rbfCtx.To,
			Value:      new(big.Int).Set(rbfCtx.Amount),
			Data:       append([]byte(nil), tx.Data()...),
			AccessList: tx.AccessList(),
		})
	default:
		return nil, errors.New("unsupported evm tx type for rbf")
	}
	raw, err := replacement.MarshalBinary()
	if err != nil {
		return nil, err
	}
	oldFee, err := mulToInt64(tx.Gas(), oldGasPrice)
	if err != nil {
		return nil, err
	}
	newFee, err := mulToInt64(tx.Gas(), newGasPrice)
	if err != nil {
		return nil, err
	}
	oldRate, err := bigToInt64(oldGasPrice)
	if err != nil {
		return nil, err
	}
	newRate, err := bigToInt64(newGasPrice)
	if err != nil {
		return nil, err
	}
	return &RBFUnsignedBuildResult{
		UnsignedPayload: raw,
		OldFeeRate:      oldRate,
		NewFeeRate:      newRate,
		OldFee:          oldFee,
		NewFee:          newFee,
	}, nil
}

func maxBigInt(a *big.Int, b *big.Int) *big.Int {
	switch {
	case a == nil && b == nil:
		return big.NewInt(0)
	case a == nil:
		return new(big.Int).Set(b)
	case b == nil:
		return new(big.Int).Set(a)
	case a.Cmp(b) >= 0:
		return new(big.Int).Set(a)
	default:
		return new(big.Int).Set(b)
	}
}

func bigToInt64(v *big.Int) (int64, error) {
	if v == nil || v.Sign() < 0 || !v.IsInt64() {
		return 0, errors.New("evm fee is out of int64 range")
	}
	return v.Int64(), nil
}

func mulToInt64(gas uint64, price *big.Int) (int64, error) {
	if price == nil || price.Sign() < 0 {
		return 0, errors.New("invalid evm fee component")
	}
	fee := new(big.Int).Mul(new(big.Int).SetUint64(gas), price)
	if !fee.IsInt64() {
		return 0, errors.New("evm fee is out of int64 range")
	}
	return fee.Int64(), nil
}
