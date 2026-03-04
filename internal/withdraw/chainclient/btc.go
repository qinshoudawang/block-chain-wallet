package chainclient

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"wallet-system/internal/chain/btc"
	"wallet-system/internal/helpers"
	"wallet-system/internal/sequence/nonce"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/redis/go-redis/v9"
)

type btcClient struct {
	cli *btc.Client
}

type btcUnsignedWithdrawTx struct {
	RawTx    string                  `json:"raw_tx"`
	Prevouts []btc.UnsignedTxPrevout `json:"prevouts"`
}

type btcRBFContext struct {
	Tx             *wire.MsgTx
	FromScript     []byte
	ToScript       []byte
	TransferAmount int64
}

type btcRBFFeePlan struct {
	OldFeeRate int64
	NewFeeRate int64
	OldFee     int64
	NewFee     int64
	DeltaFee   int64
}

const btcChangeDustLimitSat int64 = 546

func newBTCClientWithRPC(rpc *btc.Client) Client {
	return &btcClient{cli: rpc}
}

func (c *btcClient) ValidateWithdrawInput(chain string, to string, amount string) (string, *big.Int, error) {
	addr, err := decodeBTCAddressByChain(chain, to)
	if err != nil {
		return "", nil, err
	}
	amt := new(big.Int)
	if _, ok := amt.SetString(amount, 10); !ok || amt.Sign() <= 0 {
		return "", nil, errors.New("invalid amount")
	}
	if amt.Cmp(big.NewInt(math.MaxInt64)) > 0 {
		return "", nil, errors.New("btc amount too large")
	}
	return addr.EncodeAddress(), amt, nil
}

func (c *btcClient) AllocateSequence(
	ctx context.Context,
	redisClient *redis.Client,
	rt *Runtime,
	sequenceFloorProvider SequenceFloorProvider,
) (uint64, error) {
	if redisClient == nil {
		return 0, errors.New("redis is required")
	}
	if rt == nil {
		return 0, errors.New("runtime is required")
	}
	if sequenceFloorProvider == nil {
		return 0, errors.New("sequence floor provider is required")
	}
	account := strings.ToLower(strings.TrimSpace(rt.FromAddress))
	if account == "" {
		return 0, errors.New("invalid btc from address")
	}
	nm := nonce.NewManager(redisClient, rt.Chain, account, sequenceFloorProvider)
	if err := nm.EnsureInitialized(ctx); err != nil {
		return 0, err
	}
	sequence, err := nm.Allocate(ctx)
	if err != nil {
		return 0, err
	}
	if rt.UTXOReserve != nil {
		excluded, err := rt.UTXOReserve.ListReservedForAccount(ctx, rt.Chain, rt.FromAddress)
		if err != nil {
			return 0, err
		}
		rt.ExcludedUTXOKeys = excluded
		if strings.TrimSpace(rt.WithdrawID) != "" {
			rt.ReserveUTXO = func(ctx context.Context, utxoKeys []string) error {
				return rt.UTXOReserve.ReserveByWithdrawID(ctx, rt.Chain, rt.FromAddress, rt.WithdrawID, utxoKeys)
			}
		}
	}
	return sequence, nil
}

func (c *btcClient) BuildUnsignedWithdrawTx(
	ctx context.Context,
	rt Runtime,
	toAddr string,
	amount *big.Int,
	sequence uint64,
) ([]byte, error) {
	_ = sequence

	// 1) 解析并校验出入账地址（按链区分主网/测试网）。
	fromAddr, err := decodeBTCAddressByChain(rt.Chain, rt.FromAddress)
	if err != nil {
		return nil, errors.New("invalid btc from address")
	}
	toParsed, err := decodeBTCAddressByChain(rt.Chain, toAddr)
	if err != nil {
		return nil, errors.New("invalid btc to address")
	}

	// 2) 拉取热钱包可花费 UTXO，并过滤掉无效 value。
	utxos, err := c.cli.ListUnspentByAddress(ctx, fromAddr, normalizeBTCMinConf(rt.MinConf))
	if err != nil {
		return nil, err
	}
	if len(utxos) == 0 {
		return nil, errors.New("no spendable btc utxo")
	}
	spendable := make([]btc.UTXO, 0, len(utxos))
	for _, u := range utxos {
		if u.ValueSat <= 0 {
			continue
		}
		if _, reserved := rt.ExcludedUTXOKeys[formatBTCUTXOKey(u.TxID, u.Vout)]; reserved {
			continue
		}
		spendable = append(spendable, u)
	}
	if len(spendable) == 0 {
		return nil, errors.New("no spendable btc utxo")
	}

	// 3) 基于费率选择输入，计算 change。
	feeRate := c.resolveFeeRate(rt)
	selected, change, err := selectBTCUTXOs(spendable, amount, feeRate)
	if err != nil {
		return nil, err
	}
	if amount == nil || amount.Sign() <= 0 || amount.Cmp(big.NewInt(math.MaxInt64)) > 0 {
		return nil, errors.New("invalid amount")
	}
	if rt.ReserveUTXO != nil {
		utxoKeys := make([]string, 0, len(selected))
		for _, u := range selected {
			utxoKeys = append(utxoKeys, formatBTCUTXOKey(u.TxID, u.Vout))
		}
		if err := rt.ReserveUTXO(ctx, utxoKeys); err != nil {
			return nil, err
		}
	}

	// 4) 构建 unsigned raw tx，并附带 signer 需要的 prevout 元数据。
	rawTxHex, prevouts, err := c.cli.BuildUnsignedWithdrawTx(fromAddr, toParsed, selected, amount.Int64(), change.Int64())
	if err != nil {
		return nil, err
	}

	// 5) 输出给 signer 的统一 payload（raw_tx + prevouts）。
	return marshalBTCUnsignedPayload(rawTxHex, prevouts)
}

func (c *btcClient) BuildRBFUnsignedWithdrawTx(
	ctx context.Context,
	chain string,
	fromAddr string,
	toAddr string,
	amount string,
	signedPayload string,
	feeTargetBlocks int64,
	minDeltaSatPerVByte int64,
) (*RBFUnsignedBuildResult, error) {
	if c == nil || c.cli == nil {
		return nil, errors.New("btc client is required")
	}
	// 1) 解析原交易并校验 RBF 前置条件（链类型、地址脚本、金额、replaceable）。
	rbfCtx, err := buildBTCRBFContext(chain, fromAddr, toAddr, amount, signedPayload)
	if err != nil {
		return nil, err
	}
	tx := rbfCtx.Tx
	// 2) 回查每个输入对应的 prevout，得到输入总额与 signer 所需元数据。
	totalInput, prevouts, err := c.loadPrevoutsForRBF(ctx, tx)
	if err != nil {
		return nil, err
	}
	// 3) 统计原交易输出总额，用于后续 old fee 计算。
	totalOutput, err := sumWireOutputs(tx)
	if err != nil {
		return nil, err
	}
	// 4) 计算提费方案：old/new feeRate、old/new fee、deltaFee。
	feePlan, err := buildBTCRBFFeePlan(c.cli, tx, totalInput, totalOutput, feeTargetBlocks, minDeltaSatPerVByte)
	if err != nil {
		return nil, err
	}
	// 5) RBF 只通过减少找零来提费，收款输出保持不变。
	changeIdx := findChangeOutputIndex(tx, rbfCtx.FromScript, rbfCtx.ToScript, rbfCtx.TransferAmount)
	if changeIdx < 0 {
		return nil, errors.New("btc rbf requires change output")
	}
	newChange := tx.TxOut[changeIdx].Value - feePlan.DeltaFee
	if newChange < btcChangeDustLimitSat {
		return nil, errors.New("insufficient btc change for rbf fee bump")
	}
	// 6) 构造 replacement tx：输入集合与序列号保持，输出仅替换 change 数值。
	replacement := wire.NewMsgTx(tx.Version)
	replacement.LockTime = tx.LockTime
	for i := range tx.TxIn {
		in := tx.TxIn[i]
		replacement.AddTxIn(&wire.TxIn{PreviousOutPoint: in.PreviousOutPoint, Sequence: in.Sequence})
	}
	for i := range tx.TxOut {
		out := tx.TxOut[i]
		value := out.Value
		if i == changeIdx {
			value = newChange
		}
		replacement.AddTxOut(&wire.TxOut{Value: value, PkScript: append([]byte(nil), out.PkScript...)})
	}
	rawReplacement, err := serializeWireTxHex(replacement)
	if err != nil {
		return nil, err
	}
	// 7) 输出 signer 统一格式（raw_tx + prevouts）。
	unsignedPayload, err := marshalBTCUnsignedPayload(rawReplacement, prevouts)
	if err != nil {
		return nil, err
	}
	return &RBFUnsignedBuildResult{
		UnsignedPayload: unsignedPayload,
		OldFeeRate:      feePlan.OldFeeRate,
		NewFeeRate:      feePlan.NewFeeRate,
		OldFee:          feePlan.OldFee,
		NewFee:          feePlan.NewFee,
	}, nil
}

func (c *btcClient) loadPrevoutsForRBF(ctx context.Context, tx *wire.MsgTx) (int64, []btc.UnsignedTxPrevout, error) {
	prevouts := make([]btc.UnsignedTxPrevout, 0, len(tx.TxIn))
	cache := make(map[string]*btcjson.TxRawResult, len(tx.TxIn))
	var totalInput int64
	for i := range tx.TxIn {
		select {
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		default:
		}
		in := tx.TxIn[i]
		txid := in.PreviousOutPoint.Hash.String()
		prevTx, ok := cache[txid]
		if !ok {
			var err error
			prevTx, err = c.cli.GetRawTransactionVerbose(txid)
			if err != nil || prevTx == nil {
				return 0, nil, errors.New("failed to load btc prev tx")
			}
			cache[txid] = prevTx
		}
		voutIdx := int(in.PreviousOutPoint.Index)
		if voutIdx < 0 || voutIdx >= len(prevTx.Vout) {
			return 0, nil, errors.New("invalid btc vin vout")
		}
		prevOut := prevTx.Vout[voutIdx]
		valueSat, err := btcFloatToSat(prevOut.Value)
		if err != nil || valueSat <= 0 {
			return 0, nil, errors.New("invalid btc prevout value")
		}
		pkScriptHex := strings.TrimSpace(prevOut.ScriptPubKey.Hex)
		if pkScriptHex == "" {
			return 0, nil, errors.New("invalid btc prevout pk_script")
		}
		totalInput += valueSat
		prevouts = append(prevouts, btc.UnsignedTxPrevout{
			Value:    strconv.FormatInt(valueSat, 10),
			PkScript: pkScriptHex,
		})
	}
	return totalInput, prevouts, nil
}

func payToAddressScriptByChain(spec helpers.ChainSpec, addr string) ([]byte, error) {
	params := &chaincfg.MainNetParams
	if spec.IsTestnet {
		params = &chaincfg.TestNet3Params
	}
	parsed, err := btcutil.DecodeAddress(strings.TrimSpace(addr), params)
	if err != nil {
		return nil, errors.New("invalid btc address")
	}
	return txscript.PayToAddrScript(parsed)
}

func parseBTCAmountSat(v string) (int64, error) {
	amt, ok := new(big.Int).SetString(strings.TrimSpace(v), 10)
	if !ok || !amt.IsInt64() || amt.Sign() <= 0 {
		return 0, errors.New("invalid amount")
	}
	return amt.Int64(), nil
}

func findChangeOutputIndex(tx *wire.MsgTx, fromScript []byte, toScript []byte, transferAmount int64) int {
	for i := range tx.TxOut {
		out := tx.TxOut[i]
		if out == nil {
			continue
		}
		if bytes.Equal(out.PkScript, toScript) && out.Value == transferAmount {
			continue
		}
		if bytes.Equal(out.PkScript, fromScript) {
			return i
		}
	}
	return -1
}

func sumWireOutputs(tx *wire.MsgTx) (int64, error) {
	var total int64
	for i := range tx.TxOut {
		out := tx.TxOut[i]
		if out == nil || out.Value < 0 {
			return 0, errors.New("invalid btc output")
		}
		total += out.Value
	}
	return total, nil
}

func txVirtualSize(tx *wire.MsgTx) int64 {
	base := tx.SerializeSizeStripped()
	total := tx.SerializeSize()
	weight := base*3 + total
	return int64((weight + 3) / 4)
}

func serializeWireTxHex(tx *wire.MsgTx) (string, error) {
	var buf bytes.Buffer
	buf.Grow(tx.SerializeSize())
	if err := tx.Serialize(&buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf.Bytes()), nil
}

func btcFloatToSat(v float64) (int64, error) {
	amt, err := btcutil.NewAmount(v)
	if err != nil || amt < 0 {
		return 0, errors.New("invalid btc amount")
	}
	return int64(amt), nil
}

func marshalBTCUnsignedPayload(rawTx string, prevouts []btc.UnsignedTxPrevout) ([]byte, error) {
	unsigned := btcUnsignedWithdrawTx{
		RawTx:    rawTx,
		Prevouts: prevouts,
	}
	return json.Marshal(unsigned)
}

func buildBTCRBFContext(chain, fromAddr, toAddr, amount, signedPayload string) (*btcRBFContext, error) {
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return nil, err
	}
	if spec.Family != helpers.FamilyBTC {
		return nil, errors.New("invalid btc chain for rbf")
	}
	fromScript, err := payToAddressScriptByChain(spec, fromAddr)
	if err != nil {
		return nil, err
	}
	toScript, err := payToAddressScriptByChain(spec, toAddr)
	if err != nil {
		return nil, err
	}
	transferAmount, err := parseBTCAmountSat(amount)
	if err != nil {
		return nil, err
	}
	tx, err := decodeSignedBTCTx(signedPayload)
	if err != nil {
		return nil, err
	}
	if err := ensureRBFReplaceableTx(tx); err != nil {
		return nil, err
	}
	return &btcRBFContext{
		Tx:             tx,
		FromScript:     fromScript,
		ToScript:       toScript,
		TransferAmount: transferAmount,
	}, nil
}

func decodeSignedBTCTx(signedPayload string) (*wire.MsgTx, error) {
	rawSigned, err := hex.DecodeString(strings.TrimSpace(strings.TrimPrefix(signedPayload, "0x")))
	if err != nil {
		return nil, errors.New("invalid btc signed tx payload")
	}
	tx := &wire.MsgTx{}
	if err := tx.Deserialize(bytes.NewReader(rawSigned)); err != nil {
		return nil, errors.New("invalid btc signed tx payload")
	}
	return tx, nil
}

func ensureRBFReplaceableTx(tx *wire.MsgTx) error {
	if tx == nil || len(tx.TxIn) == 0 || len(tx.TxOut) == 0 {
		return errors.New("invalid btc tx")
	}
	for i := range tx.TxIn {
		if tx.TxIn[i].Sequence >= wire.MaxTxInSequenceNum-1 {
			return errors.New("btc tx is not replaceable")
		}
	}
	return nil
}

func resolveRBFNewFeeRate(cli *btc.Client, oldFeeRate, feeTargetBlocks, minDeltaSatPerVByte int64) (int64, error) {
	netRate := oldFeeRate
	if cli != nil {
		rate, err := cli.EstimateSatPerVByte(normalizeBTCFeeTarget(feeTargetBlocks))
		if err == nil && rate > 0 {
			netRate = rate
		}
	}
	if minDeltaSatPerVByte <= 0 {
		minDeltaSatPerVByte = 1
	}
	newFeeRate := helpers.MaxInt64(netRate, oldFeeRate+minDeltaSatPerVByte)
	if newFeeRate <= 0 {
		return 0, errors.New("invalid btc rbf fee rate")
	}
	return newFeeRate, nil
}

func ensureFeeIncrease(newFee, oldFee, minDeltaSatPerVByte, oldVSize int64) int64 {
	if newFee > oldFee {
		return newFee
	}
	if minDeltaSatPerVByte <= 0 {
		minDeltaSatPerVByte = 1
	}
	return oldFee + minDeltaSatPerVByte*oldVSize
}

func buildBTCRBFFeePlan(
	cli *btc.Client,
	tx *wire.MsgTx,
	totalInput int64,
	totalOutput int64,
	feeTargetBlocks int64,
	minDeltaSatPerVByte int64,
) (*btcRBFFeePlan, error) {
	if totalInput < totalOutput {
		return nil, errors.New("invalid btc tx inputs/outputs")
	}
	oldFee := totalInput - totalOutput
	oldVSize := txVirtualSize(tx)
	if oldVSize <= 0 {
		return nil, errors.New("invalid btc tx size")
	}
	oldFeeRate := helpers.CeilDivInt64(oldFee, oldVSize)
	if oldFeeRate <= 0 {
		oldFeeRate = 1
	}
	newFeeRate, err := resolveRBFNewFeeRate(cli, oldFeeRate, feeTargetBlocks, minDeltaSatPerVByte)
	if err != nil {
		return nil, err
	}
	newFee := newFeeRate * oldVSize
	newFee = ensureFeeIncrease(newFee, oldFee, minDeltaSatPerVByte, oldVSize)
	deltaFee := newFee - oldFee
	if deltaFee <= 0 {
		return nil, errors.New("invalid btc rbf fee delta")
	}
	return &btcRBFFeePlan{
		OldFeeRate: oldFeeRate,
		NewFeeRate: newFeeRate,
		OldFee:     oldFee,
		NewFee:     newFee,
		DeltaFee:   deltaFee,
	}, nil
}

func decodeBTCAddressByChain(chain string, addr string) (btcutil.Address, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, errors.New("invalid to address")
	}
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return nil, err
	}
	if spec.Family != helpers.FamilyBTC {
		return nil, errors.New("invalid btc chain")
	}

	params := &chaincfg.MainNetParams
	if spec.IsTestnet {
		params = &chaincfg.TestNet3Params
	}
	parsed, err := btcutil.DecodeAddress(addr, params)
	if err != nil || parsed == nil {
		return nil, errors.New("invalid to address")
	}
	return parsed, nil
}

func (c *btcClient) resolveFeeRate(rt Runtime) int64 {
	if c == nil {
		return 2
	}
	if c.cli != nil {
		rate, err := c.cli.EstimateSatPerVByte(normalizeBTCFeeTarget(rt.FeeTarget))
		if err == nil && rate > 0 {
			return rate
		}
	}
	if rt.FeeRate > 0 {
		return rt.FeeRate
	}
	return 2
}

func normalizeBTCMinConf(v int) int {
	if v < 0 {
		return 0
	}
	return v
}

func normalizeBTCFeeTarget(v int64) int64 {
	if v <= 0 {
		return 2
	}
	return v
}

func selectBTCUTXOs(utxos []btc.UTXO, amount *big.Int, feeRateSatPerVByte int64) ([]btc.UTXO, *big.Int, error) {
	sort.Slice(utxos, func(i, j int) bool {
		return utxos[i].ValueSat > utxos[j].ValueSat
	})

	target := amount.Int64()
	var total int64
	selected := make([]btc.UTXO, 0, len(utxos))
	for _, u := range utxos {
		selected = append(selected, u)
		total += u.ValueSat

		feeWithChange := estimateP2WPKHFee(len(selected), 2, feeRateSatPerVByte)
		if total < target+feeWithChange {
			continue
		}

		change := total - target - feeWithChange
		if change > 0 && change < btcChangeDustLimitSat {
			feeNoChange := estimateP2WPKHFee(len(selected), 1, feeRateSatPerVByte)
			if total >= target+feeNoChange {
				return selected, big.NewInt(0), nil
			}
		}

		if change >= 0 {
			return selected, big.NewInt(change), nil
		}
	}
	return nil, nil, errors.New("insufficient btc utxo balance")
}

func estimateP2WPKHFee(inputs int, outputs int, feeRateSatPerVByte int64) int64 {
	// Approximation for native segwit v0 P2WPKH tx virtual size.
	// 10 bytes base + 68 vbytes per input + 31 vbytes per output.
	vbytes := 10 + (68 * inputs) + (31 * outputs)
	return int64(math.Ceil(float64(vbytes) * float64(feeRateSatPerVByte)))
}

func formatBTCUTXOKey(txid string, vout uint32) string {
	return strings.ToLower(strings.TrimSpace(txid)) + ":" + strconv.FormatUint(uint64(vout), 10)
}
