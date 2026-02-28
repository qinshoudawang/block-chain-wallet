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

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
)

type btcClient struct {
	rpc *btc.Client
}

type btcUnsignedWithdrawTx struct {
	RawTx    string                 `json:"raw_tx"`
	Prevouts []btcUnsignedTxPrevout `json:"prevouts"`
}

type btcUnsignedTxPrevout struct {
	Value    string `json:"value"`
	PkScript string `json:"pk_script"`
}

type btcSpendableUTXO struct {
	TxID  string
	Vout  uint32
	Value int64
}

func newBTCClientWithRPC(rpc *btc.Client) Client {
	return &btcClient{rpc: rpc}
}

func (c *btcClient) RequiresNonce() bool { return false }

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

func (c *btcClient) AllocateNonce(ctx context.Context, rt Runtime, nonceFloorProvider NonceFloorProvider) (uint64, error) {
	return 0, nil
}

func (c *btcClient) BuildUnsignedWithdrawTx(
	ctx context.Context,
	rt Runtime,
	toAddr string,
	amount *big.Int,
	nonce uint64,
) ([]byte, error) {
	_ = nonce

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
	utxos, err := c.rpc.ListUnspentByAddress(ctx, fromAddr, normalizeBTCMinConf(rt.MinConf))
	if err != nil {
		return nil, err
	}
	if len(utxos) == 0 {
		return nil, errors.New("no spendable btc utxo")
	}
	spendable := make([]btcSpendableUTXO, 0, len(utxos))
	for _, u := range utxos {
		if u.ValueSat <= 0 {
			continue
		}
		spendable = append(spendable, btcSpendableUTXO{TxID: u.TxID, Vout: u.Vout, Value: u.ValueSat})
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

	// 4) 构建 unsigned raw tx，并附带 signer 需要的 prevout 元数据。
	rawTxHex, prevouts, err := buildBTCUnsignedSignPayload(fromAddr, toParsed, selected, amount.Int64(), change.Int64())
	if err != nil {
		return nil, err
	}

	// 5) 输出给 signer 的统一 payload（raw_tx + prevouts）。
	unsigned := btcUnsignedWithdrawTx{
		RawTx:    rawTxHex,
		Prevouts: prevouts,
	}
	return json.Marshal(unsigned)
}

func buildBTCUnsignedSignPayload(fromAddr, toParsed btcutil.Address, selected []btcSpendableUTXO, amount, change int64) (string, []btcUnsignedTxPrevout, error) {
	// 1) 计算 from 地址锁定脚本，作为所有输入 prevout 的脚本信息。
	fromPkScript, err := txscript.PayToAddrScript(fromAddr)
	if err != nil {
		return "", nil, err
	}

	// 2) 组装 unsigned 交易输入（逐个写入已选 UTXO）。
	tx := wire.NewMsgTx(1)
	for i := range selected {
		h, err := chainhash.NewHashFromStr(selected[i].TxID)
		if err != nil {
			return "", nil, errors.New("invalid btc input txid")
		}
		tx.AddTxIn(&wire.TxIn{
			PreviousOutPoint: wire.OutPoint{Hash: *h, Index: selected[i].Vout},
			Sequence:         wire.MaxTxInSequenceNum,
		})
	}

	// 3) 组装交易输出（目标转账 + 可选找零）。
	toPkScript, err := txscript.PayToAddrScript(toParsed)
	if err != nil {
		return "", nil, err
	}
	tx.AddTxOut(&wire.TxOut{Value: amount, PkScript: toPkScript})
	if change > 0 {
		tx.AddTxOut(&wire.TxOut{Value: change, PkScript: fromPkScript})
	}

	// 4) 序列化 unsigned tx，并编码为 hex 给 signer 使用。
	var buf bytes.Buffer
	buf.Grow(tx.SerializeSize())
	if err := tx.Serialize(&buf); err != nil {
		return "", nil, err
	}
	rawTxHex := hex.EncodeToString(buf.Bytes())

	// 5) 构造每个输入对应的 prevout 元数据（value + pk_script）。
	prevouts := make([]btcUnsignedTxPrevout, 0, len(selected))
	pkScriptHex := hex.EncodeToString(fromPkScript)
	for i := range selected {
		prevouts = append(prevouts, btcUnsignedTxPrevout{
			Value:    strconv.FormatInt(selected[i].Value, 10),
			PkScript: pkScriptHex,
		})
	}

	return rawTxHex, prevouts, nil
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
	if spec.Family != "btc" {
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
	if c.rpc != nil {
		rate, err := c.rpc.EstimateSatPerVByte(normalizeBTCFeeTarget(rt.FeeTarget))
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

func selectBTCUTXOs(utxos []btcSpendableUTXO, amount *big.Int, feeRateSatPerVByte int64) ([]btcSpendableUTXO, *big.Int, error) {
	sort.Slice(utxos, func(i, j int) bool {
		return utxos[i].Value > utxos[j].Value
	})

	target := amount.Int64()
	var total int64
	selected := make([]btcSpendableUTXO, 0, len(utxos))
	for _, u := range utxos {
		selected = append(selected, u)
		total += u.Value

		feeWithChange := estimateP2WPKHFee(len(selected), 2, feeRateSatPerVByte)
		if total < target+feeWithChange {
			continue
		}

		change := total - target - feeWithChange
		if change > 0 && change < 546 {
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
