package chainclient

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"sort"
	"strings"
	"wallet-system/internal/chain/btc"
	"wallet-system/internal/helpers"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
)

type btcClient struct {
	cli *btc.Client
}

type btcUnsignedWithdrawTx struct {
	RawTx    string                  `json:"raw_tx"`
	Prevouts []btc.UnsignedTxPrevout `json:"prevouts"`
}

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

	// 4) 构建 unsigned raw tx，并附带 signer 需要的 prevout 元数据。
	rawTxHex, prevouts, err := c.cli.BuildUnsignedWithdrawTx(fromAddr, toParsed, selected, amount.Int64(), change.Int64())
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
