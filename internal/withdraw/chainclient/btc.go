package chainclient

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"wallet-system/internal/helpers"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
)

type btcClient struct{}

type btcUnsignedWithdrawTx struct {
	Version int    `json:"version"`
	Chain   string `json:"chain"`
	From    string `json:"from"`
	To      string `json:"to"`
	Amount  string `json:"amount"`
}

func newBTCClient() Client { return &btcClient{} }

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
	if _, err := decodeBTCAddressByChain(rt.Chain, toAddr); err != nil {
		return nil, errors.New("invalid btc to address")
	}
	if amount == nil || amount.Sign() <= 0 {
		return nil, errors.New("invalid btc amount")
	}
	from := strings.TrimSpace(rt.FromAddress)
	if _, err := decodeBTCAddressByChain(rt.Chain, from); err != nil {
		return nil, errors.New("invalid btc from address")
	}
	unsigned := btcUnsignedWithdrawTx{
		Version: 1,
		Chain:   rt.Chain,
		From:    from,
		To:      strings.TrimSpace(toAddr),
		Amount:  amount.String(),
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
