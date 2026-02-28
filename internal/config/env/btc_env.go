package env

import (
	"errors"
	"math/big"
	"strings"

	"wallet-system/internal/helpers"
)

type BTCProfile struct {
	Chain         string
	FromAddress   string
	FreezeReserve *big.Int
	MinConf       int
	FeeTarget     int64
	FeeRate       int64
	Host          string
	User          string
	Pass          string
	DisableTLS    bool
	Params        string
}

func LoadBTCProfileFromEnv() (BTCProfile, bool, error) {
	btcFrom := helpers.Getenv("BTC_FROM_ADDRESS", "")
	if btcFrom == "" {
		return BTCProfile{}, false, nil
	}
	chainRaw := helpers.Getenv("BTC_CHAIN", "btc")
	spec, err := helpers.ResolveChainSpec(chainRaw)
	if err != nil {
		return BTCProfile{}, false, err
	}
	if spec.Family != "btc" {
		return BTCProfile{}, false, errors.New("BTC_CHAIN must be a btc family chain")
	}
	params := "mainnet"
	if spec.IsTestnet {
		params = "testnet3"
	}
	feeReserveSats := big.NewInt(0)
	if reserve := helpers.Getenv("BTC_WITHDRAW_FEE_RESERVE_SATS", "0"); reserve != "" {
		if _, ok := feeReserveSats.SetString(reserve, 10); !ok || feeReserveSats.Sign() < 0 {
			return BTCProfile{}, false, errors.New("invalid BTC_WITHDRAW_FEE_RESERVE_SATS")
		}
	}
	minConf := helpers.ParseIntEnv("BTC_UTXO_MIN_CONF", 1)
	if minConf < 0 {
		return BTCProfile{}, false, errors.New("invalid BTC_UTXO_MIN_CONF: must be >= 0")
	}
	feeTarget := helpers.ParseInt64Env("BTC_FEE_TARGET_BLOCKS", 2)
	if feeTarget <= 0 {
		return BTCProfile{}, false, errors.New("invalid BTC_FEE_TARGET_BLOCKS: must be > 0")
	}
	feeRate := helpers.ParseInt64Env("BTC_FEE_RATE_SAT_PER_VBYTE", 2)
	if feeRate <= 0 {
		return BTCProfile{}, false, errors.New("invalid BTC_FEE_RATE_SAT_PER_VBYTE: must be > 0")
	}
	return BTCProfile{
		Chain:         spec.CanonicalChain,
		FromAddress:   btcFrom,
		FreezeReserve: feeReserveSats,
		MinConf:       minConf,
		FeeTarget:     feeTarget,
		FeeRate:       feeRate,
		Host:          helpers.MustEnv("BTC_RPC_HOST"),
		User:          helpers.MustEnv("BTC_RPC_USER"),
		Pass:          helpers.MustEnv("BTC_RPC_PASS"),
		DisableTLS:    strings.EqualFold(helpers.Getenv("BTC_RPC_DISABLE_TLS", "true"), "true"),
		Params:        params,
	}, true, nil
}
