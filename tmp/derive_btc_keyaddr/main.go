package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"wallet-system/internal/helpers"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/btcutil/hdkeychain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/joho/godotenv"
	"github.com/tyler-smith/go-bip39"
)

func main() {
	var (
		chain       = flag.String("chain", "btc-testnet", "chain name, e.g. btc or btc-testnet")
		index       = flag.Uint("index", 0, "address index")
		mnemonicEnv = flag.String("mnemonic-env", "SIGNER_MNEMONIC", "env var name containing mnemonic")
		passphrase  = flag.String("passphrase", "", "bip39 passphrase")
	)
	flag.Parse()

	_ = godotenv.Load()

	mnemonic := strings.TrimSpace(os.Getenv(*mnemonicEnv))
	if mnemonic == "" {
		log.Fatalf("%s is empty", *mnemonicEnv)
	}
	if !bip39.IsMnemonicValid(mnemonic) {
		log.Fatal("invalid mnemonic")
	}

	spec, err := helpers.ResolveChainSpec(*chain)
	if err != nil {
		log.Fatal(err)
	}
	if spec.Family != helpers.FamilyBTC {
		log.Fatalf("chain %s is not btc family", spec.CanonicalChain)
	}

	params := &chaincfg.MainNetParams
	if spec.IsTestnet {
		params = &chaincfg.TestNet3Params
	}

	seed := bip39.NewSeed(mnemonic, *passphrase)
	master, err := hdkeychain.NewMaster(seed, params)
	if err != nil {
		log.Fatal(err)
	}

	key, err := deriveBTCPath(master, []uint32{
		hardened(spec.Purpose),
		hardened(spec.CoinType),
		hardened(spec.Account),
		0,
		uint32(*index),
	})
	if err != nil {
		log.Fatal(err)
	}

	priv, err := key.ECPrivKey()
	if err != nil {
		log.Fatal(err)
	}
	pub := priv.PubKey()

	pubHash := btcutil.Hash160(pub.SerializeCompressed())
	addr, err := btcutil.NewAddressWitnessPubKeyHash(pubHash, params)
	if err != nil {
		log.Fatal(err)
	}

	wif, err := btcutil.NewWIF(priv, params, true)
	if err != nil {
		log.Fatal(err)
	}

	path := fmt.Sprintf(spec.PathFmt, *index)
	fmt.Printf("chain=%s\n", spec.CanonicalChain)
	fmt.Printf("path=%s\n", path)
	fmt.Printf("address=%s\n", addr.EncodeAddress())
	fmt.Printf("pubkey_compressed=%s\n", hex.EncodeToString(pub.SerializeCompressed()))
	fmt.Printf("privkey_hex=%s\n", hex.EncodeToString(priv.Serialize()))
	fmt.Printf("wif=%s\n", wif.String())
}

func deriveBTCPath(key *hdkeychain.ExtendedKey, path []uint32) (*hdkeychain.ExtendedKey, error) {
	cur := key
	var err error
	for _, p := range path {
		cur, err = cur.Derive(p)
		if err != nil {
			return nil, err
		}
	}
	return cur, nil
}

func hardened(i uint32) uint32 {
	return i + hdkeychain.HardenedKeyStart
}
