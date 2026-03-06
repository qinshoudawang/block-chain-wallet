package main

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"wallet-system/internal/helpers"

	"github.com/btcsuite/btcd/btcutil/hdkeychain"
	"github.com/gagliardetto/solana-go"
	"github.com/joho/godotenv"
	"github.com/tyler-smith/go-bip39"
)

func main() {
	var (
		chain       = flag.String("chain", "sol", "chain name, e.g. sol")
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
	if spec.Family != helpers.FamilySOL {
		log.Fatalf("chain %s is not sol family", spec.CanonicalChain)
	}

	seed := bip39.NewSeed(mnemonic, *passphrase)
	edSeed, _, err := slip10DeriveEd25519(seed, []uint32{
		hardened(spec.Purpose),
		hardened(spec.CoinType),
		hardened(uint32(*index)),
		hardened(spec.Account),
	})
	if err != nil {
		log.Fatal(err)
	}

	edPriv := ed25519.NewKeyFromSeed(edSeed)
	priv := solana.PrivateKey(edPriv)
	pub := priv.PublicKey()

	keyArray := make([]int, len(priv))
	for i, b := range priv {
		keyArray[i] = int(b)
	}
	keyJSON, err := json.Marshal(keyArray)
	if err != nil {
		log.Fatal(err)
	}

	path := fmt.Sprintf(spec.PathFmt, *index)
	fmt.Printf("chain=%s\n", spec.CanonicalChain)
	fmt.Printf("path=%s\n", path)
	fmt.Printf("address=%s\n", pub.String())
	fmt.Printf("pubkey_hex=%s\n", hex.EncodeToString(pub[:]))
	fmt.Printf("private_key_base58=%s\n", priv.String())
	fmt.Printf("private_key_hex=%s\n", hex.EncodeToString(priv))
	fmt.Printf("private_key_json_array=%s\n", string(keyJSON))
}

func hardened(i uint32) uint32 {
	return i + hdkeychain.HardenedKeyStart
}

// SLIP-0010 (ed25519): all child derivations must be hardened.
func slip10DeriveEd25519(seed []byte, path []uint32) ([]byte, []byte, error) {
	mac := hmac.New(sha512.New, []byte("ed25519 seed"))
	_, _ = mac.Write(seed)
	I := mac.Sum(nil)
	if len(I) != 64 {
		return nil, nil, errors.New("invalid slip10 master length")
	}
	key := append([]byte(nil), I[:32]...)
	chainCode := append([]byte(nil), I[32:]...)

	for _, idx := range path {
		if idx < hdkeychain.HardenedKeyStart {
			return nil, nil, errors.New("solana path must use hardened indices")
		}
		mac = hmac.New(sha512.New, chainCode)
		buf := make([]byte, 0, 1+32+4)
		buf = append(buf, 0x00)
		buf = append(buf, key...)
		var ib [4]byte
		binary.BigEndian.PutUint32(ib[:], idx)
		buf = append(buf, ib[:]...)
		_, _ = mac.Write(buf)
		I = mac.Sum(nil)
		key = append(key[:0], I[:32]...)
		chainCode = append(chainCode[:0], I[32:]...)
	}
	return key, chainCode, nil
}
