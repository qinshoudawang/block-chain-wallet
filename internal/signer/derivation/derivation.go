package derivation

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"wallet-system/internal/helpers"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/btcutil/base58"
	"github.com/btcsuite/btcd/btcutil/hdkeychain"
	"github.com/btcsuite/btcd/chaincfg"
	hdwallet "github.com/miguelmota/go-ethereum-hdwallet"
	"github.com/tyler-smith/go-bip39"
)

var (
	ErrUnsupportedChain = errors.New("unsupported chain")
)

type Deriver struct {
	mnemonic string
	evm      *hdwallet.Wallet
}

func NewDeriver(mnemonic string) (*Deriver, error) {
	if strings.TrimSpace(mnemonic) == "" {
		return nil, errors.New("mnemonic is required")
	}
	evmWallet, err := hdwallet.NewFromMnemonic(mnemonic)
	if err != nil {
		return nil, err
	}
	return &Deriver{
		mnemonic: mnemonic,
		evm:      evmWallet,
	}, nil
}

func (d *Deriver) DeriveAddress(chain string, index uint32) (string, error) {
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrUnsupportedChain, chain)
	}
	switch spec.Family {
	case "evm":
		return d.deriveEVM(index, spec)
	case "btc":
		return d.deriveBTC(index, spec)
	case "sol":
		return d.deriveSolana(index, spec)
	default:
		return "", fmt.Errorf("%w: %s", ErrUnsupportedChain, chain)
	}
}

func (d *Deriver) deriveEVM(index uint32, spec helpers.ChainSpec) (string, error) {
	path := fmt.Sprintf(spec.PathFmt, index)
	dp := hdwallet.MustParseDerivationPath(path)
	account, err := d.evm.Derive(dp, false)
	if err != nil {
		return "", err
	}
	return account.Address.Hex(), nil
}

func (d *Deriver) DeriveEVMPrivateKey(chain string, index uint32) (*ecdsa.PrivateKey, string, error) {
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %s", ErrUnsupportedChain, chain)
	}
	if spec.Family != helpers.FamilyEVM {
		return nil, "", fmt.Errorf("%w: %s", ErrUnsupportedChain, chain)
	}
	path := fmt.Sprintf(spec.PathFmt, index)
	dp := hdwallet.MustParseDerivationPath(path)
	account, err := d.evm.Derive(dp, false)
	if err != nil {
		return nil, "", err
	}
	priv, err := d.evm.PrivateKey(account)
	if err != nil {
		return nil, "", err
	}
	return priv, account.Address.Hex(), nil
}

// deriveBTC returns a native segwit (P2WPKH) address using BIP84:
// mainnet: m/84'/0'/0'/0/index
// testnet: m/84'/1'/0'/0/index
func (d *Deriver) deriveBTC(index uint32, spec helpers.ChainSpec) (string, error) {
	seed := bip39.NewSeed(d.mnemonic, "")
	netParams := &chaincfg.MainNetParams
	if spec.IsTestnet {
		netParams = &chaincfg.TestNet3Params
	}
	master, err := hdkeychain.NewMaster(seed, netParams)
	if err != nil {
		return "", err
	}

	key, err := deriveBTCPath(master, []uint32{
		hardened(spec.Purpose),
		hardened(spec.CoinType),
		hardened(spec.Account),
		0,
		index,
	})
	if err != nil {
		return "", err
	}
	pub, err := key.ECPubKey()
	if err != nil {
		return "", err
	}
	pubKeyHash := btcutil.Hash160(pub.SerializeCompressed())
	addr, err := btcutil.NewAddressWitnessPubKeyHash(pubKeyHash, netParams)
	if err != nil {
		return "", err
	}
	return addr.EncodeAddress(), nil
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

// deriveSolana returns a Solana base58 address using hardened path:
// m/44'/501'/index'/0'
func (d *Deriver) deriveSolana(index uint32, spec helpers.ChainSpec) (string, error) {
	seed := bip39.NewSeed(d.mnemonic, "")
	privSeed, _, err := slip10DeriveEd25519(seed, []uint32{
		hardened(spec.Purpose),
		hardened(spec.CoinType),
		hardened(index),
		hardened(spec.Account),
	})
	if err != nil {
		return "", err
	}
	edPriv := ed25519.NewKeyFromSeed(privSeed)
	pub := edPriv.Public().(ed25519.PublicKey)
	return base58.Encode(pub), nil
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
