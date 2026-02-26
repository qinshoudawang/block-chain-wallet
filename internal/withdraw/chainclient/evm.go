package chainclient

import (
	"context"
	"errors"
	"math/big"

	"wallet-system/internal/chain/evm"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

type evmClient struct {
	eth *ethclient.Client
}

type evmValidatedWithdrawInput struct {
	to     common.Address
	amount *big.Int
}

func (i evmValidatedWithdrawInput) ToAddress() string     { return i.to.Hex() }
func (i evmValidatedWithdrawInput) AmountValue() *big.Int { return new(big.Int).Set(i.amount) }

func newEVMClient(eth *ethclient.Client) Client {
	return &evmClient{eth: eth}
}

func (c *evmClient) ValidateWithdrawInput(to string, amount string) (ValidatedWithdrawInput, error) {
	addr := common.HexToAddress(to)
	if addr == (common.Address{}) {
		return nil, errors.New("invalid to address")
	}
	amt := new(big.Int)
	if _, ok := amt.SetString(amount, 10); !ok || amt.Sign() <= 0 {
		return nil, errors.New("invalid amount")
	}
	return evmValidatedWithdrawInput{to: addr, amount: amt}, nil
}

func (c *evmClient) AllocateNonce(ctx context.Context, rt Runtime, nonceFloorProvider NonceFloorProvider) (uint64, error) {
	if c == nil || c.eth == nil {
		return 0, errors.New("evm client is required")
	}
	if rt.Redis == nil {
		return 0, errors.New("redis is required")
	}
	if nonceFloorProvider == nil {
		return 0, errors.New("nonce floor provider is required")
	}

	nm := evm.NewNonceManager(rt.Redis, c.eth, rt.Chain, rt.From)
	nm.WithNonceFloorProvider(nonceFloorProvider)
	if err := nm.EnsureInitialized(ctx); err != nil {
		return 0, err
	}
	return nm.Allocate(ctx)
}

func (c *evmClient) BuildUnsignedWithdrawTx(
	ctx context.Context,
	rt Runtime,
	in ValidatedWithdrawInput,
	nonce uint64,
) ([]byte, error) {
	if c == nil || c.eth == nil {
		return nil, errors.New("evm client is required")
	}
	if rt.ChainID == nil {
		return nil, errors.New("chain id is required")
	}
	evmIn, ok := in.(evmValidatedWithdrawInput)
	if !ok {
		if p, ok := in.(*evmValidatedWithdrawInput); ok && p != nil {
			evmIn = *p
		} else {
			return nil, errors.New("invalid evm withdraw input")
		}
	}
	return evm.NewEVMBuilder(c.eth).BuildUnsignedTx(ctx, rt.From, evmIn.to, evmIn.amount, nil, rt.ChainID, nonce)
}
