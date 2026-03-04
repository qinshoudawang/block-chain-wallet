package chainclient

import (
	"context"
	"encoding/base64"
	"errors"
	"math"
	"math/big"
	"strings"
	solchain "wallet-system/internal/chain/sol"
	"wallet-system/internal/helpers"
	"wallet-system/internal/sequence/nonce"

	"github.com/gagliardetto/solana-go"
	computebudget "github.com/gagliardetto/solana-go/programs/compute-budget"
	"github.com/redis/go-redis/v9"
)

type solanaClient struct {
	sol *solchain.Client
}

func newSolanaClient() Client {
	return &solanaClient{sol: solchain.NewClient("")}
}

func newSolanaClientWithClient(cli *solchain.Client) Client {
	return &solanaClient{sol: cli}
}

func (c *solanaClient) ValidateWithdrawInput(chain string, to string, amount string) (string, *big.Int, error) {
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return "", nil, err
	}
	if spec.Family != helpers.FamilySOL {
		return "", nil, errors.New("invalid solana chain")
	}
	if c == nil || c.sol == nil {
		return "", nil, errors.New("solana client is required")
	}
	normalizedTo, err := c.sol.NormalizeAddress(to)
	if err != nil {
		return "", nil, errors.New("invalid to address")
	}
	amt := new(big.Int)
	if _, ok := amt.SetString(amount, 10); !ok || amt.Sign() <= 0 {
		return "", nil, errors.New("invalid amount")
	}
	if !amt.IsUint64() {
		return "", nil, errors.New("solana amount too large")
	}
	return normalizedTo, amt, nil
}

func (c *solanaClient) AllocateSequence(
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
	if c == nil || c.sol == nil {
		return 0, errors.New("solana client is required")
	}
	account, err := c.sol.NormalizeAddress(rt.FromAddress)
	if err != nil {
		return 0, errors.New("invalid solana from address")
	}
	nm := nonce.NewManager(redisClient, rt.Chain, account, sequenceFloorProvider)
	if err := nm.EnsureInitialized(ctx); err != nil {
		return 0, err
	}
	return nm.Allocate(ctx)
}

func (c *solanaClient) BuildUnsignedWithdrawTx(
	ctx context.Context,
	rt Runtime,
	toAddr string,
	amount *big.Int,
	sequence uint64,
) ([]byte, error) {
	_ = sequence
	if c == nil || c.sol == nil {
		return nil, errors.New("solana client is required")
	}
	if amount == nil || amount.Sign() <= 0 {
		return nil, errors.New("invalid solana amount")
	}
	if !amount.IsUint64() {
		return nil, errors.New("solana amount too large")
	}
	return c.sol.BuildUnsignedWithdrawTx(ctx, rt.FromAddress, toAddr, amount.Uint64(), normalizeSOLPriorityFee(rt.FeeRate))
}

func (c *solanaClient) BuildRBFUnsignedWithdrawTx(
	ctx context.Context,
	chain string,
	fromAddr string,
	toAddr string,
	amount string,
	signedPayload string,
) (*RBFUnsignedBuildResult, error) {
	if c == nil || c.sol == nil {
		return nil, errors.New("solana client is required")
	}
	spec, err := helpers.ResolveChainSpec(chain)
	if err != nil {
		return nil, err
	}
	if spec.Family != helpers.FamilySOL {
		return nil, errors.New("invalid solana chain for rbf")
	}
	fromNormalized, err := c.sol.NormalizeAddress(fromAddr)
	if err != nil {
		return nil, errors.New("invalid solana from address")
	}
	toNormalized, err := c.sol.NormalizeAddress(toAddr)
	if err != nil {
		return nil, errors.New("invalid solana to address")
	}
	amt := new(big.Int)
	if _, ok := amt.SetString(amount, 10); !ok || amt.Sign() <= 0 {
		return nil, errors.New("invalid amount")
	}
	if !amt.IsUint64() {
		return nil, errors.New("solana amount too large")
	}
	oldPriorityFee, err := extractSOLPriorityFeeFromSignedPayload(signedPayload)
	if err != nil {
		return nil, err
	}
	newPriorityFee := nextSOLRBFPriorityFee(oldPriorityFee)
	unsignedPayload, err := c.sol.BuildUnsignedWithdrawTx(ctx, fromNormalized, toNormalized, amt.Uint64(), uint64(newPriorityFee))
	if err != nil {
		return nil, err
	}
	return &RBFUnsignedBuildResult{
		UnsignedPayload: unsignedPayload,
		OldFeeRate:      oldPriorityFee,
		NewFeeRate:      newPriorityFee,
		OldFee:          0,
		NewFee:          0,
	}, nil
}

const (
	defaultSOLRBFPriorityMicroLamports int64 = 1_000
	minSOLRBFBumpMicroLamports         int64 = 1_000
)

func normalizeSOLPriorityFee(v int64) uint64 {
	if v <= 0 {
		return 0
	}
	return uint64(v)
}

func extractSOLPriorityFeeFromSignedPayload(signedPayload string) (int64, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(signedPayload))
	if err != nil || len(raw) == 0 {
		return 0, errors.New("invalid solana signed tx payload")
	}
	tx, err := solana.TransactionFromBytes(raw)
	if err != nil {
		return 0, errors.New("invalid solana signed tx payload")
	}
	return extractSOLPriorityFeeFromTx(tx)
}

func extractSOLPriorityFeeFromTx(tx *solana.Transaction) (int64, error) {
	if tx == nil {
		return 0, errors.New("invalid solana tx")
	}
	var price uint64
	for i := range tx.Message.Instructions {
		ix := tx.Message.Instructions[i]
		programID, err := tx.Message.Program(ix.ProgramIDIndex)
		if err != nil {
			return 0, errors.New("invalid solana tx message")
		}
		if !programID.Equals(solana.ComputeBudget) {
			continue
		}
		decoded, err := computebudget.DecodeInstruction(nil, ix.Data)
		if err != nil || decoded == nil {
			continue
		}
		switch v := decoded.Impl.(type) {
		case computebudget.SetComputeUnitPrice:
			if v.MicroLamports > price {
				price = v.MicroLamports
			}
		case *computebudget.SetComputeUnitPrice:
			if v != nil && v.MicroLamports > price {
				price = v.MicroLamports
			}
		}
	}
	if price > math.MaxInt64 {
		return 0, errors.New("solana priority fee out of range")
	}
	return int64(price), nil
}

func nextSOLRBFPriorityFee(oldPriorityFee int64) int64 {
	if oldPriorityFee <= 0 {
		return defaultSOLRBFPriorityMicroLamports
	}
	return oldPriorityFee + minSOLRBFBumpMicroLamports
}
