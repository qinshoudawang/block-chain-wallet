package sol

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"

	"github.com/gagliardetto/solana-go"
	computebudget "github.com/gagliardetto/solana-go/programs/compute-budget"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/gagliardetto/solana-go/rpc"
)

type UnsignedWithdrawTx struct {
	From     string `json:"from"`
	TxBase64 string `json:"tx_base64"`
}

func (c *Client) BuildUnsignedWithdrawTx(ctx context.Context, from string, to string, amount uint64, priorityFeeMicroLamports uint64) ([]byte, error) {
	if err := c.ensureRPC(); err != nil {
		return nil, err
	}
	fromAddr, err := c.NormalizeAddress(from)
	if err != nil {
		return nil, errors.New("invalid solana from address")
	}
	toAddr, err := c.NormalizeAddress(to)
	if err != nil {
		return nil, errors.New("invalid solana to address")
	}
	if amount == 0 {
		return nil, errors.New("invalid solana amount")
	}
	blockhash, err := c.GetLatestBlockhash(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := buildSystemTransferTx(fromAddr, toAddr, blockhash, amount, priorityFeeMicroLamports)
	if err != nil {
		return nil, err
	}
	txBytes, err := tx.MarshalBinary()
	if err != nil {
		return nil, errors.New("marshal solana transaction failed")
	}
	payload := UnsignedWithdrawTx{
		From:     fromAddr,
		TxBase64: base64.StdEncoding.EncodeToString(txBytes),
	}
	return json.Marshal(payload)
}

func buildSystemTransferTx(from string, to string, recentBlockhash string, amount uint64, priorityFeeMicroLamports uint64) (*solana.Transaction, error) {
	if amount == 0 {
		return nil, errors.New("invalid solana amount")
	}
	fromPK, err := solana.PublicKeyFromBase58(from)
	if err != nil {
		return nil, errors.New("invalid solana from address")
	}
	toPK, err := solana.PublicKeyFromBase58(to)
	if err != nil {
		return nil, errors.New("invalid solana to address")
	}
	blockhash, err := solana.HashFromBase58(recentBlockhash)
	if err != nil {
		return nil, errors.New("invalid recent blockhash")
	}
	instruction, err := system.NewTransferInstruction(amount, fromPK, toPK).ValidateAndBuild()
	if err != nil {
		return nil, errors.New("invalid solana transfer instruction")
	}
	instructions := make([]solana.Instruction, 0, 2)
	if priorityFeeMicroLamports > 0 {
		priorityIx, err := computebudget.NewSetComputeUnitPriceInstruction(priorityFeeMicroLamports).ValidateAndBuild()
		if err != nil {
			return nil, errors.New("invalid solana priority fee")
		}
		instructions = append(instructions, priorityIx)
	}
	instructions = append(instructions, instruction)
	tx, err := solana.NewTransaction(instructions, blockhash, solana.TransactionPayer(fromPK))
	if err != nil {
		return nil, errors.New("build solana transaction failed")
	}
	return tx, nil
}

func (c *Client) GetLatestBlockhash(ctx context.Context) (string, error) {
	if err := c.ensureRPC(); err != nil {
		return "", err
	}
	resp, err := c.rpc.GetLatestBlockhash(ctx, rpc.CommitmentConfirmed)
	if err != nil {
		return "", err
	}
	if resp == nil || resp.Value == nil {
		return "", errors.New("empty latest blockhash")
	}
	return resp.Value.Blockhash.String(), nil
}
