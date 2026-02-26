package evm

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type EVMSender struct {
	cli *ethclient.Client
}

func NewEVMSender(rpc string) (*EVMSender, error) {
	cli, err := ethclient.Dial(rpc)
	if err != nil {
		return nil, err
	}
	return &EVMSender{cli: cli}, nil
}

func (s *EVMSender) Broadcast(ctx context.Context, signedTxBytes []byte) (string, error) {
	var tx types.Transaction
	if err := tx.UnmarshalBinary(signedTxBytes); err != nil {
		return "", err
	}
	if err := s.cli.SendTransaction(ctx, &tx); err != nil {
		return "", err
	}
	return tx.Hash().Hex(), nil
}

func (s *EVMSender) LatestHeight(ctx context.Context) (uint64, error) {
	if s == nil || s.cli == nil {
		return 0, ErrSenderNotConfigured
	}
	return s.cli.BlockNumber(ctx)
}

func (s *EVMSender) TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	if s == nil || s.cli == nil {
		return nil, ErrSenderNotConfigured
	}
	return s.cli.TransactionReceipt(ctx, txHash)
}

func (s *EVMSender) Close() error {
	if s == nil || s.cli == nil {
		return nil
	}
	s.cli.Close()
	return nil
}

var ErrSenderNotConfigured = errSenderNotConfigured{}

type errSenderNotConfigured struct{}

func (errSenderNotConfigured) Error() string { return "evm sender not configured" }
