package txsender

import (
	"context"

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
