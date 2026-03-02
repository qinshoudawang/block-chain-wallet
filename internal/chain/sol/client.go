package sol

import (
	"errors"
	"strings"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

type Client struct {
	rpcURL string
	rpc    *rpc.Client
}

func NewClient(rpcURL string) *Client {
	trimmed := strings.TrimSpace(rpcURL)
	return &Client{
		rpcURL: trimmed,
		rpc:    rpc.New(trimmed),
	}
}

func (c *Client) RPC() string {
	if c == nil {
		return ""
	}
	return c.rpcURL
}

func (c *Client) NormalizeAddress(addr string) (string, error) {
	trimmed := strings.TrimSpace(addr)
	if trimmed == "" {
		return "", errors.New("invalid solana address")
	}
	pk, err := solana.PublicKeyFromBase58(trimmed)
	if err != nil {
		return "", errors.New("invalid solana address")
	}
	return pk.String(), nil
}

func (c *Client) DecodeAddress(addr string) ([]byte, error) {
	trimmed := strings.TrimSpace(addr)
	if trimmed == "" {
		return nil, errors.New("invalid solana address")
	}
	pk, err := solana.PublicKeyFromBase58(trimmed)
	if err != nil {
		return nil, err
	}
	return pk.Bytes(), nil
}

func (c *Client) ensureRPC() error {
	if c == nil || c.rpc == nil || strings.TrimSpace(c.rpcURL) == "" {
		return errors.New("solana rpc client is required")
	}
	return nil
}
