package token

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

type Payload struct {
	WithdrawID string
	RequestID  string
	Chain      string
	From       string
	To         string
	Amount     string
	UnsignedTx []byte
}

func CanonicalMessage(p Payload) string {
	unsignedTxHash := sha256Hex(p.UnsignedTx)
	parts := []string{
		p.WithdrawID,
		p.RequestID,
		strings.ToLower(p.Chain),
		strings.ToLower(p.From),
		strings.ToLower(p.To),
		p.Amount,
		strings.ToLower(unsignedTxHash),
	}
	return strings.Join(parts, "|")
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
