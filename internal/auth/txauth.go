package auth

import (
	"strings"
	"wallet-system/internal/auth/crypto/hmac"
)

type TxPayload struct {
	WithdrawID string
	RequestID  string
	Chain      string
	From       string
	To         string
	Amount     string
	UnsignedTx []byte
}

func MakeToken(secret []byte, p TxPayload) string {
	unsignedTxHash := hmac.SHA256Hex(p.UnsignedTx)
	msg := canonicalMessage(p, unsignedTxHash)
	return hmac.SignHex(secret, msg)
}

func VerifyToken(secret []byte, p TxPayload, token string) bool {
	unsignedTxHash := hmac.SHA256Hex(p.UnsignedTx)
	msg := canonicalMessage(p, unsignedTxHash)
	return hmac.VerifyHex(secret, msg, token)
}

func canonicalMessage(p TxPayload, unsignedTxHash string) string {
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
