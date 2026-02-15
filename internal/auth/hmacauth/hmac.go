package hmacauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

type Payload struct {
	WithdrawID     string
	RequestID      string
	Chain          string
	From           string
	To             string
	Amount         string
	UnsignedTxHash string // hex string of sha256(unsigned_tx) or keccak; choose one and fix
}

func CanonicalMessage(p Payload) string {
	parts := []string{
		p.WithdrawID,
		p.RequestID,
		strings.ToLower(p.Chain),
		strings.ToLower(p.From),
		strings.ToLower(p.To),
		p.Amount,
		strings.ToLower(p.UnsignedTxHash),
	}
	return strings.Join(parts, "|")
}

// SignHex returns hex(hmac_sha256(secret, message))
func SignHex(secret []byte, message string) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(message))
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyHex compares in constant time
func VerifyHex(secret []byte, message string, tokenHex string) bool {
	got, err := hex.DecodeString(tokenHex)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(message))
	expected := mac.Sum(nil)
	return hmac.Equal(expected, got)
}

func SHA256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
