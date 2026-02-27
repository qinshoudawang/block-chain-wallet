package hmac

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

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
