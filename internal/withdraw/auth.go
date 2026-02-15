package withdraw

import "wallet-system/internal/auth/hmacauth"

type AuthSigner struct {
	secret []byte
}

func NewAuthSigner(secret []byte) *AuthSigner {
	return &AuthSigner{secret: secret}
}

func (a *AuthSigner) MakeAuthToken(withdrawID, requestID, chain, from, to, amount string, unsignedTx []byte) (token string, unsignedTxHash string) {
	unsignedTxHash = hmacauth.SHA256Hex(unsignedTx)
	msg := hmacauth.CanonicalMessage(hmacauth.Payload{
		WithdrawID:     withdrawID,
		RequestID:      requestID,
		Chain:          chain,
		From:           from,
		To:             to,
		Amount:         amount,
		UnsignedTxHash: unsignedTxHash,
	})
	token = hmacauth.SignHex(a.secret, msg)
	return token, unsignedTxHash
}
