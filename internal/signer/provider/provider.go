package provider

type SignerProvider interface {
	Sign(unsignedTx []byte) ([]byte, error)
}

func trim0x(s string) string {
	if len(s) >= 2 && (s[:2] == "0x" || s[:2] == "0X") {
		return s[2:]
	}
	return s
}
