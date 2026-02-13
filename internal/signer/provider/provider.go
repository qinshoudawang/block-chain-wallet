package provider

type SignerProvider interface {
	Sign(unsignedTx []byte) ([]byte, error)
}
