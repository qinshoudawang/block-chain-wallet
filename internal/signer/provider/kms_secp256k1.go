package provider

import (
	"encoding/asn1"

	"github.com/btcsuite/btcd/btcec/v2"
)

type pkixPublicKey struct {
	Algorithm        pkixAlgorithmIdentifier
	SubjectPublicKey asn1.BitString
}

type pkixAlgorithmIdentifier struct {
	Algorithm  asn1.ObjectIdentifier
	Parameters asn1.RawValue `asn1:"optional"`
}

func ParseKMSSECP256K1PublicKey(der []byte) (*btcec.PublicKey, error) {
	var pkixKey pkixPublicKey
	if _, err := asn1.Unmarshal(der, &pkixKey); err != nil {
		return nil, err
	}
	return btcec.ParsePubKey(pkixKey.SubjectPublicKey.Bytes)
}
