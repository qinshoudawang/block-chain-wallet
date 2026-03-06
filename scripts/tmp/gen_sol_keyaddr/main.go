package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/gagliardetto/solana-go"
)

func main() {
	var out = flag.String("out", "", "optional output file path for Solana keypair JSON array (64-byte)")
	flag.Parse()

	priv, err := solana.NewRandomPrivateKey()
	if err != nil {
		log.Fatal(err)
	}
	pub := priv.PublicKey()

	raw := []byte(priv)
	keyArray := make([]int, len(raw))
	for i, b := range raw {
		keyArray[i] = int(b)
	}
	keyJSON, err := json.Marshal(keyArray)
	if err != nil {
		log.Fatal(err)
	}
	base64JSON, err := json.Marshal(raw)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("address=%s\n", pub.String())
	fmt.Printf("private_key_base58=%s\n", priv.String())
	fmt.Printf("private_key_hex=%s\n", hex.EncodeToString(raw))
	fmt.Printf("private_key_json_array=%s\n", string(keyJSON))
	fmt.Printf("private_key_json_base64=%s\n", string(base64JSON))

	if strings.TrimSpace(*out) != "" {
		if err := writeFile(*out, keyJSON); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("saved_keypair_json=%s\n", *out)
	}
}

func writeFile(path string, content []byte) error {
	return os.WriteFile(path, append(content, '\n'), 0o600)
}
