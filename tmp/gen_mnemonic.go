package main

import (
	"fmt"
	"log"

	"github.com/tyler-smith/go-bip39"
)

func main() {
	entropy, err := bip39.NewEntropy(128) // 128=12词，256=24词
	if err != nil {
		log.Fatal(err)
	}
	m, err := bip39.NewMnemonic(entropy)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(m)
}
