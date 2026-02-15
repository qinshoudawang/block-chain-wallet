package helpers

import (
	"log"
	"math/big"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

func init() {
	if err := godotenv.Load(".env"); err != nil && !os.IsNotExist(err) {
		log.Fatalf("load .env failed: %v", err)
	}
}

func Getenv(k, def string) string {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	return v
}

func MustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		panic(k + " is required")
	}
	return v
}

func MustAtoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return n
}

func MustBig(s string) *big.Int {
	bi := new(big.Int)
	if _, ok := bi.SetString(s, 10); !ok {
		panic("invalid big int")
	}
	return bi
}
