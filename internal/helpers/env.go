package helpers

import (
	"math/big"
	"os"
	"strconv"
)

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
