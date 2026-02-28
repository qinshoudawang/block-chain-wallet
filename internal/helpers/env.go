package helpers

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

func init() {
	if err := godotenv.Load(".env"); err != nil && !os.IsNotExist(err) {
		log.Fatalf("load .env failed: %v", err)
	}
}

func Getenv(k, def string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	return v
}

func MustEnv(k string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		panic(k + " is required")
	}
	return v
}

func ParseIntEnv(key string, defaultValue int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultValue
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		panic("invalid " + key + ": " + err.Error())
	}
	return v
}

func ParseInt64Env(key string, defaultValue int64) int64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultValue
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		panic("invalid " + key + ": " + err.Error())
	}
	return v
}
