package evm

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/go-redis/v9"
)

var (
	ErrNotInitialized = errors.New("nonce manager not initialized")
)

// Manager manages nonce for a single (chain, from) pair.
type NonceManager struct {
	rdb   *redis.Client
	eth   *ethclient.Client
	chain string
	from  common.Address

	ttl time.Duration // optional TTL for the nonce key (can be 0 = no ttl)
}

func NewNonceManager(rdb *redis.Client, eth *ethclient.Client, chain string, from common.Address) *NonceManager {
	return &NonceManager{
		rdb:   rdb,
		eth:   eth,
		chain: chain,
		from:  from,
		ttl:   0,
	}
}

func (m *NonceManager) nonceKey() string {
	return fmt.Sprintf("nonce:%s:%s:next", m.chain, m.from.Hex())
}

// EnsureInitialized sets Redis nonce key based on chain pending nonce if missing.
// Call this at signer startup, and also as a fallback if Allocate sees missing key.
func (m *NonceManager) EnsureInitialized(ctx context.Context) error {
	key := m.nonceKey()

	// Fast path: already exists
	exists, err := m.rdb.Exists(ctx, key).Result()
	if err != nil {
		return err
	}
	if exists == 1 {
		return nil
	}

	// Query chain pending nonce
	pn, err := m.eth.PendingNonceAt(ctx, m.from)
	if err != nil {
		return err
	}

	// Set only if still missing (race safe)
	ok, err := m.rdb.SetNX(ctx, key, pn, m.ttl).Result()
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	// Someone else initialized after our Exists
	return nil
}

// Allocate returns a nonce and increments stored next nonce by 1.
func (m *NonceManager) Allocate(ctx context.Context) (uint64, error) {
	key := m.nonceKey()

	// Use Lua to:
	// - if key missing => return -1 (so caller can EnsureInitialized then retry)
	// - else => GET current, INCR, return old
	const lua = `
local v = redis.call("GET", KEYS[1])
if not v then
  return -1
end
redis.call("INCR", KEYS[1])
return tonumber(v)
`
	res, err := m.rdb.Eval(ctx, lua, []string{key}).Result()
	if err != nil {
		return 0, err
	}

	n, ok := res.(int64)
	if !ok {
		// Sometimes redis returns string; handle it defensively
		switch t := res.(type) {
		case string:
			parsed, perr := strconv.ParseInt(t, 10, 64)
			if perr != nil {
				return 0, perr
			}
			n = parsed
		default:
			return 0, errors.New("unexpected redis lua result type")
		}
	}

	if n < 0 {
		return 0, ErrNotInitialized
	}
	return uint64(n), nil
}
