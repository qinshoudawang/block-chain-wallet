package nonce

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrNotInitialized = errors.New("nonce manager not initialized")
)

type InitNonceProvider = func(context.Context) (uint64, error)
type NonceFloorProvider = func(context.Context) (uint64, error)

// Manager manages nonce for a single (chain, account) pair.
// It is chain-agnostic; initProvider supplies chain-specific initial nonce.
type Manager struct {
	rdb                *redis.Client
	chain              string
	account            string
	ttl                time.Duration
	initProvider       InitNonceProvider
	nonceFloorProvider NonceFloorProvider
}

func NewManager(rdb *redis.Client, chain string, account string, initProvider InitNonceProvider) *Manager {
	return &Manager{
		rdb:          rdb,
		chain:        chain,
		account:      account,
		ttl:          0,
		initProvider: initProvider,
	}
}

func (m *Manager) WithNonceFloorProvider(fn NonceFloorProvider) *Manager {
	m.nonceFloorProvider = fn
	return m
}

func (m *Manager) nonceKey() string {
	return fmt.Sprintf("nonce:%s:%s:next", m.chain, m.account)
}

func (m *Manager) EnsureInitialized(ctx context.Context) error {
	if m == nil || m.rdb == nil {
		return errors.New("redis client is required")
	}
	if m.initProvider == nil {
		return errors.New("nonce init provider is required")
	}
	key := m.nonceKey()

	exists, err := m.rdb.Exists(ctx, key).Result()
	if err != nil {
		return err
	}
	if exists == 1 {
		return nil
	}

	n, err := m.initProvider(ctx)
	if err != nil {
		return err
	}

	// 取DB和链上的最大值
	if m.nonceFloorProvider != nil {
		floor, err := m.nonceFloorProvider(ctx)
		if err != nil {
			return err
		}
		if floor > n {
			n = floor
		}
	}

	ok, err := m.rdb.SetNX(ctx, key, n, m.ttl).Result()
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	return nil
}

func (m *Manager) EnsureAtLeast(ctx context.Context, minNonce uint64) error {
	key := m.nonceKey()
	const lua = `
local minv = tonumber(ARGV[1])
local v = redis.call("GET", KEYS[1])
if not v then
  redis.call("SET", KEYS[1], minv)
  return minv
end
local cur = tonumber(v)
if cur < minv then
  redis.call("SET", KEYS[1], minv)
  return minv
end
return cur
`
	_, err := m.rdb.Eval(ctx, lua, []string{key}, minNonce).Result()
	return err
}

func (m *Manager) Allocate(ctx context.Context) (uint64, error) {
	key := m.nonceKey()
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
