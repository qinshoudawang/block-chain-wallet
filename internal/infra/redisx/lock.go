package redisx

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var (
	ErrLockNotAcquired = errors.New("lock not acquired")
)

type Lock struct {
	rdb   *redis.Client
	key   string
	token string
	ttl   time.Duration
}

func NewLock(rdb *redis.Client, key string, ttl time.Duration) *Lock {
	return &Lock{
		rdb: rdb,
		key: key,
		ttl: ttl,
	}
}

// TryLock: SET key token NX PX ttl
func (l *Lock) TryLock(ctx context.Context) error {
	l.token = uuid.NewString()
	ok, err := l.rdb.SetNX(ctx, l.key, l.token, l.ttl).Result()
	if err != nil {
		return err
	}
	if !ok {
		return ErrLockNotAcquired
	}
	return nil
}

// Unlock: only delete if token matches (Lua)
func (l *Lock) Unlock(ctx context.Context) error {
	const lua = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end`
	_, err := l.rdb.Eval(ctx, lua, []string{l.key}, l.token).Result()
	return err
}

// Acquire acquires a lock and returns an unlock closure.
// The unlock closure uses a background context and should be safe in deferred paths.
func Acquire(ctx context.Context, rdb *redis.Client, key string, ttl time.Duration) (func(), error) {
	if rdb == nil {
		return nil, errors.New("redis is required")
	}
	lock := NewLock(rdb, key, ttl)
	if err := lock.TryLock(ctx); err != nil {
		return nil, err
	}
	return func() { _ = lock.Unlock(context.Background()) }, nil
}
