package idempotency

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrDuplicateProcessing = errors.New("duplicate request: still processing")
	ErrDuplicateNoResult   = errors.New("duplicate request: no result")
)

type Guard struct {
	rdb *redis.Client
	ttl time.Duration
}

func New(rdb *redis.Client, ttl time.Duration) *Guard {
	return &Guard{rdb: rdb, ttl: ttl}
}

// Keys:
// sign:req:{id}:state = processing|done
// sign:req:{id}:result = <base64 or raw bytes>
func stateKey(id string) string  { return "sign:req:" + id + ":state" }
func resultKey(id string) string { return "sign:req:" + id + ":result" }

// Check returns (okToProceed, cachedResult, err) without mutating idempotency state.
func (g *Guard) Check(ctx context.Context, requestID string) (bool, []byte, error) {
	st, err := g.rdb.Get(ctx, stateKey(requestID)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return true, nil, nil
		}
		return false, nil, err
	}

	if st == "done" {
		res, err := g.rdb.Get(ctx, resultKey(requestID)).Bytes()
		if err != nil {
			return false, nil, ErrDuplicateNoResult
		}
		return false, res, nil
	}
	if st == "processing" {
		return false, nil, ErrDuplicateProcessing
	}
	return false, nil, errors.New("idempotency state inconsistent")
}

// Begin returns (okToProceed, cachedResult, err)
func (g *Guard) Begin(ctx context.Context, requestID string) (bool, []byte, error) {
	// Try create state=processing (NX)
	ok, err := g.rdb.SetNX(ctx, stateKey(requestID), "processing", g.ttl).Result()
	if err != nil {
		return false, nil, err
	}
	if ok {
		return true, nil, nil // proceed
	}

	// Not ok: already exists. Check state.
	st, err := g.rdb.Get(ctx, stateKey(requestID)).Result()
	if err != nil {
		// key disappeared (rare), treat as retryable
		return false, nil, err
	}

	if st == "done" {
		res, err := g.rdb.Get(ctx, resultKey(requestID)).Bytes()
		if err != nil {
			return false, nil, ErrDuplicateNoResult
		}
		return false, res, nil // return cached result
	}

	// processing
	return false, nil, ErrDuplicateProcessing
}

func (g *Guard) Finish(ctx context.Context, requestID string, result []byte) error {
	pipe := g.rdb.TxPipeline()
	pipe.Set(ctx, resultKey(requestID), result, g.ttl)
	pipe.Set(ctx, stateKey(requestID), "done", g.ttl)
	_, err := pipe.Exec(ctx)
	return err
}
