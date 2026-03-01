package utxoreserve

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	storagerepo "wallet-system/internal/storage/repo"

	"github.com/redis/go-redis/v9"
)

type Manager struct {
	rdb  *redis.Client
	repo *storagerepo.UTXOReservationRepo
	ttl  time.Duration
}

func NewManager(rdb *redis.Client, repo *storagerepo.UTXOReservationRepo, ttl time.Duration) *Manager {
	if ttl <= 0 {
		ttl = 2 * time.Hour
	}
	return &Manager{
		rdb:  rdb,
		repo: repo,
		ttl:  ttl,
	}
}

func (m *Manager) ListReservedForAccount(ctx context.Context, chain string, address string) (map[string]struct{}, error) {
	if m == nil {
		return nil, nil
	}
	if m.rdb == nil {
		return m.listReservedFromDB(ctx, chain, address)
	}
	accountKey := accountRedisKey(chain, address)
	keys, err := m.rdb.HKeys(ctx, accountKey).Result()
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 && m.repo != nil {
		return m.loadReservedFromDBToRedis(ctx, chain, address, accountKey)
	}
	return toSet(keys), nil
}

func (m *Manager) listReservedFromDB(ctx context.Context, chain, address string) (map[string]struct{}, error) {
	if m.repo == nil {
		return nil, nil
	}
	keys, err := m.repo.ListReservedKeysByAccount(ctx, chain, address)
	if err != nil {
		return nil, err
	}
	return toSet(keys), nil
}

func (m *Manager) loadReservedFromDBToRedis(ctx context.Context, chain, address, accountKey string) (map[string]struct{}, error) {
	rows, err := m.repo.ListReservedByAccount(ctx, chain, address)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	pipe := m.rdb.TxPipeline()
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		k := strings.TrimSpace(row.UTXOKey)
		owner := strings.TrimSpace(row.WithdrawID)
		if k == "" || owner == "" {
			continue
		}
		keys = append(keys, k)
		pipe.HSet(ctx, accountKey, k, owner)
		pipe.HSet(ctx, withdrawRedisKey(owner), k, accountKey)
		pipe.Expire(ctx, withdrawRedisKey(owner), m.ttl)
	}
	pipe.Expire(ctx, accountKey, m.ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	return toSet(keys), nil
}

func toSet(keys []string) map[string]struct{} {
	out := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		k = strings.TrimSpace(k)
		if k == "" {
			continue
		}
		out[k] = struct{}{}
	}
	return out
}

// ReserveByWithdrawID atomically reserves a list of UTXO keys for the same (chain,address).
// It fails when any requested UTXO is already reserved by another withdraw.
func (m *Manager) ReserveByWithdrawID(ctx context.Context, chain string, address string, withdrawID string, utxoKeys []string) error {
	if m == nil || m.rdb == nil {
		return nil
	}
	withdrawID = strings.TrimSpace(withdrawID)
	if withdrawID == "" {
		return errors.New("withdraw id is required")
	}
	deduped := dedupeUTXOKeys(utxoKeys)
	if len(deduped) == 0 {
		return errors.New("empty utxo keys")
	}

	accountKey := accountRedisKey(chain, address)
	withdrawKey := withdrawRedisKey(withdrawID)

	ttlMillis := m.ttl.Milliseconds()
	if ttlMillis <= 0 {
		ttlMillis = int64((2 * time.Hour).Milliseconds())
	}

	const lua = `
local account_key = KEYS[1]
local withdraw_key = KEYS[2]
local withdraw_id = ARGV[1]
local ttl_ms = tonumber(ARGV[2])

for i = 3, #ARGV do
  local utxo = ARGV[i]
  local owner = redis.call("HGET", account_key, utxo)
  if owner and owner ~= withdraw_id then
    return 0
  end
end

for i = 3, #ARGV do
  local utxo = ARGV[i]
  redis.call("HSET", account_key, utxo, withdraw_id)
  redis.call("HSET", withdraw_key, utxo, account_key)
end

redis.call("PEXPIRE", account_key, ttl_ms)
redis.call("PEXPIRE", withdraw_key, ttl_ms)
return 1
`

	args := make([]any, 0, len(deduped)+2)
	args = append(args, withdrawID, ttlMillis)
	for _, k := range deduped {
		args = append(args, k)
	}
	res, err := m.rdb.Eval(ctx, lua, []string{accountKey, withdrawKey}, args...).Int()
	if err != nil {
		return err
	}
	if res != 1 {
		return errors.New("utxo already reserved")
	}
	if m.repo != nil {
		if err := m.repo.UpsertReserved(ctx, chain, address, withdrawID, deduped); err != nil {
			// Roll back redis reservation on DB persistence failure.
			_ = m.releaseRedisByWithdrawID(ctx, withdrawID)
			return err
		}
	}
	return nil
}

func (m *Manager) ReleaseByWithdrawID(ctx context.Context, withdrawID string) error {
	if m == nil {
		return nil
	}
	withdrawID = strings.TrimSpace(withdrawID)
	if withdrawID == "" {
		return nil
	}
	if m.repo != nil {
		if err := m.repo.ReleaseByWithdrawID(ctx, withdrawID); err != nil {
			return err
		}
	}
	return m.releaseRedisByWithdrawID(ctx, withdrawID)
}

func (m *Manager) releaseRedisByWithdrawID(ctx context.Context, withdrawID string) error {
	if m == nil || m.rdb == nil {
		return nil
	}
	withdrawKey := withdrawRedisKey(withdrawID)

	const lua = `
local withdraw_key = KEYS[1]
local withdraw_id = ARGV[1]
local pairs = redis.call("HGETALL", withdraw_key)

for i = 1, #pairs, 2 do
  local utxo = pairs[i]
  local account_key = pairs[i + 1]
  local owner = redis.call("HGET", account_key, utxo)
  if owner == withdraw_id then
    redis.call("HDEL", account_key, utxo)
  end
end

redis.call("DEL", withdraw_key)
return 1
`
	_, err := m.rdb.Eval(ctx, lua, []string{withdrawKey}, withdrawID).Result()
	return err
}

func accountRedisKey(chain string, address string) string {
	return fmt.Sprintf("utxo:reserved:acct:%s:%s", strings.ToLower(strings.TrimSpace(chain)), strings.ToLower(strings.TrimSpace(address)))
}

func withdrawRedisKey(withdrawID string) string {
	return "utxo:reserved:wd:" + strings.TrimSpace(withdrawID)
}

func dedupeUTXOKeys(keys []string) []string {
	seen := make(map[string]struct{}, len(keys))
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		k = strings.TrimSpace(k)
		if k == "" {
			continue
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, k)
	}
	return out
}
