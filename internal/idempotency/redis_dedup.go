package idempotency

import (
	"context"
	"errors"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/redis/go-redis/v9"
)

// Store is the subset of the Redis client the deduper uses. *redis.Client
// satisfies it; tests substitute an in-memory implementation with the same
// SETNX semantics so they need no running Redis.
type Store interface {
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
}

// Deduper guarantees at-most-once delivery downstream for a given IdempotencyKey
// within the TTL window. SETNX is atomic on Redis; the first claim wins and any
// concurrent consumer that observes !Acquired skips downstream invocation.
type Deduper struct {
	rdb Store
	ttl time.Duration
}

func New(rdb Store, ttl time.Duration) *Deduper {
	return &Deduper{rdb: rdb, ttl: ttl}
}

func key(idempotencyKey string) string {
	h := xxhash.Sum64String(idempotencyKey)
	return formatHexKey(h)
}

// formatHexKey renders "dedup:" followed by the 16 lowercase hex digits of h,
// without allocating through fmt. The prefix is 6 bytes, so the digits start at
// index 6 and the result is always 22 bytes.
func formatHexKey(h uint64) string {
	const hex = "0123456789abcdef"
	const prefix = "dedup:"
	out := make([]byte, len(prefix)+16)
	copy(out, prefix)
	for i := 0; i < 16; i++ {
		out[len(prefix)+i] = hex[(h>>uint(60-4*i))&0xf]
	}
	return string(out)
}

// TryClaim returns (true, nil) when this consumer wins the dedup race for the key.
// (false, nil) means another consumer already processed (or is processing) this key
// and the caller must NOT invoke downstream.
func (d *Deduper) TryClaim(ctx context.Context, idempotencyKey string) (bool, error) {
	ok, err := d.rdb.SetNX(ctx, key(idempotencyKey), "1", d.ttl).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}

// Release frees the dedup slot so a later attempt at the same key can re-claim
// it. The consumer calls this when the downstream call failed or the breaker
// rejected it, so a failed delivery does not permanently suppress the key.
func (d *Deduper) Release(ctx context.Context, idempotencyKey string) error {
	_, err := d.rdb.Del(ctx, key(idempotencyKey)).Result()
	if errors.Is(err, redis.Nil) {
		return nil
	}
	return err
}
