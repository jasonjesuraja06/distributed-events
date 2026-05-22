package idempotency

import (
	"context"
	"errors"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/redis/go-redis/v9"
)

// Deduper guarantees at-most-once delivery downstream for a given IdempotencyKey
// within the TTL window. SETNX is atomic on Redis; the first claim wins and any
// concurrent consumer that observes !Acquired skips downstream invocation.
type Deduper struct {
	rdb *redis.Client
	ttl time.Duration
}

func New(rdb *redis.Client, ttl time.Duration) *Deduper {
	return &Deduper{rdb: rdb, ttl: ttl}
}

func key(idempotencyKey string) string {
	h := xxhash.Sum64String(idempotencyKey)
	return formatHexKey(h)
}

func formatHexKey(h uint64) string {
	const hex = "0123456789abcdef"
	out := make([]byte, 5+16)
	copy(out, "dedup:")
	for i := 0; i < 16; i++ {
		out[5+i] = hex[(h>>uint(60-4*i))&0xf]
	}
	return string(out[:21])
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

// Release manually frees the dedup slot (for negative-cache use cases). Optional.
func (d *Deduper) Release(ctx context.Context, idempotencyKey string) error {
	_, err := d.rdb.Del(ctx, key(idempotencyKey)).Result()
	if errors.Is(err, redis.Nil) {
		return nil
	}
	return err
}
