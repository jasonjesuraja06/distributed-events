package idempotency

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// fakeStore implements Store with real SETNX semantics: the key is written only
// if absent and unexpired, and the write carries an expiry. now() is injectable
// so TTL expiry can be tested without sleeping.
type fakeStore struct {
	mu      sync.Mutex
	entries map[string]time.Time // key -> expires at
	now     func() time.Time
	setErr  error
	delErr  error
	setNXs  int
	dels    int
}

func newFakeStore() *fakeStore {
	return &fakeStore{entries: map[string]time.Time{}, now: time.Now}
}

func (f *fakeStore) SetNX(ctx context.Context, k string, _ any, ttl time.Duration) *redis.BoolCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	cmd := redis.NewBoolCmd(ctx)
	f.setNXs++
	if f.setErr != nil {
		cmd.SetErr(f.setErr)
		return cmd
	}
	if exp, ok := f.entries[k]; ok && exp.After(f.now()) {
		cmd.SetVal(false)
		return cmd
	}
	f.entries[k] = f.now().Add(ttl)
	cmd.SetVal(true)
	return cmd
}

func (f *fakeStore) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	cmd := redis.NewIntCmd(ctx)
	f.dels++
	if f.delErr != nil {
		cmd.SetErr(f.delErr)
		return cmd
	}
	var n int64
	for _, k := range keys {
		if _, ok := f.entries[k]; ok {
			delete(f.entries, k)
			n++
		}
	}
	cmd.SetVal(n)
	return cmd
}

func (f *fakeStore) has(k string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.entries[k]
	return ok
}

func TestTryClaimFirstWinsSecondIsSuppressed(t *testing.T) {
	d := New(newFakeStore(), time.Hour)
	ctx := context.Background()

	first, err := d.TryClaim(ctx, "order-42")
	if err != nil || !first {
		t.Fatalf("first claim: got (%v, %v), want (true, nil)", first, err)
	}
	second, err := d.TryClaim(ctx, "order-42")
	if err != nil {
		t.Fatalf("second claim error: %v", err)
	}
	if second {
		t.Fatal("second claim on the same key succeeded; downstream would see a duplicate")
	}
}

func TestTryClaimDistinctKeysDoNotCollide(t *testing.T) {
	d := New(newFakeStore(), time.Hour)
	ctx := context.Background()
	for _, k := range []string{"a", "b", "c", "order-1", "order-2"} {
		ok, err := d.TryClaim(ctx, k)
		if err != nil || !ok {
			t.Fatalf("claim %q: got (%v, %v), want (true, nil)", k, ok, err)
		}
	}
}

func TestClaimIsReleasableSoARetryCanReclaim(t *testing.T) {
	f := newFakeStore()
	d := New(f, time.Hour)
	ctx := context.Background()

	if ok, _ := d.TryClaim(ctx, "order-42"); !ok {
		t.Fatal("first claim should win")
	}
	if ok, _ := d.TryClaim(ctx, "order-42"); ok {
		t.Fatal("second claim should be suppressed while the slot is held")
	}
	// This is the consumer's failure path: the downstream call failed, so the
	// slot is released and the event becomes eligible again.
	if err := d.Release(ctx, "order-42"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if f.has(key("order-42")) {
		t.Fatal("release left the key in the store")
	}
	if ok, err := d.TryClaim(ctx, "order-42"); err != nil || !ok {
		t.Fatalf("claim after release: got (%v, %v), want (true, nil)", ok, err)
	}
}

func TestReleaseOfUnheldKeyIsNotAnError(t *testing.T) {
	d := New(newFakeStore(), time.Hour)
	if err := d.Release(context.Background(), "never-claimed"); err != nil {
		t.Fatalf("release of absent key: %v", err)
	}
}

func TestClaimIsReleasedByTTLExpiry(t *testing.T) {
	f := newFakeStore()
	base := time.Unix(1_700_000_000, 0)
	clock := base
	f.now = func() time.Time { return clock }
	d := New(f, 30*time.Second)
	ctx := context.Background()

	if ok, _ := d.TryClaim(ctx, "order-42"); !ok {
		t.Fatal("first claim should win")
	}
	clock = base.Add(29 * time.Second)
	if ok, _ := d.TryClaim(ctx, "order-42"); ok {
		t.Fatal("claim inside the TTL window should still be suppressed")
	}
	clock = base.Add(31 * time.Second)
	if ok, _ := d.TryClaim(ctx, "order-42"); !ok {
		t.Fatal("claim after the TTL window should be allowed; dedup is at-most-once within TTL only")
	}
}

func TestTryClaimReportsStoreErrorAndDoesNotClaim(t *testing.T) {
	f := newFakeStore()
	f.setErr = errors.New("redis down")
	d := New(f, time.Hour)

	ok, err := d.TryClaim(context.Background(), "order-42")
	if err == nil {
		t.Fatal("store error must surface; the consumer counts it and drops the event")
	}
	if ok {
		t.Fatal("a failed SETNX must not report a won claim")
	}
}

func TestReleaseSurfacesStoreError(t *testing.T) {
	f := newFakeStore()
	f.delErr = errors.New("redis down")
	if err := New(f, time.Hour).Release(context.Background(), "order-42"); err == nil {
		t.Fatal("Del error must surface")
	}
}

func TestConcurrentClaimsElectExactlyOneWinner(t *testing.T) {
	f := newFakeStore()
	d := New(f, time.Hour)
	const racers = 64

	var wg sync.WaitGroup
	wins := make(chan bool, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := d.TryClaim(context.Background(), "hot-key")
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			wins <- ok
		}()
	}
	wg.Wait()
	close(wins)

	won := 0
	for ok := range wins {
		if ok {
			won++
		}
	}
	if won != 1 {
		t.Fatalf("got %d winners across %d concurrent claims, want exactly 1", won, racers)
	}
}

func TestFormatHexKeyShape(t *testing.T) {
	got := formatHexKey(0)
	if want := "dedup:0000000000000000"; got != want {
		t.Fatalf("formatHexKey(0) = %q, want %q", got, want)
	}
	got = formatHexKey(^uint64(0))
	if want := "dedup:ffffffffffffffff"; got != want {
		t.Fatalf("formatHexKey(max) = %q, want %q", got, want)
	}
	got = formatHexKey(0x0123456789abcdef)
	if want := "dedup:0123456789abcdef"; got != want {
		t.Fatalf("formatHexKey(0x0123456789abcdef) = %q, want %q", got, want)
	}
}

// The colon matters: docs and operational commands scan Redis with the glob
// "dedup:*", which matches nothing if the prefix is malformed.
func TestFormatHexKeyKeepsTheColonSeparator(t *testing.T) {
	for _, h := range []uint64{0, 1, 0xdeadbeef, ^uint64(0)} {
		k := formatHexKey(h)
		if !strings.HasPrefix(k, "dedup:") {
			t.Fatalf("formatHexKey(%#x) = %q, want a \"dedup:\" prefix", h, k)
		}
		if len(k) != len("dedup:")+16 {
			t.Fatalf("formatHexKey(%#x) = %q, want length %d", h, k, len("dedup:")+16)
		}
		if strings.ContainsAny(strings.TrimPrefix(k, "dedup:"), "ghijklmnopqrstuvwxyz:") {
			t.Fatalf("formatHexKey(%#x) = %q, suffix is not pure hex", h, k)
		}
	}
}

func TestKeyIsDeterministicAndCollisionFreeOverSamples(t *testing.T) {
	seen := map[string]string{}
	for i := 0; i < 5000; i++ {
		in := "idem-" + strconv.Itoa(i)
		k := key(in)
		if k != key(in) {
			t.Fatalf("key(%q) is not deterministic", in)
		}
		if prev, dup := seen[k]; dup {
			t.Fatalf("key collision: %q and %q both hash to %q", prev, in, k)
		}
		seen[k] = in
	}
}
