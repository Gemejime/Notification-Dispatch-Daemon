package router

import (
	"sync"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
)

const (
	// DefaultDedupWindowTTL is the retention window for (tenant_id, event_id) idempotency deduplication.
	DefaultDedupWindowTTL = 5 * time.Minute
	// DefaultMaxDedupEntries bounds the in-memory deduplication map to protect GOMEMLIMIT=1800MiB.
	DefaultMaxDedupEntries = 65536
)

type tokenBucket struct {
	tokens     float64
	capacity   float64
	refillRate float64
	lastRefill time.Time
}

// Filter enforces bounded-memory (tenant_id, event_id) idempotency deduplication
// and per-tenant token-bucket burst throttling on the hot ingestion path.
type Filter struct {
	mu              sync.Mutex
	dedupTTL        time.Duration
	maxDedupEntries int
	seenEvents      map[string]time.Time
	insertionOrder  []string
	tenantBuckets   map[string]*tokenBucket
}

// NewFilter initializes a bounded Filter with the specified deduplication TTL and maximum entry ceiling.
func NewFilter(dedupTTL time.Duration, maxDedupEntries int) *Filter {
	if dedupTTL <= 0 {
		dedupTTL = DefaultDedupWindowTTL
	}
	if maxDedupEntries <= 0 {
		maxDedupEntries = DefaultMaxDedupEntries
	}
	return &Filter{
		dedupTTL:        dedupTTL,
		maxDedupEntries: maxDedupEntries,
		seenEvents:      make(map[string]time.Time, 1024),
		insertionOrder:  make([]string, 0, 1024),
		tenantBuckets:   make(map[string]*tokenBucket, 256),
	}
}

// CheckAndRecordDuplicate atomically checks whether (tenantID, eventID) was already processed
// within the deduplication TTL window, and records it if not.
func (f *Filter) CheckAndRecordDuplicate(tenantID, eventID string, now time.Time) bool {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	key := tenantID + ":" + eventID

	f.mu.Lock()
	defer f.mu.Unlock()

	if exp, exists := f.seenEvents[key]; exists {
		if now.Before(exp) {
			return true
		}
		delete(f.seenEvents, key)
	}

	if len(f.seenEvents) >= f.maxDedupEntries {
		f.evictExpiredOrOldestLocked(now)
	}

	f.seenEvents[key] = now.Add(f.dedupTTL)
	f.insertionOrder = append(f.insertionOrder, key)
	return false
}

// AllowTenantBurst evaluates a per-tenant token-bucket rate limiter configured at rateLimitRPS.
// Terminal events (is_terminal == true) are always allowed through to guarantee terminal state delivery.
func (f *Filter) AllowTenantBurst(tenantID string, rateLimitRPS int, isTerminal bool, now time.Time) bool {
	if isTerminal {
		return true
	}
	if rateLimitRPS <= 0 {
		rateLimitRPS = entity.DefaultSubscriptionRateLimitRPS
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	rate := float64(rateLimitRPS)
	bucket, exists := f.tenantBuckets[tenantID]
	if !exists {
		f.tenantBuckets[tenantID] = &tokenBucket{
			tokens:     rate - 1,
			capacity:   rate,
			refillRate: rate,
			lastRefill: now,
		}
		return true
	}

	bucket.capacity = rate
	bucket.refillRate = rate

	elapsed := now.Sub(bucket.lastRefill).Seconds()
	if elapsed > 0 {
		bucket.tokens += elapsed * bucket.refillRate
		if bucket.tokens > bucket.capacity {
			bucket.tokens = bucket.capacity
		}
		bucket.lastRefill = now
	}

	if bucket.tokens < 1.0 {
		return false
	}
	bucket.tokens -= 1.0
	return true
}

// DedupSize returns the current number of entries tracked in the deduplication window.
func (f *Filter) DedupSize() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.seenEvents)
}

func (f *Filter) evictExpiredOrOldestLocked(now time.Time) {
	// First sweep expired keys from the front of the insertion queue.
	cutoff := 0
	for cutoff < len(f.insertionOrder) {
		k := f.insertionOrder[cutoff]
		exp, exists := f.seenEvents[k]
		if !exists {
			cutoff++
			continue
		}
		if now.After(exp) {
			delete(f.seenEvents, k)
			cutoff++
			continue
		}
		break
	}
	if cutoff > 0 {
		f.insertionOrder = f.insertionOrder[cutoff:]
	}

	// If still at capacity, evict the oldest 10% of entries to bound memory strictly.
	for len(f.seenEvents) >= f.maxDedupEntries && len(f.insertionOrder) > 0 {
		oldestKey := f.insertionOrder[0]
		f.insertionOrder = f.insertionOrder[1:]
		delete(f.seenEvents, oldestKey)
	}
}
