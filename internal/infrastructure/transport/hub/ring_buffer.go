// Package hub implements high-density per-client ring buffering and connection session management
// for rag-notification-dispatch-daemon.
package hub

import (
	"errors"
	"sync"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
)

const (
	// DefaultRingBufferSize enforces the 256-event non-blocking per-client queue bound.
	DefaultRingBufferSize = entity.MaxConnectionBufferEvents
	// DefaultSlowConsumerTimeout is the 5-second saturation ceiling before socket eviction.
	DefaultSlowConsumerTimeout = entity.SlowConsumerEvictionTimeout

	DropReasonRingBufferSaturated = "ring_buffer_saturated"
	DropReasonTerminalPreemption  = "terminal_frame_preemption"
	DropReasonAllTerminalEviction = "all_terminal_eviction"
	DropReasonSlowConsumerEvicted = "slow_consumer_evicted"
)

var (
	ErrBufferClosed  = errors.New("ring buffer is closed")
	ErrBufferEvicted = errors.New("connection evicted due to slow consumer saturation")
)

// PushStatus represents the granular result of attempting to push an event into the ring buffer.
type PushStatus int

const (
	PushSuccess PushStatus = iota
	PushDroppedSaturated
	PushPreemptedOldestNonTerminal
	PushEvictedAllTerminal
	PushEvictedSlowConsumer
	PushClosed
)

// RingBufferEntry represents a lightweight pre-formatted SSE frame queued in the ring buffer.
type RingBufferEntry struct {
	EventID    string
	Frame      []byte
	IsTerminal bool
}

// RingBuffer provides an isolated, non-blocking 256-event ring buffer per active SSE stream.
// It enforces the terminal retention policy, Slow-Loris eviction (> 5s continuous saturation),
// and non-blocking O(1) enqueueing.
type RingBuffer struct {
	mu                  sync.Mutex
	capacity            int
	buffer              []RingBufferEntry
	head                int
	tail                int
	count               int
	saturatedSince      *time.Time
	slowConsumerTimeout time.Duration
	notifyCh            chan struct{}
	drainCh             chan time.Duration
	closed              bool
	evicted             bool
	evictionReason      string
	tenantID            string
	connectionID        string
	metrics             *observability.MetricsRegistry
}

// NewRingBuffer allocates a 256-capacity ring buffer with ~2KiB baseline footprint.
func NewRingBuffer(
	tenantID, connectionID string,
	capacity int,
	slowConsumerTimeout time.Duration,
	metrics *observability.MetricsRegistry,
) *RingBuffer {
	if capacity <= 0 {
		capacity = DefaultRingBufferSize
	}
	if slowConsumerTimeout <= 0 {
		slowConsumerTimeout = DefaultSlowConsumerTimeout
	}
	return &RingBuffer{
		capacity:            capacity,
		buffer:              make([]RingBufferEntry, capacity),
		slowConsumerTimeout: slowConsumerTimeout,
		notifyCh:            make(chan struct{}, 1),
		drainCh:             make(chan time.Duration, 1),
		tenantID:            tenantID,
		connectionID:        connectionID,
		metrics:             metrics,
	}
}

// Push appends an event to the ring buffer in O(1) non-blocking time, enforcing
// slow-consumer drops and terminal preemption policies.
func (rb *RingBuffer) Push(entry RingBufferEntry, now time.Time) PushStatus {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.closed {
		return PushClosed
	}

	if rb.evicted {
		if rb.evictionReason == DropReasonAllTerminalEviction {
			return PushEvictedAllTerminal
		}
		return PushEvictedSlowConsumer
	}

	// 1. Check continuous saturation duration ceiling (> 5s).
	if rb.count >= rb.capacity && rb.saturatedSince != nil {
		if now.Sub(*rb.saturatedSince) > rb.slowConsumerTimeout {
			rb.markEvictedLocked(DropReasonSlowConsumerEvicted)
			return PushEvictedSlowConsumer
		}
	}

	// 2. Uncongested path: buffer has available capacity.
	if rb.count < rb.capacity {
		rb.buffer[rb.tail] = entry
		rb.tail = (rb.tail + 1) % rb.capacity
		rb.count++
		if rb.count == rb.capacity {
			ts := now.UTC()
			rb.saturatedSince = &ts
		} else {
			rb.saturatedSince = nil
		}
		rb.signalNotifyLocked()
		return PushSuccess
	}

	// 3. Buffer is saturated (count == capacity).
	if rb.saturatedSince == nil {
		ts := now.UTC()
		rb.saturatedSince = &ts
	}

	// Case A: Incoming frame is non-terminal. Drop it to protect terminal states and memory.
	if !entry.IsTerminal {
		if rb.metrics != nil {
			rb.metrics.IncDroppedFrames(rb.tenantID, DropReasonRingBufferSaturated)
		}
		return PushDroppedSaturated
	}

	// Case B: Incoming frame is terminal. Find oldest unread non-terminal frame to preempt.
	oldestNonTerminalIdx := -1
	for i := 0; i < rb.count; i++ {
		idx := (rb.head + i) % rb.capacity
		if !rb.buffer[idx].IsTerminal {
			oldestNonTerminalIdx = i
			break
		}
	}

	if oldestNonTerminalIdx >= 0 {
		// Shift subsequent elements left by 1 to maintain strict FIFO order of remaining frames.
		for j := oldestNonTerminalIdx; j < rb.count-1; j++ {
			curr := (rb.head + j) % rb.capacity
			next := (rb.head + j + 1) % rb.capacity
			rb.buffer[curr] = rb.buffer[next]
		}
		// Place the new terminal frame at the newly vacated end of the queue.
		lastIdx := (rb.head + rb.count - 1) % rb.capacity
		rb.buffer[lastIdx] = entry

		if rb.metrics != nil {
			rb.metrics.IncDroppedFrames(rb.tenantID, DropReasonTerminalPreemption)
		}
		rb.signalNotifyLocked()
		return PushPreemptedOldestNonTerminal
	}

	// Case C: All 256 buffered frames are already terminal. Evict connection immediately.
	rb.markEvictedLocked(DropReasonAllTerminalEviction)
	return PushEvictedAllTerminal
}

// TryPop retrieves the next FIFO frame from the ring buffer in O(1) non-blocking time.
func (rb *RingBuffer) TryPop() (RingBufferEntry, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.count == 0 {
		return RingBufferEntry{}, false
	}

	entry := rb.buffer[rb.head]
	rb.buffer[rb.head] = RingBufferEntry{} // Clear reference to allow GC
	rb.head = (rb.head + 1) % rb.capacity
	rb.count--

	if rb.count < rb.capacity {
		rb.saturatedSince = nil
	}

	return entry, true
}

// CheckStalled evaluates if the buffer has been continuously saturated for longer than
// the slow consumer timeout (> 5s). Called periodically by ConnectionHub background sweep.
func (rb *RingBuffer) CheckStalled(now time.Time) (bool, string) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.evicted {
		return true, rb.evictionReason
	}

	if rb.count >= rb.capacity && rb.saturatedSince != nil {
		if now.Sub(*rb.saturatedSince) > rb.slowConsumerTimeout {
			rb.markEvictedLocked(DropReasonSlowConsumerEvicted)
			return true, DropReasonSlowConsumerEvicted
		}
	}

	return false, ""
}

// markEvictedLocked flags the buffer as evicted and records Prometheus metrics.
func (rb *RingBuffer) markEvictedLocked(reason string) {
	rb.evicted = true
	rb.evictionReason = reason
	if rb.metrics != nil {
		rb.metrics.IncDroppedFrames(rb.tenantID, reason)
	}
	rb.signalNotifyLocked()
}

// signalNotifyLocked performs a non-blocking wakeup send to the consumer goroutine.
func (rb *RingBuffer) signalNotifyLocked() {
	select {
	case rb.notifyCh <- struct{}{}:
	default:
	}
}

// Notify returns a receive-only channel signaled whenever frames are enqueued, evicted, or closed.
func (rb *RingBuffer) Notify() <-chan struct{} {
	return rb.notifyCh
}

// TriggerDrain initiates staggered graceful draining on this stream with the assigned jitter delay.
func (rb *RingBuffer) TriggerDrain(delay time.Duration) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	select {
	case rb.drainCh <- delay:
	default:
	}
	rb.signalNotifyLocked()
}

// DrainDelay returns the channel delivering the staggered drain delay, if triggered.
func (rb *RingBuffer) DrainDelay() <-chan time.Duration {
	return rb.drainCh
}

// Count returns the current number of buffered frames.
func (rb *RingBuffer) Count() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.count
}

// IsEvicted reports whether the buffer has been evicted.
func (rb *RingBuffer) IsEvicted() (bool, string) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.evicted, rb.evictionReason
}

// SaturatedSince returns a copy of the saturation start timestamp, if saturated.
func (rb *RingBuffer) SaturatedSince() *time.Time {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.saturatedSince == nil {
		return nil
	}
	cp := *rb.saturatedSince
	return &cp
}

// Close closes the ring buffer and wakes up any waiting consumer.
func (rb *RingBuffer) Close() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.closed {
		return
	}
	rb.closed = true
	rb.signalNotifyLocked()
}
