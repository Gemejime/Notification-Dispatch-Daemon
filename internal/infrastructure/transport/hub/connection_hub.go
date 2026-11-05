// Package hub implements high-density per-client ring buffering and connection session management
// for rag-notification-dispatch-daemon.
package hub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
)

var (
	ErrConnectionNotFound = errors.New("connection not found")
	ErrNilConnection      = errors.New("connection cannot be nil")
)

// ActiveSession wraps an active client connection, its isolated ring buffer, and cancellation handle.
type ActiveSession struct {
	Conn       entity.Connection
	RingBuffer *RingBuffer
	Cancel     context.CancelFunc
}

// ConnectionHub manages all active local SSE connections for the pod node.
// It implements command.NodeEventDispatcher and orchestrates thread-safe fan-out,
// slow-consumer sweeping, deterministic cursor pagination, and staggered SIGTERM draining (ADR-005).
type ConnectionHub struct {
	mu                sync.RWMutex
	nodeID            string
	byID              map[string]*ActiveSession
	byTenant          map[string]map[string]*ActiveSession
	byUser            map[string]map[string]map[string]*ActiveSession // tenantID -> userID -> connID -> session
	metrics           *observability.MetricsRegistry
	logger            *slog.Logger
	slowConsumerLimit time.Duration
	drainMinJitter    time.Duration
	drainMaxJitter    time.Duration
	closed            bool
}

// NewConnectionHub creates a thread-safe connection hub for the specified pod node.
func NewConnectionHub(
	nodeID string,
	slowConsumerLimit time.Duration,
	drainMinJitter, drainMaxJitter time.Duration,
	metrics *observability.MetricsRegistry,
	logger *slog.Logger,
) *ConnectionHub {
	if slowConsumerLimit <= 0 {
		slowConsumerLimit = DefaultSlowConsumerTimeout
	}
	if drainMinJitter <= 0 {
		drainMinJitter = 30 * time.Second
	}
	if drainMaxJitter <= drainMinJitter {
		drainMaxJitter = 60 * time.Second
	}
	if logger == nil {
		logger = observability.NewSecuredLogger(nil, slog.LevelInfo)
	}
	return &ConnectionHub{
		nodeID:            nodeID,
		byID:              make(map[string]*ActiveSession),
		byTenant:          make(map[string]map[string]*ActiveSession),
		byUser:            make(map[string]map[string]map[string]*ActiveSession),
		metrics:           metrics,
		logger:            logger,
		slowConsumerLimit: slowConsumerLimit,
		drainMinJitter:    drainMinJitter,
		drainMaxJitter:    drainMaxJitter,
	}
}

// Register adds a validated active connection and its associated ring buffer to the hub registries.
func (h *ConnectionHub) Register(conn entity.Connection, rb *RingBuffer, cancel context.CancelFunc) error {
	if err := conn.Validate(); err != nil {
		return fmt.Errorf("validate connection: %w", err)
	}
	if rb == nil {
		return errors.New("ring buffer cannot be nil")
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return errors.New("connection hub is closed")
	}

	sess := &ActiveSession{
		Conn:       conn,
		RingBuffer: rb,
		Cancel:     cancel,
	}

	h.byID[conn.ConnectionID] = sess

	if _, ok := h.byTenant[conn.TenantID]; !ok {
		h.byTenant[conn.TenantID] = make(map[string]*ActiveSession)
	}
	h.byTenant[conn.TenantID][conn.ConnectionID] = sess

	if conn.UserID != "" {
		if _, ok := h.byUser[conn.TenantID]; !ok {
			h.byUser[conn.TenantID] = make(map[string]map[string]*ActiveSession)
		}
		if _, ok := h.byUser[conn.TenantID][conn.UserID]; !ok {
			h.byUser[conn.TenantID][conn.UserID] = make(map[string]*ActiveSession)
		}
		h.byUser[conn.TenantID][conn.UserID][conn.ConnectionID] = sess
	}

	if h.metrics != nil {
		h.metrics.AddActiveSSEConnections(conn.TenantID, conn.NodeID, 1)
	}

	h.logger.Debug("sse connection registered",
		slog.String("connection_id", conn.ConnectionID),
		slog.String("tenant_id", conn.TenantID),
		slog.String("user_id", conn.UserID),
		slog.String("node_id", conn.NodeID),
	)

	return nil
}

// Deregister removes a connection from all local indexes, cleans up resources, and updates metrics.
func (h *ConnectionHub) Deregister(connectionID string) (*entity.Connection, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	sess, ok := h.byID[connectionID]
	if !ok {
		return nil, false
	}

	delete(h.byID, connectionID)

	if tMap, ok := h.byTenant[sess.Conn.TenantID]; ok {
		delete(tMap, connectionID)
		if len(tMap) == 0 {
			delete(h.byTenant, sess.Conn.TenantID)
		}
	}

	if sess.Conn.UserID != "" {
		if tUsers, ok := h.byUser[sess.Conn.TenantID]; ok {
			if uMap, ok := tUsers[sess.Conn.UserID]; ok {
				delete(uMap, connectionID)
				if len(uMap) == 0 {
					delete(tUsers, sess.Conn.UserID)
				}
			}
			if len(tUsers) == 0 {
				delete(h.byUser, sess.Conn.TenantID)
			}
		}
	}

	if h.metrics != nil {
		h.metrics.AddActiveSSEConnections(sess.Conn.TenantID, sess.Conn.NodeID, -1)
	}

	sess.RingBuffer.Close()
	if sess.Cancel != nil {
		sess.Cancel()
	}

	h.logger.Debug("sse connection deregistered",
		slog.String("connection_id", sess.Conn.ConnectionID),
		slog.String("tenant_id", sess.Conn.TenantID),
	)

	return &sess.Conn, true
}

// GetSession returns a copy of the connection and its ring buffer, if active.
func (h *ConnectionHub) GetSession(connectionID string) (*ActiveSession, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	sess, ok := h.byID[connectionID]
	if !ok {
		return nil, false
	}
	return sess, true
}

// TotalActiveConnections returns the count of active local connections on this node.
func (h *ConnectionHub) TotalActiveConnections() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.byID)
}

// TenantActiveConnections returns the count of active local connections for a specific tenant.
func (h *ConnectionHub) TenantActiveConnections(tenantID string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.byTenant[tenantID])
}

// DispatchSSEFrame delivers an event to all matching local connections for the event's tenant.
// Implements command.NodeEventDispatcher.
func (h *ConnectionHub) DispatchSSEFrame(_ context.Context, event entity.NotificationEvent, sseFrame []byte) error {
	now := time.Now().UTC()
	entry := RingBufferEntry{
		EventID:    event.EventID,
		Frame:      sseFrame,
		IsTerminal: event.IsTerminal,
	}

	h.mu.RLock()
	var targets []*ActiveSession
	if event.UserID != "" {
		// Optimization: Event is targeted to a specific user. Query the user index directly.
		if tUsers, ok := h.byUser[event.TenantID]; ok {
			if uMap, ok := tUsers[event.UserID]; ok {
				targets = make([]*ActiveSession, 0, len(uMap))
				for _, sess := range uMap {
					if sess.Conn.AcceptsEvent(event) {
						targets = append(targets, sess)
					}
				}
			}
		}
	} else {
		// Broadcast to all tenant connections that accept this event.
		if tMap, ok := h.byTenant[event.TenantID]; ok {
			targets = make([]*ActiveSession, 0, len(tMap))
			for _, sess := range tMap {
				if sess.Conn.AcceptsEvent(event) {
					targets = append(targets, sess)
				}
			}
		}
	}
	h.mu.RUnlock()

	var toEvict []string
	for _, sess := range targets {
		status := sess.RingBuffer.Push(entry, now)
		if status == PushEvictedSlowConsumer || status == PushEvictedAllTerminal {
			toEvict = append(toEvict, sess.Conn.ConnectionID)
		}
	}

	if len(toEvict) > 0 {
		go func(connIDs []string) {
			for _, cid := range connIDs {
				h.Deregister(cid)
			}
		}(toEvict)
	}

	return nil
}

// SweepSlowConsumers iterates all active sessions and evicts connections continuously
// saturated for longer than the slow-consumer ceiling (> 5s).
func (h *ConnectionHub) SweepSlowConsumers(now time.Time) []string {
	h.mu.RLock()
	var candidates []*ActiveSession
	for _, sess := range h.byID {
		candidates = append(candidates, sess)
	}
	h.mu.RUnlock()

	var evictedIDs []string
	for _, sess := range candidates {
		stalled, reason := sess.RingBuffer.CheckStalled(now)
		if stalled {
			h.logger.Warn("evicting slow-consumer sse connection",
				slog.String("connection_id", sess.Conn.ConnectionID),
				slog.String("tenant_id", sess.Conn.TenantID),
				slog.String("reason", reason),
			)
			h.Deregister(sess.Conn.ConnectionID)
			evictedIDs = append(evictedIDs, sess.Conn.ConnectionID)
		}
	}

	return evictedIDs
}

// StartSlowConsumerSweeper starts a background loop that periodically sweeps and evicts slow consumers.
func (h *ConnectionHub) StartSlowConsumerSweeper(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 1 * time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				_ = h.SweepSlowConsumers(t.UTC())
			}
		}
	}()
}

// DrainWithJitter performs per-connection staggered graceful draining on SIGTERM (ADR-005).
// It assigns each active stream its own independent randomized jitter delay in [minJitter, maxJitter],
// signals the stream to emit an SSE `retry: <jitter_ms>\n\n` directive frame, and schedules socket closure.
func (h *ConnectionHub) DrainWithJitter(ctx context.Context, minJitter, maxJitter time.Duration) <-chan struct{} {
	if minJitter <= 0 {
		minJitter = h.drainMinJitter
	}
	if maxJitter <= minJitter {
		maxJitter = h.drainMaxJitter
	}

	h.mu.Lock()
	h.closed = true
	var sessions []*ActiveSession
	for _, sess := range h.byID {
		sessions = append(sessions, sess)
	}
	h.mu.Unlock()

	doneCh := make(chan struct{})
	if len(sessions) == 0 {
		close(doneCh)
		return doneCh
	}

	var wg sync.WaitGroup
	spread := int64(maxJitter - minJitter)

	for _, sess := range sessions {
		wg.Add(1)
		// Assign independent randomized jitter delay per stream to prevent thundering herds.
		var jitterDelay time.Duration
		if spread > 0 {
			jitterDelay = minJitter + time.Duration(rand.Int64N(spread+1))
		} else {
			jitterDelay = minJitter
		}

		go func(s *ActiveSession, delay time.Duration) {
			defer wg.Done()
			select {
			case <-time.After(delay):
			case <-ctx.Done():
			}
			s.RingBuffer.TriggerDrain(delay)
			select {
			case <-s.RingBuffer.DrainDone():
			case <-ctx.Done():
			case <-time.After(500 * time.Millisecond):
			}
			h.Deregister(s.Conn.ConnectionID)
		}(sess, jitterDelay)
	}

	go func() {
		wg.Wait()
		close(doneCh)
	}()

	return doneCh
}

// ListActiveSessions returns a deterministic cursor-paginated view of active sessions for a tenant.
func (h *ConnectionHub) ListActiveSessions(tenantID string, limit int, cursor string) ([]entity.Connection, string, int) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}

	h.mu.RLock()
	tMap, ok := h.byTenant[tenantID]
	if !ok || len(tMap) == 0 {
		h.mu.RUnlock()
		return []entity.Connection{}, "", 0
	}

	total := len(tMap)
	all := make([]entity.Connection, 0, total)
	for _, sess := range tMap {
		c := sess.Conn
		c.BufferedEvents = sess.RingBuffer.Count()
		all = append(all, c)
	}
	h.mu.RUnlock()

	// Deterministic sort by ConnectionID
	sort.Slice(all, func(i, j int) bool {
		return all[i].ConnectionID < all[j].ConnectionID
	})

	startIdx := 0
	if cursor != "" {
		for i, c := range all {
			if c.ConnectionID > cursor {
				startIdx = i
				break
			}
			if i == len(all)-1 {
				startIdx = len(all)
			}
		}
	}

	endIdx := startIdx + limit
	if endIdx > len(all) {
		endIdx = len(all)
	}

	if startIdx >= len(all) {
		return []entity.Connection{}, "", total
	}

	paged := all[startIdx:endIdx]
	var nextCursor string
	if endIdx < len(all) {
		nextCursor = paged[len(paged)-1].ConnectionID
	}

	return paged, nextCursor, total
}
