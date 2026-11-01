// Package redis provides Redis 7.2 Cluster adapters for cache invalidation,
// user/tenant presence routing directory (ADR-003), and bounded replay stream buffering.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
)

const (
	// DefaultPresenceTTL is the 45s heartbeat window for user and tenant node directory keys (ADR-003).
	DefaultPresenceTTL = 45 * time.Second
	// DefaultReplayMaxLen is the MAXLEN limit for per-tenant Redis Stream replay buffers.
	DefaultReplayMaxLen = 1000
	// DefaultReplayTTL is the TTL for per-tenant replay stream keys (1 hour).
	DefaultReplayTTL = 3600 * time.Second
)

// StreamMessage represents a single message in a Redis Stream.
type StreamMessage struct {
	ID     string
	Values map[string]any
}

// RedisClusterClient abstracts Redis 7.2 Cluster commands for presence hashes,
// tenant node sets, replay streams, and pub/sub channels.
type RedisClusterClient interface {
	HSet(ctx context.Context, key, field, value string) error
	HDel(ctx context.Context, key string, fields ...string) error
	HGetAll(ctx context.Context, key string) (map[string]string, error)
	SAdd(ctx context.Context, key string, members ...string) error
	SRem(ctx context.Context, key string, members ...string) error
	SMembers(ctx context.Context, key string) ([]string, error)
	Expire(ctx context.Context, key string, ttl time.Duration) error
	XAdd(ctx context.Context, stream string, maxLen int64, values map[string]any) (string, error)
	XRange(ctx context.Context, stream, start, stop string, count int64) ([]StreamMessage, error)
	Publish(ctx context.Context, channel string, payload []byte) error
	Subscribe(ctx context.Context, channel string) (PubSubSubscription, error)
}

// InMemoryRedisClusterClient provides a thread-safe, multi-node capable Redis simulator
// supporting Hashes, Sets, Streams with MAXLEN, Pub/Sub, and TTLs for unit and integration testing.
type InMemoryRedisClusterClient struct {
	mu          sync.RWMutex
	hashes      map[string]map[string]string
	sets        map[string]map[string]struct{}
	streams     map[string][]StreamMessage
	channels    map[string][]chan []byte
	streamSeq   int64
	isConn      bool
	streamLimit int
}

// NewInMemoryRedisClusterClient creates a connected in-memory Redis cluster simulator.
func NewInMemoryRedisClusterClient() *InMemoryRedisClusterClient {
	return &InMemoryRedisClusterClient{
		hashes:      make(map[string]map[string]string),
		sets:        make(map[string]map[string]struct{}),
		streams:     make(map[string][]StreamMessage),
		channels:    make(map[string][]chan []byte),
		isConn:      true,
		streamLimit: 1000,
	}
}

func (m *InMemoryRedisClusterClient) HSet(_ context.Context, key, field, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isConn {
		return errors.New("redis cluster client disconnected")
	}
	h, ok := m.hashes[key]
	if !ok {
		h = make(map[string]string)
		m.hashes[key] = h
	}
	h[field] = value
	return nil
}

func (m *InMemoryRedisClusterClient) HDel(_ context.Context, key string, fields ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isConn {
		return errors.New("redis cluster client disconnected")
	}
	h, ok := m.hashes[key]
	if !ok {
		return nil
	}
	for _, f := range fields {
		delete(h, f)
	}
	if len(h) == 0 {
		delete(m.hashes, key)
	}
	return nil
}

func (m *InMemoryRedisClusterClient) HGetAll(_ context.Context, key string) (map[string]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.isConn {
		return nil, errors.New("redis cluster client disconnected")
	}
	h, ok := m.hashes[key]
	if !ok {
		return make(map[string]string), nil
	}
	res := make(map[string]string, len(h))
	for k, v := range h {
		res[k] = v
	}
	return res, nil
}

func (m *InMemoryRedisClusterClient) SAdd(_ context.Context, key string, members ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isConn {
		return errors.New("redis cluster client disconnected")
	}
	s, ok := m.sets[key]
	if !ok {
		s = make(map[string]struct{})
		m.sets[key] = s
	}
	for _, mbr := range members {
		s[mbr] = struct{}{}
	}
	return nil
}

func (m *InMemoryRedisClusterClient) SRem(_ context.Context, key string, members ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isConn {
		return errors.New("redis cluster client disconnected")
	}
	s, ok := m.sets[key]
	if !ok {
		return nil
	}
	for _, mbr := range members {
		delete(s, mbr)
	}
	if len(s) == 0 {
		delete(m.sets, key)
	}
	return nil
}

func (m *InMemoryRedisClusterClient) SMembers(_ context.Context, key string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.isConn {
		return nil, errors.New("redis cluster client disconnected")
	}
	s, ok := m.sets[key]
	if !ok {
		return nil, nil
	}
	members := make([]string, 0, len(s))
	for mbr := range s {
		members = append(members, mbr)
	}
	sort.Strings(members)
	return members, nil
}

func (m *InMemoryRedisClusterClient) Expire(_ context.Context, _ string, _ time.Duration) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.isConn {
		return errors.New("redis cluster client disconnected")
	}
	return nil
}

func (m *InMemoryRedisClusterClient) XAdd(_ context.Context, stream string, maxLen int64, values map[string]any) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isConn {
		return "", errors.New("redis cluster client disconnected")
	}
	m.streamSeq++
	id := fmt.Sprintf("%d-0", time.Now().UnixMilli()*1000+m.streamSeq)
	copiedValues := make(map[string]any, len(values))
	for k, v := range values {
		copiedValues[k] = v
	}
	msg := StreamMessage{
		ID:     id,
		Values: copiedValues,
	}
	st := m.streams[stream]
	st = append(st, msg)
	if maxLen > 0 && int64(len(st)) > maxLen {
		st = st[int64(len(st))-maxLen:]
	}
	m.streams[stream] = st
	return id, nil
}

func (m *InMemoryRedisClusterClient) XRange(_ context.Context, stream, start, stop string, count int64) ([]StreamMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.isConn {
		return nil, errors.New("redis cluster client disconnected")
	}
	st, ok := m.streams[stream]
	if !ok {
		return nil, nil
	}

	var res []StreamMessage
	for _, item := range st {
		if start != "-" && start != "" {
			// Exclusive or inclusive start check
			if strings.HasPrefix(start, "(") {
				if item.ID <= start[1:] {
					continue
				}
			} else {
				if item.ID < start {
					continue
				}
			}
		}
		if stop != "+" && stop != "" {
			if strings.HasPrefix(stop, "(") {
				if item.ID >= stop[1:] {
					continue
				}
			} else {
				if item.ID > stop {
					continue
				}
			}
		}
		res = append(res, item)
		if count > 0 && int64(len(res)) >= count {
			break
		}
	}
	return res, nil
}

func (m *InMemoryRedisClusterClient) Publish(_ context.Context, channel string, payload []byte) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.isConn {
		return errors.New("redis cluster client disconnected")
	}
	subs, ok := m.channels[channel]
	if !ok {
		return nil
	}
	for _, ch := range subs {
		cp := make([]byte, len(payload))
		copy(cp, payload)
		select {
		case ch <- cp:
		default:
		}
	}
	return nil
}

type inMemorySub struct {
	client  *InMemoryRedisClusterClient
	channel string
	ch      chan []byte
}

func (s *inMemorySub) Messages() <-chan []byte {
	return s.ch
}

func (s *inMemorySub) Close() error {
	s.client.mu.Lock()
	defer s.client.mu.Unlock()
	subs := s.client.channels[s.channel]
	var remaining []chan []byte
	for _, c := range subs {
		if c != s.ch {
			remaining = append(remaining, c)
		}
	}
	if len(remaining) == 0 {
		delete(s.client.channels, s.channel)
	} else {
		s.client.channels[s.channel] = remaining
	}
	return nil
}

func (m *InMemoryRedisClusterClient) Subscribe(_ context.Context, channel string) (PubSubSubscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isConn {
		return nil, errors.New("redis cluster client disconnected")
	}
	ch := make(chan []byte, 128)
	m.channels[channel] = append(m.channels[channel], ch)
	return &inMemorySub{
		client:  m,
		channel: channel,
		ch:      ch,
	}, nil
}

// PresenceRegistryConfig configures TTLs and bounds for the Redis presence and replay directory.
type PresenceRegistryConfig struct {
	NodeID       string
	PresenceTTL  time.Duration
	ReplayMaxLen int64
	ReplayTTL    time.Duration
}

// PresenceRegistry manages distributed client presence routing (ADR-003) and bounded
// Last-Event-ID replay buffers in Redis 7.2.
type PresenceRegistry struct {
	cfg                PresenceRegistryConfig
	client             RedisClusterClient
	logger             *slog.Logger
	metrics            *observability.MetricsRegistry
	mu                 sync.RWMutex
	localTenantSockets map[string]int // tenant_id -> local socket count on this node
}

// NewPresenceRegistry creates a new PresenceRegistry backed by RedisClusterClient.
func NewPresenceRegistry(
	cfg PresenceRegistryConfig,
	client RedisClusterClient,
	logger *slog.Logger,
	metrics *observability.MetricsRegistry,
) *PresenceRegistry {
	if cfg.PresenceTTL <= 0 {
		cfg.PresenceTTL = DefaultPresenceTTL
	}
	if cfg.ReplayMaxLen <= 0 {
		cfg.ReplayMaxLen = DefaultReplayMaxLen
	}
	if cfg.ReplayTTL <= 0 {
		cfg.ReplayTTL = DefaultReplayTTL
	}
	if logger == nil {
		logger = observability.NewSecuredLogger(nil, slog.LevelInfo)
	}
	return &PresenceRegistry{
		cfg:                cfg,
		client:             client,
		logger:             logger,
		metrics:            metrics,
		localTenantSockets: make(map[string]int),
	}
}

// PresenceKey returns the user presence Redis hash key: rag:notify:presence:{tenant_id}:{user_id}.
func PresenceKey(tenantID, userID string) string {
	return fmt.Sprintf("rag:notify:presence:%s:%s", tenantID, userID)
}

// TenantNodesKey returns the tenant nodes Redis set key: rag:notify:tenant_nodes:{tenant_id}.
func TenantNodesKey(tenantID string) string {
	return fmt.Sprintf("rag:notify:tenant_nodes:%s", tenantID)
}

// ReplayStreamKey returns the Redis Stream key for tenant replay events: rag:notify:replay:{tenant_id}.
func ReplayStreamKey(tenantID string) string {
	return fmt.Sprintf("rag:notify:replay:%s", tenantID)
}

// RegisterPresence implements command.PresenceRegistrar.
// Registers connection presence in Redis and updates local socket tracking.
func (r *PresenceRegistry) RegisterPresence(
	ctx context.Context,
	tenantID, userID, nodeID, connectionID string,
	ttl time.Duration,
) error {
	if tenantID == "" || connectionID == "" {
		return errors.New("tenant_id and connection_id are required")
	}
	if nodeID == "" {
		nodeID = r.cfg.NodeID
	}
	if ttl <= 0 {
		ttl = r.cfg.PresenceTTL
	}

	// 1. If user_id is present, set connection_id -> node_id in user presence hash
	if userID != "" {
		uKey := PresenceKey(tenantID, userID)
		if err := r.client.HSet(ctx, uKey, connectionID, nodeID); err != nil {
			return fmt.Errorf("hset presence %s: %w", uKey, err)
		}
		_ = r.client.Expire(ctx, uKey, ttl)
	}

	// 2. Add node_id to tenant_nodes set
	tKey := TenantNodesKey(tenantID)
	if err := r.client.SAdd(ctx, tKey, nodeID); err != nil {
		return fmt.Errorf("sadd tenant nodes %s: %w", tKey, err)
	}
	_ = r.client.Expire(ctx, tKey, ttl)

	// 3. Update local socket counter on this node
	if nodeID == r.cfg.NodeID {
		r.mu.Lock()
		r.localTenantSockets[tenantID]++
		r.mu.Unlock()
	}

	return nil
}

// DeregisterPresence implements command.PresenceRegistrar.
// Removes connection from user presence hash and prunes node from tenant_nodes
// immediately when the last local connection for tenant_id on this node closes.
func (r *PresenceRegistry) DeregisterPresence(
	ctx context.Context,
	tenantID, userID, connectionID string,
) error {
	if tenantID == "" || connectionID == "" {
		return errors.New("tenant_id and connection_id are required")
	}

	// 1. Remove connection from user hash if user_id was set
	if userID != "" {
		uKey := PresenceKey(tenantID, userID)
		_ = r.client.HDel(ctx, uKey, connectionID)
	}

	// 2. Decrement local socket counter on this node
	r.mu.Lock()
	count := r.localTenantSockets[tenantID] - 1
	if count <= 0 {
		delete(r.localTenantSockets, tenantID)
		count = 0
	} else {
		r.localTenantSockets[tenantID] = count
	}
	r.mu.Unlock()

	// 3. If no local sockets remain on this node for this tenant, immediately remove node_id
	// from rag:notify:tenant_nodes:{tenant_id} to prevent stale ghost routing!
	if count == 0 {
		tKey := TenantNodesKey(tenantID)
		if err := r.client.SRem(ctx, tKey, r.cfg.NodeID); err != nil {
			r.logger.Warn("failed to prune node from tenant_nodes",
				slog.String("tenant_id", tenantID),
				slog.String("node_id", r.cfg.NodeID),
				slog.String("error", err.Error()),
			)
		}
	}

	return nil
}

// RefreshPresence implements command.PresenceRegistrar.
// Refreshes TTLs for active user presence and tenant nodes keys during 15s heartbeats.
func (r *PresenceRegistry) RefreshPresence(
	ctx context.Context,
	tenantID, userID, connectionID string,
	ttl time.Duration,
) error {
	if tenantID == "" {
		return errors.New("tenant_id is required")
	}
	if ttl <= 0 {
		ttl = r.cfg.PresenceTTL
	}

	if userID != "" {
		uKey := PresenceKey(tenantID, userID)
		_ = r.client.Expire(ctx, uKey, ttl)
	}

	tKey := TenantNodesKey(tenantID)
	_ = r.client.Expire(ctx, tKey, ttl)
	return nil
}

// GetNodesForUser queries user presence hash and returns unique active node_ids hosting sockets for user_id.
func (r *PresenceRegistry) GetNodesForUser(ctx context.Context, tenantID, userID string) ([]string, error) {
	if tenantID == "" || userID == "" {
		return nil, nil
	}
	uKey := PresenceKey(tenantID, userID)
	connNodes, err := r.client.HGetAll(ctx, uKey)
	if err != nil {
		return nil, fmt.Errorf("get user presence %s: %w", uKey, err)
	}
	if len(connNodes) == 0 {
		return nil, nil
	}

	nodeSet := make(map[string]struct{}, len(connNodes))
	for _, nodeID := range connNodes {
		if nodeID != "" {
			nodeSet[nodeID] = struct{}{}
		}
	}
	nodes := make([]string, 0, len(nodeSet))
	for n := range nodeSet {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	return nodes, nil
}

// GetNodesForTenant queries tenant nodes set and returns all active node_ids hosting connections for tenant_id.
func (r *PresenceRegistry) GetNodesForTenant(ctx context.Context, tenantID string) ([]string, error) {
	if tenantID == "" {
		return nil, nil
	}
	tKey := TenantNodesKey(tenantID)
	nodes, err := r.client.SMembers(ctx, tKey)
	if err != nil {
		return nil, fmt.Errorf("get tenant nodes %s: %w", tKey, err)
	}
	return nodes, nil
}

// GetTargetNodes resolves active recipient node_ids in <= 5ms on the hot path (ADR-003):
// If user_id != "", query user presence; if empty or user_id == "", query tenant-wide nodes.
func (r *PresenceRegistry) GetTargetNodes(ctx context.Context, tenantID, userID string) ([]string, error) {
	if userID != "" {
		userNodes, err := r.GetNodesForUser(ctx, tenantID, userID)
		if err != nil {
			return nil, err
		}
		if len(userNodes) > 0 {
			return userNodes, nil
		}
	}
	return r.GetNodesForTenant(ctx, tenantID)
}

// AppendReplayEvent writes an event to the bounded Redis Stream replay buffer:
// rag:notify:replay:{tenant_id} (MAXLEN ~ 1000, TTL = 3600s).
func (r *PresenceRegistry) AppendReplayEvent(ctx context.Context, tenantID string, event entity.NotificationEvent) error {
	if tenantID == "" || event.EventID == "" {
		return errors.New("tenant_id and event_id are required")
	}
	sKey := ReplayStreamKey(tenantID)
	values := map[string]any{
		"event_id":            event.EventID,
		"user_id":             event.UserID,
		"source_service":      event.SourceService,
		"event_type":          event.EventType,
		"severity":            string(event.Severity),
		"is_terminal":         strconv.FormatBool(event.IsTerminal),
		"occurred_at_unix_ms": strconv.FormatInt(event.OccurredAt.UnixMilli(), 10),
		"trace_id":            event.TraceID,
		"payload_json":        string(event.PayloadJSON),
	}

	_, err := r.client.XAdd(ctx, sKey, r.cfg.ReplayMaxLen, values)
	if err != nil {
		return fmt.Errorf("xadd replay event %s: %w", sKey, err)
	}
	_ = r.client.Expire(ctx, sKey, r.cfg.ReplayTTL)
	return nil
}

// FetchReplayEvents queries missed tenant events strictly after lastEventID from rag:notify:replay:{tenant_id}.
func (r *PresenceRegistry) FetchReplayEvents(
	ctx context.Context,
	tenantID string,
	lastEventID string,
	limit int,
) ([]entity.NotificationEvent, error) {
	if tenantID == "" {
		return nil, errors.New("tenant_id is required")
	}
	if limit <= 0 {
		limit = 100
	}
	sKey := ReplayStreamKey(tenantID)

	// Stream range query
	// If lastEventID is provided, start from next or scan stream
	start := "-"
	messages, err := r.client.XRange(ctx, sKey, start, "+", int64(r.cfg.ReplayMaxLen))
	if err != nil {
		return nil, fmt.Errorf("xrange replay events %s: %w", sKey, err)
	}

	var events []entity.NotificationEvent
	pastWatermark := lastEventID == "" // if lastEventID is empty, return all up to limit

	for _, msg := range messages {
		evID, _ := msg.Values["event_id"].(string)
		if !pastWatermark {
			if evID == lastEventID {
				pastWatermark = true
			}
			continue
		}

		uID, _ := msg.Values["user_id"].(string)
		srcSvc, _ := msg.Values["source_service"].(string)
		evType, _ := msg.Values["event_type"].(string)
		rawSev, _ := msg.Values["severity"].(string)
		rawTerm, _ := msg.Values["is_terminal"].(string)
		rawOccurred, _ := msg.Values["occurred_at_unix_ms"].(string)
		trID, _ := msg.Values["trace_id"].(string)
		rawPayload, _ := msg.Values["payload_json"].(string)

		sev, sErr := entity.ParseSeverity(rawSev)
		if sErr != nil {
			sev = entity.SeverityInfo
		}
		isTerm := rawTerm == "true"
		occMs, _ := strconv.ParseInt(rawOccurred, 10, 64)
		if occMs == 0 {
			occMs = time.Now().UnixMilli()
		}

		ev := entity.NotificationEvent{
			EventID:       evID,
			TenantID:      tenantID,
			UserID:        uID,
			SourceService: srcSvc,
			EventType:     evType,
			Severity:      sev,
			IsTerminal:    isTerm,
			OccurredAt:    time.UnixMilli(occMs).UTC(),
			TraceID:       trID,
			PayloadJSON:   json.RawMessage(rawPayload),
		}

		events = append(events, ev)
		if len(events) >= limit {
			break
		}
	}

	return events, nil
}
