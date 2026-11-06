// Package observability provides Secured Logging (log/slog JSONHandler with automatic
// secret/token/PII redaction) and Prometheus telemetry metrics for rag-notification-dispatch-daemon.
package observability

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// RedactedPlaceholder is the canonical mask emitted whenever a sensitive attribute is logged.
const RedactedPlaceholder = "[REDACTED]"

var redactedAttributeKeys = map[string]struct{}{
	"authorization":       {},
	"authorization_token": {},
	"bearer":              {},
	"bearer_token":        {},
	"token":               {},
	"jwt":                 {},
	"jwt_public_key_pem":  {},
	"secret":              {},
	"secret_ref":          {},
	"hmac_key":            {},
	"hmac_secret":         {},
	"signing_secret":      {},
	"password":            {},
	"redis_password":      {},
	"dsn":                 {},
	"postgres_dsn":        {},
	"payload":             {},
	"payload_json":        {},
}

// SecretString wraps sensitive configuration values (DSNs, passwords, tokens, keys)
// so they always serialize as "[REDACTED]" in logs and string formatting.
type SecretString struct {
	raw string
}

// NewSecretString wraps a sensitive string value.
func NewSecretString(raw string) SecretString {
	return SecretString{raw: raw}
}

// Unmasked returns the underlying plaintext value strictly for driver initialization.
func (s SecretString) Unmasked() string {
	return s.raw
}

// IsEmpty reports whether the underlying secret is empty.
func (s SecretString) IsEmpty() bool {
	return s.raw == ""
}

// String masks the secret value to prevent accidental leakage.
func (s SecretString) String() string {
	return RedactedPlaceholder
}

// LogValue implements slog.LogValuer to guarantee redaction in structured JSON logs.
func (s SecretString) LogValue() slog.Value {
	return slog.StringValue(RedactedPlaceholder)
}

// ParseLogLevel converts a 12-Factor LOG_LEVEL string into a slog.Level.
func ParseLogLevel(raw string) slog.Level {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN", "WARNING":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// NewSecuredLogger constructs a structured JSON slog.Logger with automatic attribute redaction.
func NewSecuredLogger(w io.Writer, level slog.Level) *slog.Logger {
	if w == nil {
		w = os.Stdout
	}
	opts := &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: redactSlogAttr,
	}
	return slog.New(slog.NewJSONHandler(w, opts))
}

func redactSlogAttr(_ []string, a slog.Attr) slog.Attr {
	keyLower := strings.ToLower(strings.TrimSpace(a.Key))
	if isRedactedKey(keyLower) {
		return slog.String(a.Key, RedactedPlaceholder)
	}
	val := a.Value.Resolve()
	if val.Kind() == slog.KindGroup {
		attrs := val.Group()
		redactedGroup := make([]any, 0, len(attrs))
		for _, sub := range attrs {
			redactedGroup = append(redactedGroup, redactSlogAttr(nil, sub))
		}
		return slog.Group(a.Key, redactedGroup...)
	}
	if val.Kind() == slog.KindString {
		s := val.String()
		if looksLikeSensitiveCredential(s) {
			return slog.String(a.Key, RedactedPlaceholder)
		}
	}
	return slog.Attr{Key: a.Key, Value: val}
}

func isRedactedKey(keyLower string) bool {
	if _, ok := redactedAttributeKeys[keyLower]; ok {
		return true
	}
	for pattern := range redactedAttributeKeys {
		if strings.Contains(keyLower, pattern) && pattern != "payload" {
			return true
		}
	}
	return false
}

func looksLikeSensitiveCredential(val string) bool {
	trimmed := strings.TrimSpace(val)
	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, "bearer ") {
		return true
	}
	if strings.HasPrefix(lower, "postgres://") || strings.HasPrefix(lower, "postgresql://") || strings.HasPrefix(lower, "redis://") {
		return true
	}
	if strings.Count(trimmed, ".") == 2 && strings.HasPrefix(trimmed, "eyJ") {
		return true
	}
	return false
}

// DefaultLatencyBuckets defines Prometheus histogram buckets (in seconds) tuned for sub-50ms SSE writes.
var DefaultLatencyBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.015, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0}

type histogramSeries struct {
	buckets []float64
	counts  []uint64
	sum     float64
	total   uint64
}

// MetricsRegistry maintains thread-safe counters, gauges, and histograms for the 6 canonical
// Prometheus metrics required by rag-notification-dispatch-daemon.
type MetricsRegistry struct {
	mu                   sync.RWMutex
	activeSSEConnections map[string]int64
	eventsProcessedTotal map[string]uint64
	eventsEmittedTotal   map[string]uint64
	droppedFramesTotal   map[string]uint64
	webhookEnqueuedTotal map[string]uint64
	cacheInvalidations   map[string]uint64
	clientWriteLatency   map[string]*histogramSeries
	cdcConsumerLag       int64
}

// NewMetricsRegistry creates a new thread-safe Prometheus MetricsRegistry.
func NewMetricsRegistry() *MetricsRegistry {
	return &MetricsRegistry{
		activeSSEConnections: make(map[string]int64),
		eventsProcessedTotal: make(map[string]uint64),
		eventsEmittedTotal:   make(map[string]uint64),
		droppedFramesTotal:   make(map[string]uint64),
		webhookEnqueuedTotal: make(map[string]uint64),
		cacheInvalidations:   make(map[string]uint64),
		clientWriteLatency:   make(map[string]*histogramSeries),
	}
}

// SetActiveSSEConnections sets notification_active_sse_connections{tenant_id, node_id}.
func (m *MetricsRegistry) SetActiveSSEConnections(tenantID, nodeID string, count int64) {
	key := fmt.Sprintf(`tenant_id=%q,node_id=%q`, tenantID, nodeID)
	m.mu.Lock()
	m.activeSSEConnections[key] = count
	m.mu.Unlock()
}

// AddActiveSSEConnections adjusts notification_active_sse_connections{tenant_id, node_id} by delta.
func (m *MetricsRegistry) AddActiveSSEConnections(tenantID, nodeID string, delta int64) {
	key := fmt.Sprintf(`tenant_id=%q,node_id=%q`, tenantID, nodeID)
	m.mu.Lock()
	m.activeSSEConnections[key] += delta
	if m.activeSSEConnections[key] < 0 {
		m.activeSSEConnections[key] = 0
	}
	m.mu.Unlock()
}

// IncEventsProcessed increments notification_events_processed_total{event_type, status}.
func (m *MetricsRegistry) IncEventsProcessed(eventType, status string) {
	if eventType == "" {
		eventType = "unknown"
	}
	key := fmt.Sprintf(`event_type=%q,status=%q`, eventType, status)
	m.mu.Lock()
	m.eventsProcessedTotal[key]++
	m.mu.Unlock()
}

// EventsProcessedCount returns the current counter value for notification_events_processed_total{event_type, status}.
func (m *MetricsRegistry) EventsProcessedCount(eventType, status string) uint64 {
	if eventType == "" {
		eventType = "unknown"
	}
	key := fmt.Sprintf(`event_type=%q,status=%q`, eventType, status)
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.eventsProcessedTotal[key]
}

// ObserveClientWriteLatency records a duration (in seconds) in notification_client_write_latency_seconds{node_id}.
func (m *MetricsRegistry) ObserveClientWriteLatency(nodeID string, seconds float64) {
	key := fmt.Sprintf(`node_id=%q`, nodeID)
	m.mu.Lock()
	series, ok := m.clientWriteLatency[key]
	if !ok {
		series = &histogramSeries{
			buckets: DefaultLatencyBuckets,
			counts:  make([]uint64, len(DefaultLatencyBuckets)),
		}
		m.clientWriteLatency[key] = series
	}
	for i, bound := range series.buckets {
		if seconds <= bound {
			series.counts[i]++
		}
	}
	series.sum += seconds
	series.total++
	m.mu.Unlock()
}

// IncDroppedFrames increments notification_dropped_frames_total{tenant_id, reason}.
func (m *MetricsRegistry) IncDroppedFrames(tenantID, reason string) {
	key := fmt.Sprintf(`tenant_id=%q,reason=%q`, tenantID, reason)
	m.mu.Lock()
	m.droppedFramesTotal[key]++
	m.mu.Unlock()
}

// DroppedFramesCount returns the current counter value for notification_dropped_frames_total{tenant_id, reason}.
func (m *MetricsRegistry) DroppedFramesCount(tenantID, reason string) uint64 {
	key := fmt.Sprintf(`tenant_id=%q,reason=%q`, tenantID, reason)
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.droppedFramesTotal[key]
}

// IncEventsEmitted increments notification_events_emitted_total{tenant_id}.
func (m *MetricsRegistry) IncEventsEmitted(tenantID string) {
	if tenantID == "" {
		tenantID = "unknown"
	}
	key := fmt.Sprintf(`tenant_id=%q`, tenantID)
	m.mu.Lock()
	m.eventsEmittedTotal[key]++
	m.mu.Unlock()
}

// EventsEmittedCount returns the current counter value for notification_events_emitted_total{tenant_id}.
func (m *MetricsRegistry) EventsEmittedCount(tenantID string) uint64 {
	if tenantID == "" {
		tenantID = "unknown"
	}
	key := fmt.Sprintf(`tenant_id=%q`, tenantID)
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.eventsEmittedTotal[key]
}

// SetCDCConsumerLag sets the notification_cdc_consumer_lag gauge.
func (m *MetricsRegistry) SetCDCConsumerLag(lag int64) {
	m.mu.Lock()
	m.cdcConsumerLag = lag
	m.mu.Unlock()
}

// CDCConsumerLag returns the current notification_cdc_consumer_lag gauge.
func (m *MetricsRegistry) CDCConsumerLag() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cdcConsumerLag
}

// IncWebhookEnqueued increments notification_webhook_enqueued_total{tenant_id, status}.
func (m *MetricsRegistry) IncWebhookEnqueued(tenantID, status string) {
	key := fmt.Sprintf(`tenant_id=%q,status=%q`, tenantID, status)
	m.mu.Lock()
	m.webhookEnqueuedTotal[key]++
	m.mu.Unlock()
}

// WebhookEnqueuedCount returns the current counter value for notification_webhook_enqueued_total{tenant_id, status}.
func (m *MetricsRegistry) WebhookEnqueuedCount(tenantID, status string) uint64 {
	key := fmt.Sprintf(`tenant_id=%q,status=%q`, tenantID, status)
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.webhookEnqueuedTotal[key]
}

// IncCacheInvalidations increments notification_cache_invalidations_total{reason}.
func (m *MetricsRegistry) IncCacheInvalidations(reason string) {
	key := fmt.Sprintf(`reason=%q`, reason)
	m.mu.Lock()
	m.cacheInvalidations[key]++
	m.mu.Unlock()
}

// CacheInvalidationsCount returns the current counter value for notification_cache_invalidations_total{reason}.
func (m *MetricsRegistry) CacheInvalidationsCount(reason string) uint64 {
	key := fmt.Sprintf(`reason=%q`, reason)
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cacheInvalidations[key]
}

// RenderPrometheusText formats all registered metrics in standard Prometheus text exposition format.
func (m *MetricsRegistry) RenderPrometheusText() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var b strings.Builder

	b.WriteString("# HELP notification_active_sse_connections Active SSE push connections per tenant and pod node.\n")
	b.WriteString("# TYPE notification_active_sse_connections gauge\n")
	writeSortedInt64Map(&b, "notification_active_sse_connections", m.activeSSEConnections)

	b.WriteString("# HELP notification_events_processed_total Total CDC outbox notification events processed.\n")
	b.WriteString("# TYPE notification_events_processed_total counter\n")
	writeSortedUint64Map(&b, "notification_events_processed_total", m.eventsProcessedTotal)

	b.WriteString("# HELP notification_client_write_latency_seconds Histogram of SSE socket write latency in seconds.\n")
	b.WriteString("# TYPE notification_client_write_latency_seconds histogram\n")
	histKeys := make([]string, 0, len(m.clientWriteLatency))
	for k := range m.clientWriteLatency {
		histKeys = append(histKeys, k)
	}
	sort.Strings(histKeys)
	for _, k := range histKeys {
		series := m.clientWriteLatency[k]
		for i, bound := range series.buckets {
			b.WriteString("notification_client_write_latency_seconds_bucket{")
			b.WriteString(k)
			b.WriteString(`,le="`)
			b.WriteString(strconv.FormatFloat(bound, 'f', -1, 64))
			b.WriteString(`"} `)
			b.WriteString(strconv.FormatUint(series.counts[i], 10))
			b.WriteByte('\n')
		}
		b.WriteString("notification_client_write_latency_seconds_bucket{")
		b.WriteString(k)
		b.WriteString(`,le="+Inf"} `)
		b.WriteString(strconv.FormatUint(series.total, 10))
		b.WriteByte('\n')
		b.WriteString("notification_client_write_latency_seconds_sum{")
		b.WriteString(k)
		b.WriteString("} ")
		b.WriteString(strconv.FormatFloat(series.sum, 'f', 6, 64))
		b.WriteByte('\n')
		b.WriteString("notification_client_write_latency_seconds_count{")
		b.WriteString(k)
		b.WriteString("} ")
		b.WriteString(strconv.FormatUint(series.total, 10))
		b.WriteByte('\n')
	}

	b.WriteString("# HELP notification_dropped_frames_total Total SSE notification frames dropped due to slow-consumer ring buffer saturation.\n")
	b.WriteString("# TYPE notification_dropped_frames_total counter\n")
	writeSortedUint64Map(&b, "notification_dropped_frames_total", m.droppedFramesTotal)

	b.WriteString("# HELP notification_webhook_enqueued_total Total outbound webhook delivery tasks enqueued.\n")
	b.WriteString("# TYPE notification_webhook_enqueued_total counter\n")
	writeSortedUint64Map(&b, "notification_webhook_enqueued_total", m.webhookEnqueuedTotal)

	b.WriteString("# HELP notification_cache_invalidations_total Total pod-local subscription cache invalidation events processed.\n")
	b.WriteString("# TYPE notification_cache_invalidations_total counter\n")
	writeSortedUint64Map(&b, "notification_cache_invalidations_total", m.cacheInvalidations)

	b.WriteString("# HELP notification_events_emitted_total Total SSE notification frames successfully emitted to clients.\n")
	b.WriteString("# TYPE notification_events_emitted_total counter\n")
	writeSortedUint64Map(&b, "notification_events_emitted_total", m.eventsEmittedTotal)

	b.WriteString("# HELP notification_cdc_consumer_lag CDC milestone outbox consumer offset lag in records.\n")
	b.WriteString("# TYPE notification_cdc_consumer_lag gauge\n")
	b.WriteString("notification_cdc_consumer_lag ")
	b.WriteString(strconv.FormatInt(m.cdcConsumerLag, 10))
	b.WriteByte('\n')

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	b.WriteString("# HELP go_memstats_alloc_bytes Number of bytes allocated and still in use.\n")
	b.WriteString("# TYPE go_memstats_alloc_bytes gauge\n")
	b.WriteString("go_memstats_alloc_bytes ")
	b.WriteString(strconv.FormatUint(memStats.Alloc, 10))
	b.WriteByte('\n')

	return b.String()
}

func writeSortedInt64Map(b *strings.Builder, metric string, data map[string]int64) {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteString(metric)
		b.WriteByte('{')
		b.WriteString(k)
		b.WriteString("} ")
		b.WriteString(strconv.FormatInt(data[k], 10))
		b.WriteByte('\n')
	}
}

func writeSortedUint64Map(b *strings.Builder, metric string, data map[string]uint64) {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteString(metric)
		b.WriteByte('{')
		b.WriteString(k)
		b.WriteString("} ")
		b.WriteString(strconv.FormatUint(data[k], 10))
		b.WriteByte('\n')
	}
}
