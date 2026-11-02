// Package entity defines core domain entities and value objects for rag-notification-dispatch-daemon.
// This package contains pure Go business logic with zero external I/O dependencies.
package entity

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"
)

// Severity represents a domain notification severity level.
type Severity string

const (
	SeverityInfo     Severity = "INFO"
	SeverityWarning  Severity = "WARNING"
	SeverityError    Severity = "ERROR"
	SeverityCritical Severity = "CRITICAL"
)

// DefaultSSERetryMs is the default reconnect retry directive emitted on SSE frames.
const DefaultSSERetryMs = 5000

// Canonical upstream services emitting CDC outbox notification events.
const (
	SourceServiceKBCatalog            = "rag-kb-catalog-service"
	SourceServiceIngestionCoordinator = "rag-ingestion-coordinator"
	SourceServiceChunkLifecycle       = "rag-chunk-lifecycle-service"
)

// Canonical domain event types defined in the OpenAPI 3.1 SSENotificationFrame schema.
const (
	EventTypeIngestionMilestone     = "ingestion.milestone"
	EventTypeIndexBlueGreenSwitched = "index.blue_green_switched"
	EventTypeChunkOverlayConflict   = "chunk.overlay_conflict"
	EventTypeEvaluationQualityAlert = "evaluation.quality_alert"
)

// ParseSeverity validates and normalizes a string into a domain Severity.
func ParseSeverity(raw string) (Severity, error) {
	switch Severity(raw) {
	case SeverityInfo:
		return SeverityInfo, nil
	case SeverityWarning:
		return SeverityWarning, nil
	case SeverityError:
		return SeverityError, nil
	case SeverityCritical:
		return SeverityCritical, nil
	default:
		return "", fmt.Errorf("unsupported severity %q", raw)
	}
}

// Rank returns the numerical severity priority (INFO=1 .. CRITICAL=4) for threshold comparisons.
func (s Severity) Rank() int {
	switch s {
	case SeverityInfo:
		return 1
	case SeverityWarning:
		return 2
	case SeverityError:
		return 3
	case SeverityCritical:
		return 4
	default:
		return 0
	}
}

// IsValid reports whether the severity level is one of the 4 canonical values.
func (s Severity) IsValid() bool {
	return s.Rank() > 0
}

// IsValidSourceService checks whether the upstream service is a recognized producer.
func IsValidSourceService(service string) bool {
	switch service {
	case SourceServiceKBCatalog, SourceServiceIngestionCoordinator, SourceServiceChunkLifecycle:
		return true
	default:
		return false
	}
}

// IsCanonicalEventType checks whether the event type is one of the canonical RAG domain events.
func IsCanonicalEventType(eventType string) bool {
	switch eventType {
	case EventTypeIngestionMilestone,
		EventTypeIndexBlueGreenSwitched,
		EventTypeChunkOverlayConflict,
		EventTypeEvaluationQualityAlert:
		return true
	default:
		return false
	}
}

// NotificationEvent represents an immutable domain notification event ingested from the CDC outbox stream.
type NotificationEvent struct {
	EventID       string
	TenantID      string
	UserID        string
	SourceService string
	EventType     string
	Severity      Severity
	IsTerminal    bool
	OccurredAt    time.Time
	TraceID       string
	PayloadJSON   []byte
}

// Validate enforces domain invariants on NotificationEvent before routing or serialization.
func (e NotificationEvent) Validate() error {
	if e.EventID == "" {
		return errors.New("event_id is required")
	}
	if e.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if e.SourceService == "" {
		return errors.New("source_service is required")
	}
	if !IsValidSourceService(e.SourceService) {
		return fmt.Errorf("unrecognized source_service %q", e.SourceService)
	}
	if e.EventType == "" {
		return errors.New("event_type is required")
	}
	if !IsCanonicalEventType(e.EventType) {
		return fmt.Errorf("unrecognized event_type %q", e.EventType)
	}
	if !e.Severity.IsValid() {
		return fmt.Errorf("invalid severity %q", e.Severity)
	}
	if e.OccurredAt.IsZero() {
		return errors.New("occurred_at is required")
	}
	trimmed := bytes.TrimSpace(e.PayloadJSON)
	if len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(trimmed) {
		return errors.New("payload_json must be a valid JSON object")
	}
	return nil
}

// FormatSSEDataJSON constructs the JSON body for the SSE `data:` line using a zero-intermediary byte buffer.
// It compacts validated PayloadJSON bytes into a single line before splicing them directly into the output slice
// without map[string]any unmarshaling or reflection overhead on the hot path.
func (e NotificationEvent) FormatSSEDataJSON() []byte {
	compactedPayload := compactPayloadJSON(e.PayloadJSON)
	capacity := 160 + len(e.EventID) + len(e.TenantID) + len(e.UserID) + len(e.EventType) + len(compactedPayload)
	buf := make([]byte, 0, capacity)
	return e.appendSSEDataJSON(buf, compactedPayload)
}

// FormatSSEFrame formats a complete Server-Sent Events wire frame (`id:`, `event:`, `retry:`, `data:\n\n`)
// directly into a single contiguous byte slice ready for non-blocking ring buffer enqueueing and socket flushing.
func (e NotificationEvent) FormatSSEFrame(retryMs int) []byte {
	if retryMs <= 0 {
		retryMs = DefaultSSERetryMs
	}
	compactedPayload := compactPayloadJSON(e.PayloadJSON)
	capacity := 208 + 2*len(e.EventID) + len(e.TenantID) + len(e.UserID) + 2*len(e.EventType) + len(compactedPayload)
	buf := make([]byte, 0, capacity)

	buf = append(buf, "id: "...)
	buf = append(buf, e.EventID...)
	buf = append(buf, "\nevent: "...)
	buf = append(buf, e.EventType...)
	buf = append(buf, "\nretry: "...)
	buf = strconv.AppendInt(buf, int64(retryMs), 10)
	buf = append(buf, "\ndata: "...)
	buf = e.appendSSEDataJSON(buf, compactedPayload)
	buf = append(buf, "\n\n"...)
	return buf
}

func compactPayloadJSON(raw []byte) []byte {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return []byte("{}")
	}
	// Fast-path: if there is no whitespace or newline character inside, no allocation or compaction needed.
	hasWhitespace := false
	for _, b := range trimmed {
		if b == '\n' || b == '\r' || b == '\t' || b == ' ' {
			hasWhitespace = true
			break
		}
	}
	if !hasWhitespace {
		return trimmed
	}
	var dst bytes.Buffer
	dst.Grow(len(trimmed))
	if err := json.Compact(&dst, trimmed); err != nil {
		return []byte("{}")
	}
	return dst.Bytes()
}

func appendJSONString(buf []byte, s string) []byte {
	b, _ := json.Marshal(s)
	return append(buf, b...)
}

func (e NotificationEvent) appendSSEDataJSON(buf []byte, compactedPayload []byte) []byte {
	buf = append(buf, `{"event_id":`...)
	buf = appendJSONString(buf, e.EventID)
	buf = append(buf, `,"tenant_id":`...)
	buf = appendJSONString(buf, e.TenantID)
	if e.UserID != "" {
		buf = append(buf, `,"user_id":`...)
		buf = appendJSONString(buf, e.UserID)
	}
	buf = append(buf, `,"event_type":`...)
	buf = appendJSONString(buf, e.EventType)
	buf = append(buf, `,"severity":`...)
	buf = appendJSONString(buf, string(e.Severity))
	buf = append(buf, `,"is_terminal":`...)
	buf = strconv.AppendBool(buf, e.IsTerminal)
	buf = append(buf, `,"occurred_at":"`...)
	buf = e.OccurredAt.UTC().AppendFormat(buf, time.RFC3339)
	buf = append(buf, `","payload":`...)
	buf = append(buf, compactedPayload...)
	buf = append(buf, '}')
	return buf
}

// LogValue implements slog.LogValuer to enforce Secured Logging redaction of notification payload text.
func (e NotificationEvent) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("event_id", e.EventID),
		slog.String("tenant_id", e.TenantID),
		slog.String("user_id", e.UserID),
		slog.String("source_service", e.SourceService),
		slog.String("event_type", e.EventType),
		slog.String("severity", string(e.Severity)),
		slog.Bool("is_terminal", e.IsTerminal),
		slog.Time("occurred_at", e.OccurredAt.UTC()),
		slog.String("trace_id", e.TraceID),
		slog.Int("payload_bytes_len", len(e.PayloadJSON)),
		slog.String("payload_json", "[REDACTED]"),
	)
}
