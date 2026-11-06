package entity

import (
	"errors"
	"log/slog"
	"time"
)

const (
	// MaxConnectionBufferEvents is the hard capacity of the per-client non-blocking SSE ring buffer.
	MaxConnectionBufferEvents = 256
	// SlowConsumerEvictionTimeout is the maximum continuous buffer saturation duration before socket eviction.
	SlowConsumerEvictionTimeout = 5 * time.Second
)

// ContextKey represents typed keys for storing session identifiers in request contexts.
type ContextKey string

const (
	ContextKeyTenantID ContextKey = "tenant_id"
	ContextKeyUserID   ContextKey = "user_id"
	ContextKeyTraceID  ContextKey = "trace_id"
)

// Connection represents an active unidirectional SSE client session aggregate.
type Connection struct {
	ConnectionID   string
	TenantID       string
	UserID         string
	NodeID         string
	SeverityFilter Severity
	LastEventID    string
	ConnectedAt    time.Time
	BufferedEvents int
	SaturatedSince *time.Time
}

// Validate checks mandatory session fields on Connection.
func (c Connection) Validate() error {
	if c.ConnectionID == "" {
		return errors.New("connection_id is required")
	}
	if c.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if c.NodeID == "" {
		return errors.New("node_id is required")
	}
	if c.SeverityFilter != "" && !c.SeverityFilter.IsValid() {
		return errors.New("invalid severity_filter")
	}
	if c.BufferedEvents < 0 || c.BufferedEvents > MaxConnectionBufferEvents {
		return errors.New("buffered_events out of bounds [0, 256]")
	}
	return nil
}

// AcceptsEvent reports whether the connection's tenant, user, and optional severity filter match the event.
// User-targeted notifications (event.UserID != "") are strictly delivered to matching user connections
// and never leak to connections with mismatched or empty UserID.
func (c Connection) AcceptsEvent(event NotificationEvent) bool {
	if c.TenantID != event.TenantID {
		return false
	}
	if event.UserID != "" && c.UserID != event.UserID {
		return false
	}
	if c.SeverityFilter != "" && event.Severity.Rank() < c.SeverityFilter.Rank() {
		return false
	}
	return true
}

// RecordBufferState updates the connection's buffered event count and tracks continuous saturation watermarks.
func (c *Connection) RecordBufferState(buffered int, now time.Time) {
	if buffered < 0 {
		buffered = 0
	}
	if buffered > MaxConnectionBufferEvents {
		buffered = MaxConnectionBufferEvents
	}
	c.BufferedEvents = buffered
	if buffered >= MaxConnectionBufferEvents {
		if c.SaturatedSince == nil {
			ts := now.UTC()
			c.SaturatedSince = &ts
		}
	} else {
		c.SaturatedSince = nil
	}
}

// IsSlowConsumerStalled reports whether the connection's ring buffer has been continuously saturated
// for longer than SlowConsumerEvictionTimeout (5 seconds).
func (c Connection) IsSlowConsumerStalled(now time.Time) bool {
	if c.SaturatedSince == nil || c.BufferedEvents < MaxConnectionBufferEvents {
		return false
	}
	return now.Sub(*c.SaturatedSince) > SlowConsumerEvictionTimeout
}

// LogValue implements slog.LogValuer for safe structured session telemetry.
func (c Connection) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("connection_id", c.ConnectionID),
		slog.String("tenant_id", c.TenantID),
		slog.String("user_id", c.UserID),
		slog.String("node_id", c.NodeID),
		slog.String("severity_filter", string(c.SeverityFilter)),
		slog.Time("connected_at", c.ConnectedAt.UTC()),
		slog.Int("buffered_events", c.BufferedEvents),
	)
}
