// Package sse provides high-density Server-Sent Events (SSE) streaming infrastructure
// adhering to ADR-002, edge proxy bypass headers, and Slow-Loris write deadline guards.
package sse

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
)

var (
	// KeepAlivePingComment is the periodic 15-second SSE heartbeat comment frame.
	KeepAlivePingComment = []byte(": ping\n\n")

	ErrFlusherNotSupported = errors.New("http.ResponseWriter does not support http.Flusher")
)

// Flusher wraps http.ResponseWriter and http.Flusher with edge proxy bypass headers,
// per-write socket deadlines (Slow-Loris guard), and Prometheus write latency telemetry.
type Flusher struct {
	w             http.ResponseWriter
	flusher       http.Flusher
	rc            *http.ResponseController
	nodeID        string
	writeDeadline time.Duration
	metrics       *observability.MetricsRegistry
}

// NewFlusher initializes an SSE stream writer with Slow-Loris write deadline protection.
func NewFlusher(
	w http.ResponseWriter,
	nodeID string,
	writeDeadline time.Duration,
	metrics *observability.MetricsRegistry,
) (*Flusher, error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, ErrFlusherNotSupported
	}
	if writeDeadline <= 0 {
		writeDeadline = entity.SlowConsumerEvictionTimeout // default 5s
	}

	return &Flusher{
		w:             w,
		flusher:       flusher,
		rc:            http.NewResponseController(w),
		nodeID:        nodeID,
		writeDeadline: writeDeadline,
		metrics:       metrics,
	}, nil
}

// WriteHeaders writes standard ADR-002 edge proxy bypass headers to the HTTP response.
func (f *Flusher) WriteHeaders() {
	h := f.w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	f.w.WriteHeader(http.StatusOK)
	f.flusher.Flush()
}

// WriteFrame flushes raw bytes to the SSE stream bounded by the Slow-Loris write deadline,
// recording write latency in Prometheus histogram notification_client_write_latency_seconds{node_id}.
func (f *Flusher) WriteFrame(data []byte) error {
	start := time.Now()

	// Enforce write deadline using Go 1.20+ ResponseController so zero-window clients cannot hang goroutines.
	if f.rc != nil && f.writeDeadline > 0 {
		_ = f.rc.SetWriteDeadline(time.Now().Add(f.writeDeadline))
	}

	_, err := f.w.Write(data)
	if err != nil {
		if f.rc != nil {
			_ = f.rc.SetWriteDeadline(time.Time{})
		}
		return fmt.Errorf("write sse bytes: %w", err)
	}

	f.flusher.Flush()

	if f.rc != nil {
		_ = f.rc.SetWriteDeadline(time.Time{})
	}

	elapsed := time.Since(start)
	if f.metrics != nil {
		f.metrics.ObserveClientWriteLatency(f.nodeID, elapsed.Seconds())
	}

	return nil
}

// WritePing sends an immediate : ping\n\n comment frame to bypass idle proxy timeouts (Runbook 3).
func (f *Flusher) WritePing() error {
	return f.WriteFrame(KeepAlivePingComment)
}

// WriteRetryDirective sends an SSE retry directive frame: retry: <ms>\n\n before socket closure.
func (f *Flusher) WriteRetryDirective(retryDuration time.Duration) error {
	ms := retryDuration.Milliseconds()
	if ms <= 0 {
		ms = int64(entity.DefaultSSERetryMs)
	}
	frame := fmt.Sprintf("retry: %d\n\n", ms)
	return f.WriteFrame([]byte(frame))
}
