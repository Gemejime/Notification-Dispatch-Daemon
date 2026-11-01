// Package kafka provides the CDC outbox consumer group engine (`CDCConsumer`)
// subscribing to upstream domain event streams (`rag.cdc.kb-catalog`, `rag.cdc.ingestion`,
// `rag.cdc.chunk-lifecycle`) with deterministic offset watermarks and poison-pill isolation.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	notificationv1 "github.com/gemejime/rag-notification-dispatch-daemon/api/proto/v1"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/application/command"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
)

// DefaultCDCTopics defines the canonical upstream transactional outbox topics.
var DefaultCDCTopics = []string{
	"rag.cdc.kb-catalog",
	"rag.cdc.ingestion",
	"rag.cdc.chunk-lifecycle",
}

// CDCRecord represents a single broker message fetched from a Kafka / Redis CDC stream partition.
type CDCRecord struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

// RecordStream abstracts message consumption and offset committing over a Kafka 3.7+ consumer group.
type RecordStream interface {
	Fetch(ctx context.Context) (CDCRecord, error)
	CommitOffset(ctx context.Context, topic string, partition int, offset int64) error
	Close() error
}

// ConsumerConfig holds the 12-Factor configuration for the CDC consumer group.
type ConsumerConfig struct {
	Brokers       []string
	ConsumerGroup string
	Topics        []string
}

type partitionKey struct {
	topic     string
	partition int
}

// CDCConsumer ingests CDC outbox envelopes from upstream RAG services, invokes ProcessCDCEventHandler,
// isolates poison-pill messages without stalling partition lag, and tracks committed offsets per partition.
type CDCConsumer struct {
	cfg              ConsumerConfig
	stream           RecordStream
	handler          *command.ProcessCDCEventHandler
	logger           *slog.Logger
	metrics          *observability.MetricsRegistry
	consuming        atomic.Bool
	mu               sync.RWMutex
	committedOffsets map[partitionKey]int64
}

// NewCDCConsumer constructs a CDCConsumer wired to the hot-path ProcessCDCEventHandler.
func NewCDCConsumer(
	cfg ConsumerConfig,
	stream RecordStream,
	handler *command.ProcessCDCEventHandler,
	logger *slog.Logger,
	metrics *observability.MetricsRegistry,
) *CDCConsumer {
	if len(cfg.Topics) == 0 {
		cfg.Topics = DefaultCDCTopics
	}
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "rag-notification-dispatch-daemon"
	}
	if logger == nil {
		logger = observability.NewSecuredLogger(nil, slog.LevelInfo)
	}
	if metrics == nil {
		metrics = observability.NewMetricsRegistry()
	}
	return &CDCConsumer{
		cfg:              cfg,
		stream:           stream,
		handler:          handler,
		logger:           logger,
		metrics:          metrics,
		committedOffsets: make(map[partitionKey]int64),
	}
}

// Run starts the continuous CDC ingestion loop until ctx is canceled or the stream closes.
func (c *CDCConsumer) Run(ctx context.Context) error {
	if c.stream == nil {
		return errors.New("cdc record stream is nil")
	}
	if c.handler == nil {
		return errors.New("process cdc event handler is nil")
	}

	c.consuming.Store(true)
	defer c.consuming.Store(false)

	for {
		if ctx.Err() != nil {
			return nil
		}
		record, err := c.stream.Fetch(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return fmt.Errorf("fetch cdc record: %w", err)
		}
		if err := c.ProcessRecord(ctx, record); err != nil {
			return err
		}
	}
}

// ProcessRecord unmarshals a single CDCRecord, routes it through ProcessCDCEventHandler,
// and commits the next partition offset (`record.Offset + 1`).
// Poison-pill records (corrupt wire bytes or invalid envelopes) are logged with redacted metadata,
// counted in Prometheus (`status="malformed"`), and committed past so a single bad message
// never wedges consumer lag past the 5,000-record alert threshold.
func (c *CDCConsumer) ProcessRecord(ctx context.Context, record CDCRecord) error {
	var envelope notificationv1.NotificationEventEnvelope
	if err := envelope.UnmarshalWire(record.Value); err != nil {
		c.metrics.IncEventsProcessed("malformed", "malformed")
		c.logger.Warn("skipping malformed cdc wire record (poison-pill isolated)",
			slog.String("topic", record.Topic),
			slog.Int("partition", record.Partition),
			slog.Int64("offset", record.Offset),
			slog.Int("payload_bytes_len", len(record.Value)),
			slog.String("error", err.Error()),
		)
		return c.commitRecordOffset(ctx, record)
	}

	if _, err := c.handler.Handle(ctx, &envelope); err != nil {
		if envelope.Validate() != nil {
			// Schema validation failure: isolate poison pill and advance offset.
			c.logger.Warn("skipping invalid cdc event envelope (poison-pill isolated)",
				slog.String("topic", record.Topic),
				slog.Int("partition", record.Partition),
				slog.Int64("offset", record.Offset),
				slog.String("event_id", envelope.EventId),
				slog.String("tenant_id", envelope.TenantId),
				slog.String("error", err.Error()),
			)
			return c.commitRecordOffset(ctx, record)
		}
		// Downstream infrastructure error (e.g., Redis/Queue transient timeout): do not advance offset.
		return fmt.Errorf("process cdc record at %s[%d]@%d: %w", record.Topic, record.Partition, record.Offset, err)
	}

	return c.commitRecordOffset(ctx, record)
}

func (c *CDCConsumer) commitRecordOffset(ctx context.Context, record CDCRecord) error {
	nextOffset := record.Offset + 1
	if c.stream != nil {
		if err := c.stream.CommitOffset(ctx, record.Topic, record.Partition, nextOffset); err != nil {
			return fmt.Errorf("commit offset %s[%d]@%d: %w", record.Topic, record.Partition, nextOffset, err)
		}
	}
	c.mu.Lock()
	c.committedOffsets[partitionKey{topic: record.Topic, partition: record.Partition}] = nextOffset
	c.mu.Unlock()
	return nil
}

// CommittedOffset returns the highest committed next-offset watermark for (topic, partition),
// or -1 if no offset has been committed for that partition yet.
func (c *CDCConsumer) CommittedOffset(topic string, partition int) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	offset, ok := c.committedOffsets[partitionKey{topic: topic, partition: partition}]
	if !ok {
		return -1
	}
	return offset
}

// IsConsuming reports whether the CDC consumer loop is actively running.
func (c *CDCConsumer) IsConsuming() bool {
	return c.consuming.Load()
}
