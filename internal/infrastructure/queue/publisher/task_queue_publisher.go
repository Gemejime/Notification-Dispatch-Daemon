// Package publisher implements outbound message broker publishers for rag-notification-dispatch-daemon.
package publisher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	notificationv1 "github.com/gemejime/rag-notification-dispatch-daemon/api/proto/v1"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
)

const (
	// DefaultWebhookQueueStream is the default Redis Stream / broker routing key for outbound webhook delivery tasks.
	DefaultWebhookQueueStream = "rag:webhook:tasks"
)

// StreamMessagePublisher abstracts outbound stream brokers (Redis Streams XADD, RabbitMQ, etc.).
type StreamMessagePublisher interface {
	PublishStreamMessage(ctx context.Context, stream string, payload []byte) error
}

// TaskQueuePublisher implements command.WebhookTaskEnqueuer, serializing WebhookDeliveryTask
// to binary Protobuf wire format (MarshalWire) and publishing to the outbound task stream (ADR-001).
type TaskQueuePublisher struct {
	streamName string
	broker     StreamMessagePublisher
	logger     *slog.Logger
	metrics    *observability.MetricsRegistry
}

// NewTaskQueuePublisher creates a new TaskQueuePublisher.
func NewTaskQueuePublisher(
	streamName string,
	broker StreamMessagePublisher,
	logger *slog.Logger,
	metrics *observability.MetricsRegistry,
) *TaskQueuePublisher {
	if streamName == "" {
		streamName = DefaultWebhookQueueStream
	}
	if logger == nil {
		logger = observability.NewSecuredLogger(nil, slog.LevelInfo)
	}
	return &TaskQueuePublisher{
		streamName: streamName,
		broker:     broker,
		logger:     logger,
		metrics:    metrics,
	}
}

// EnqueueWebhookTask serializes the task into Protobuf v3 binary wire format and writes to the broker stream.
// Fulfills the P99 <= 15ms hand-off latency requirement (ADR-001).
func (p *TaskQueuePublisher) EnqueueWebhookTask(ctx context.Context, task *notificationv1.WebhookDeliveryTask) error {
	if task == nil {
		return errors.New("cannot enqueue nil webhook delivery task")
	}

	wireBytes, err := task.MarshalWire()
	if err != nil {
		return fmt.Errorf("marshal wire webhook task %s: %w", task.TaskId, err)
	}

	if p.broker == nil {
		return errors.New("broker publisher is nil")
	}

	if err := p.broker.PublishStreamMessage(ctx, p.streamName, wireBytes); err != nil {
		return fmt.Errorf("publish task to stream %s: %w", p.streamName, err)
	}

	return nil
}

// InMemoryTaskQueueBroker provides an in-memory stream broker for testing and local development.
type InMemoryTaskQueueBroker struct {
	mu       sync.Mutex
	messages map[string][][]byte
}

// NewInMemoryTaskQueueBroker creates a new in-memory task broker.
func NewInMemoryTaskQueueBroker() *InMemoryTaskQueueBroker {
	return &InMemoryTaskQueueBroker{
		messages: make(map[string][][]byte),
	}
}

// PublishStreamMessage appends the wire payload to the in-memory stream buffer.
func (b *InMemoryTaskQueueBroker) PublishStreamMessage(_ context.Context, stream string, payload []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	cp := make([]byte, len(payload))
	copy(cp, payload)
	b.messages[stream] = append(b.messages[stream], cp)
	return nil
}

// GetMessages returns all payloads published to the given stream.
func (b *InMemoryTaskQueueBroker) GetMessages(stream string) [][]byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	msgs := b.messages[stream]
	out := make([][]byte, len(msgs))
	for i, m := range msgs {
		out[i] = make([]byte, len(m))
		copy(out[i], m)
	}
	return out
}

// MessageCount returns the number of payloads enqueued in the given stream.
func (b *InMemoryTaskQueueBroker) MessageCount(stream string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.messages[stream])
}

// Reset clears all captured stream messages.
func (b *InMemoryTaskQueueBroker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.messages = make(map[string][][]byte)
}
