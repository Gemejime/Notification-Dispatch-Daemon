// Package redis provides Redis 7.2 Cluster adapters for cache invalidation pub/sub (`tenant.config.updated`)
// and targeted node presence routing.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/repository"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
)

const (
	// TenantConfigUpdatedChannel is the canonical Redis Pub/Sub channel for cache invalidation (ADR-004).
	TenantConfigUpdatedChannel = "tenant.config.updated"
	// DefaultInvalidationReloadTimeout enforces the <= 100ms cache coherence SLO during reload-on-invalidation.
	DefaultInvalidationReloadTimeout = 80 * time.Millisecond
)

// InvalidationMessage represents the JSON payload broadcast on `tenant.config.updated`.
type InvalidationMessage struct {
	TenantID        string `json:"tenant_id"`
	Reason          string `json:"reason,omitempty"`
	UpdatedAtUnixMs int64  `json:"updated_at_unix_ms"`
}

// PubSubSubscription represents an active subscription to a Redis Pub/Sub channel.
type PubSubSubscription interface {
	Messages() <-chan []byte
	Close() error
}

// PubSubClient abstracts Redis Pub/Sub subscribing and publishing for `tenant.config.updated`.
type PubSubClient interface {
	Subscribe(ctx context.Context, channel string) (PubSubSubscription, error)
	Publish(ctx context.Context, channel string, payload []byte) error
}

// InMemoryPubSubClient provides an in-process PubSubClient implementation with connection health reporting.
type InMemoryPubSubClient struct {
	ch        chan []byte
	connected atomic.Bool
}

// NewInMemoryPubSubClient creates a connected InMemoryPubSubClient.
func NewInMemoryPubSubClient() *InMemoryPubSubClient {
	c := &InMemoryPubSubClient{
		ch: make(chan []byte, 64),
	}
	c.connected.Store(true)
	return c
}

// Subscribe returns an active subscription reading from the in-memory pub/sub channel.
func (c *InMemoryPubSubClient) Subscribe(_ context.Context, _ string) (PubSubSubscription, error) {
	if !c.connected.Load() {
		return nil, errors.New("redis backplane disconnected")
	}
	return c, nil
}

// Publish sends a payload to the in-memory pub/sub channel.
func (c *InMemoryPubSubClient) Publish(ctx context.Context, _ string, payload []byte) error {
	if !c.connected.Load() {
		return errors.New("redis backplane disconnected")
	}
	cp := make([]byte, len(payload))
	copy(cp, payload)
	select {
	case c.ch <- cp:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Messages returns the receive channel for subscribed messages.
func (c *InMemoryPubSubClient) Messages() <-chan []byte { return c.ch }

// Close closes the subscription.
func (c *InMemoryPubSubClient) Close() error { return nil }

// IsConnected reports whether the Redis backplane client is connected and healthy.
func (c *InMemoryPubSubClient) IsConnected() bool {
	return c != nil && c.connected.Load()
}

// SetConnected updates the connection health state of the Redis backplane client.
func (c *InMemoryPubSubClient) SetConnected(connected bool) {
	c.connected.Store(connected)
}

// InvalidationSubscriber listens on `tenant.config.updated` and performs asynchronous
// Reload-on-Invalidation from the cold-path PostgreSQL repositories within <= 100ms,
// ensuring subsequent hot-path CDC events never suffer a false cache-miss blackhole.
type InvalidationSubscriber struct {
	pubsub        PubSubClient
	cache         repository.SubscriptionCache
	subRepo       repository.SubscriptionRepository
	webhookRepo   repository.WebhookTargetRepository
	logger        *slog.Logger
	metrics       *observability.MetricsRegistry
	reloadTimeout time.Duration
}

// NewInvalidationSubscriber constructs an InvalidationSubscriber for `tenant.config.updated`.
func NewInvalidationSubscriber(
	pubsub PubSubClient,
	cache repository.SubscriptionCache,
	subRepo repository.SubscriptionRepository,
	webhookRepo repository.WebhookTargetRepository,
	logger *slog.Logger,
	metrics *observability.MetricsRegistry,
) *InvalidationSubscriber {
	if logger == nil {
		logger = observability.NewSecuredLogger(nil, slog.LevelInfo)
	}
	if metrics == nil {
		metrics = observability.NewMetricsRegistry()
	}
	return &InvalidationSubscriber{
		pubsub:        pubsub,
		cache:         cache,
		subRepo:       subRepo,
		webhookRepo:   webhookRepo,
		logger:        logger,
		metrics:       metrics,
		reloadTimeout: DefaultInvalidationReloadTimeout,
	}
}

// Run subscribes to `tenant.config.updated` and processes invalidation messages until ctx is canceled.
func (s *InvalidationSubscriber) Run(ctx context.Context) error {
	if s.pubsub == nil {
		return errors.New("pubsub client is required")
	}
	sub, err := s.pubsub.Subscribe(ctx, TenantConfigUpdatedChannel)
	if err != nil {
		return fmt.Errorf("subscribe to %s: %w", TenantConfigUpdatedChannel, err)
	}
	defer func() {
		_ = sub.Close()
	}()

	msgCh := sub.Messages()
	for {
		select {
		case <-ctx.Done():
			return nil
		case raw, ok := <-msgCh:
			if !ok {
				return nil
			}
			if err := s.HandleInvalidationPayload(ctx, raw); err != nil {
				s.logger.Warn("failed to process cache invalidation message",
					slog.String("channel", TenantConfigUpdatedChannel),
					slog.String("error", err.Error()),
				)
			}
		}
	}
}

// HandleInvalidationPayload parses a `tenant.config.updated` JSON message, reloads the tenant's
// latest rules from the cold-path repositories within the <= 100ms coherence budget, and atomically
// updates the pod-local SubscriptionCache.
func (s *InvalidationSubscriber) HandleInvalidationPayload(ctx context.Context, raw []byte) error {
	var msg InvalidationMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		s.metrics.IncCacheInvalidations("malformed_payload")
		return fmt.Errorf("unmarshal invalidation message: %w", err)
	}
	if msg.TenantID == "" {
		s.metrics.IncCacheInvalidations("missing_tenant_id")
		return errors.New("invalidation message missing tenant_id")
	}

	reason := msg.Reason
	if reason == "" {
		reason = "tenant_config_updated"
	}

	reloadCtx, cancel := context.WithTimeout(ctx, s.reloadTimeout)
	defer cancel()

	var subs []entity.Subscription
	var targets []entity.WebhookTarget
	var err error

	if s.subRepo != nil {
		subs, err = s.subRepo.ListByTenant(reloadCtx, msg.TenantID)
		if err != nil {
			s.metrics.IncCacheInvalidations("reload_error")
			return fmt.Errorf("reload subscriptions for tenant %s: %w", msg.TenantID, err)
		}
	}

	if s.webhookRepo != nil {
		targets, err = s.webhookRepo.ListByTenant(reloadCtx, msg.TenantID)
		if err != nil {
			s.metrics.IncCacheInvalidations("reload_error")
			return fmt.Errorf("reload webhook targets for tenant %s: %w", msg.TenantID, err)
		}
	}

	if s.cache != nil {
		s.cache.ReplaceTenantRules(msg.TenantID, subs, targets)
	}
	s.metrics.IncCacheInvalidations(reason)

	s.logger.Info("pod-local tenant subscription cache reloaded on invalidation",
		slog.String("tenant_id", msg.TenantID),
		slog.String("reason", reason),
		slog.Int("subscriptions_count", len(subs)),
		slog.Int("webhook_targets_count", len(targets)),
	)
	return nil
}

// BroadcastInvalidation publishes a `tenant.config.updated` message to all daemon pods.
func (s *InvalidationSubscriber) BroadcastInvalidation(ctx context.Context, tenantID, reason string) error {
	if tenantID == "" {
		return errors.New("tenant_id is required")
	}
	if reason == "" {
		reason = "manual_invalidation"
	}
	msg := InvalidationMessage{
		TenantID:        tenantID,
		Reason:          reason,
		UpdatedAtUnixMs: time.Now().UTC().UnixMilli(),
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal invalidation message: %w", err)
	}
	if s.pubsub != nil {
		if err := s.pubsub.Publish(ctx, TenantConfigUpdatedChannel, payload); err != nil {
			return fmt.Errorf("publish invalidation to %s: %w", TenantConfigUpdatedChannel, err)
		}
	}
	return s.HandleInvalidationPayload(ctx, payload)
}
