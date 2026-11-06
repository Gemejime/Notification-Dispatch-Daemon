// Package memory provides the thread-safe pod-local in-memory subscription and webhook target cache.
// CRITICAL INVARIANT (ADR-004): All hot-path CDC routing evaluations read exclusively from this
// in-memory cache with zero synchronous SQL queries.
package memory

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/repository"
)

type tenantRuleEntry struct {
	subscriptions  []entity.Subscription
	webhookTargets []entity.WebhookTarget
}

// SubscriptionCache is a high-concurrency, RWMutex-guarded pod-local read cache storing
// tenant notification subscriptions and webhook targets.
type SubscriptionCache struct {
	mu      sync.RWMutex
	entries map[string]tenantRuleEntry
	warmed  atomic.Bool
}

// NewSubscriptionCache creates an empty pod-local SubscriptionCache.
func NewSubscriptionCache() *SubscriptionCache {
	return &SubscriptionCache{
		entries: make(map[string]tenantRuleEntry, 256),
	}
}

// WarmUp hydrates the pod-local cache on cold start from the cold-path PostgreSQL repositories.
func (c *SubscriptionCache) WarmUp(
	ctx context.Context,
	subRepo repository.SubscriptionRepository,
	webhookRepo repository.WebhookTargetRepository,
) error {
	var subs []entity.Subscription
	var targets []entity.WebhookTarget
	var err error

	if subRepo != nil {
		subs, err = subRepo.ListAllActive(ctx)
		if err != nil {
			return fmt.Errorf("warm up subscriptions from cold store: %w", err)
		}
	}
	if webhookRepo != nil {
		targets, err = webhookRepo.ListAllActive(ctx)
		if err != nil {
			return fmt.Errorf("warm up webhook targets from cold store: %w", err)
		}
	}

	c.ReplaceAll(subs, targets)
	return nil
}

// ReplaceAll atomically replaces the entire pod-local cache state and marks the cache as warmed.
func (c *SubscriptionCache) ReplaceAll(subs []entity.Subscription, targets []entity.WebhookTarget) {
	next := make(map[string]tenantRuleEntry, 64)

	for _, s := range subs {
		if s.TenantID == "" {
			continue
		}
		entry := next[s.TenantID]
		entry.subscriptions = append(entry.subscriptions, s)
		next[s.TenantID] = entry
	}

	for _, t := range targets {
		if t.TenantID == "" {
			continue
		}
		entry := next[t.TenantID]
		entry.webhookTargets = append(entry.webhookTargets, copyWebhookTarget(t))
		next[t.TenantID] = entry
	}

	c.mu.Lock()
	c.entries = next
	c.warmed.Store(true)
	c.mu.Unlock()
}

// ReplaceTenantRules atomically updates a single tenant's cached subscriptions and webhook targets.
// If both slices are empty, the tenant key is removed from the map to conserve pod memory.
func (c *SubscriptionCache) ReplaceTenantRules(
	tenantID string,
	subs []entity.Subscription,
	targets []entity.WebhookTarget,
) {
	if tenantID == "" {
		return
	}

	copiedSubs := make([]entity.Subscription, len(subs))
	copy(copiedSubs, subs)

	copiedTargets := make([]entity.WebhookTarget, len(targets))
	for i, t := range targets {
		copiedTargets[i] = copyWebhookTarget(t)
	}

	c.mu.Lock()
	if len(copiedSubs) == 0 && len(copiedTargets) == 0 {
		delete(c.entries, tenantID)
	} else {
		c.entries[tenantID] = tenantRuleEntry{
			subscriptions:  copiedSubs,
			webhookTargets: copiedTargets,
		}
	}
	c.warmed.Store(true)
	c.mu.Unlock()
}

// EvictTenant removes a tenant's cached rules from pod-local memory.
func (c *SubscriptionCache) EvictTenant(tenantID string) {
	if tenantID == "" {
		return
	}
	c.mu.Lock()
	delete(c.entries, tenantID)
	c.mu.Unlock()
}

// GetTenantRules retrieves a defensive copy of the tenant's cached subscriptions and webhook targets
// with zero database I/O.
func (c *SubscriptionCache) GetTenantRules(tenantID string) ([]entity.Subscription, []entity.WebhookTarget) {
	c.mu.RLock()
	entry, exists := c.entries[tenantID]
	if !exists {
		c.mu.RUnlock()
		return nil, nil
	}

	subs := make([]entity.Subscription, len(entry.subscriptions))
	copy(subs, entry.subscriptions)

	targets := make([]entity.WebhookTarget, len(entry.webhookTargets))
	for i, t := range entry.webhookTargets {
		targets[i] = copyWebhookTarget(t)
	}
	c.mu.RUnlock()

	return subs, targets
}

// IsWarmed reports whether the cache has completed initial hydration.
func (c *SubscriptionCache) IsWarmed() bool {
	return c.warmed.Load()
}

// TenantCount returns the number of active tenants currently cached in memory.
func (c *SubscriptionCache) TenantCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

func copyWebhookTarget(t entity.WebhookTarget) entity.WebhookTarget {
	eventsCopy := make([]string, len(t.EventTypes))
	copy(eventsCopy, t.EventTypes)
	t.EventTypes = eventsCopy
	return t
}
