// Package router provides pure in-memory notification routing evaluation, deduplication,
// and per-tenant burst rate-limiting logic with zero external I/O dependencies.
package router

import (
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
)

// RoutingDecision encapsulates the pure in-memory evaluation outcome for a single NotificationEvent.
type RoutingDecision struct {
	TenantID              string
	EventID               string
	DeliverSSE            bool
	DeliverWebhook        bool
	Suppressed            bool
	EffectiveRateLimitRPS int
	MatchedSubscriptions  []string
	MatchedWebhookTargets []entity.WebhookTarget
}

// ShouldDispatch reports whether the event requires downstream SSE or Webhook dispatch.
func (d RoutingDecision) ShouldDispatch() bool {
	return !d.Suppressed && (d.DeliverSSE || (d.DeliverWebhook && len(d.MatchedWebhookTargets) > 0))
}

// RuleEvaluator evaluates NotificationEvents against pod-local cached tenant subscriptions
// and webhook targets without performing any synchronous database reads (ADR-004).
type RuleEvaluator struct{}

// NewRuleEvaluator creates a stateless in-memory RuleEvaluator.
func NewRuleEvaluator() *RuleEvaluator {
	return &RuleEvaluator{}
}

// Evaluate computes the RoutingDecision for an event given the tenant's cached subscriptions and webhook targets.
func (e *RuleEvaluator) Evaluate(
	event entity.NotificationEvent,
	subscriptions []entity.Subscription,
	webhookTargets []entity.WebhookTarget,
) RoutingDecision {
	decision := RoutingDecision{
		TenantID: event.TenantID,
		EventID:  event.EventID,
	}

	if len(subscriptions) == 0 {
		return decision
	}

	// Check if any matching rule explicitly suppresses the event type for this tenant.
	for _, sub := range subscriptions {
		if sub.TenantID == event.TenantID && sub.Suppressed && entity.MatchEventPattern(sub.EventTypePattern, event.EventType) {
			decision.Suppressed = true
			return decision
		}
	}

	for _, sub := range subscriptions {
		if !sub.MatchesEvent(event) {
			continue
		}
		decision.MatchedSubscriptions = append(decision.MatchedSubscriptions, sub.SubscriptionID)
		if sub.SSEEnabled {
			decision.DeliverSSE = true
		}
		if sub.WebhookEnabled {
			decision.DeliverWebhook = true
		}
		rps := sub.EffectiveRateLimitRPS()
		if rps > decision.EffectiveRateLimitRPS {
			decision.EffectiveRateLimitRPS = rps
		}
	}

	if decision.DeliverWebhook && len(webhookTargets) > 0 {
		seenTargets := make(map[string]struct{}, len(webhookTargets))
		for _, target := range webhookTargets {
			if !target.MatchesEvent(event) {
				continue
			}
			if _, exists := seenTargets[target.TargetID]; exists {
				continue
			}
			seenTargets[target.TargetID] = struct{}{}
			decision.MatchedWebhookTargets = append(decision.MatchedWebhookTargets, target)
		}
		if len(decision.MatchedWebhookTargets) == 0 {
			decision.DeliverWebhook = false
		}
	} else {
		decision.DeliverWebhook = false
	}

	return decision
}
