package entity

import (
	"errors"
	"strings"
	"time"
)

// DefaultSubscriptionRateLimitRPS is the default per-tenant notification rate limit in requests/events per second.
const DefaultSubscriptionRateLimitRPS = 100

// Subscription represents a tenant's notification routing rule cached in pod-local memory (ADR-004).
type Subscription struct {
	SubscriptionID   string
	TenantID         string
	EventTypePattern string
	MinSeverity      Severity
	SSEEnabled       bool
	WebhookEnabled   bool
	Suppressed       bool
	RateLimitRPS     int
	UpdatedAt        time.Time
}

// Validate checks that the subscription configuration is well-formed.
func (s Subscription) Validate() error {
	if s.SubscriptionID == "" {
		return errors.New("subscription_id is required")
	}
	if s.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if s.EventTypePattern == "" {
		return errors.New("event_type_pattern is required")
	}
	if !s.MinSeverity.IsValid() {
		return errors.New("min_severity must be a valid Severity")
	}
	if s.RateLimitRPS < 0 {
		return errors.New("rate_limit_rps cannot be negative")
	}
	return nil
}

// EffectiveRateLimitRPS returns RateLimitRPS or DefaultSubscriptionRateLimitRPS when unset.
func (s Subscription) EffectiveRateLimitRPS() int {
	if s.RateLimitRPS <= 0 {
		return DefaultSubscriptionRateLimitRPS
	}
	return s.RateLimitRPS
}

// MatchesEvent evaluates whether the subscription matches the given NotificationEvent
// based on tenant isolation, suppression state, severity threshold, and event type pattern.
func (s Subscription) MatchesEvent(event NotificationEvent) bool {
	if s.TenantID != event.TenantID {
		return false
	}
	if s.Suppressed {
		return false
	}
	if event.Severity.Rank() < s.MinSeverity.Rank() {
		return false
	}
	return MatchEventPattern(s.EventTypePattern, event.EventType)
}

// MatchEventPattern supports exact matches ("ingestion.milestone"), global wildcard ("*"),
// and namespace prefix wildcards ("ingestion.*").
func MatchEventPattern(pattern, eventType string) bool {
	if pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, ".*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(eventType, prefix)
	}
	return pattern == eventType
}
