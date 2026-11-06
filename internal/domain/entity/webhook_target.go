package entity

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
	"time"
)

// DefaultWebhookSignatureHeader is the default HTTP header carrying the HMAC-SHA256 signature.
const DefaultWebhookSignatureHeader = "X-RAG-Signature-256"

// WebhookTarget represents a customer webhook endpoint configuration stored in PostgreSQL
// and cached in pod-local memory for asynchronous webhook task enqueueing (ADR-001).
type WebhookTarget struct {
	TargetID            string
	TenantID            string
	TargetURL           string
	SecretRef           string
	SignatureHeaderName string
	Active              bool
	EventTypes          []string
	UpdatedAt           time.Time
}

// Validate checks all mandatory fields on WebhookTarget.
func (w WebhookTarget) Validate() error {
	if w.TargetID == "" {
		return errors.New("target_id is required")
	}
	if w.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if w.TargetURL == "" || (!strings.HasPrefix(w.TargetURL, "https://") && !strings.HasPrefix(w.TargetURL, "http://")) {
		return errors.New("target_url must be a valid HTTP/HTTPS URL")
	}
	if w.SecretRef == "" {
		return errors.New("secret_ref is required")
	}
	if len(w.EventTypes) == 0 {
		return errors.New("event_types must contain at least one pattern")
	}
	return nil
}

// EffectiveSignatureHeader returns the configured signature header name or DefaultWebhookSignatureHeader.
func (w WebhookTarget) EffectiveSignatureHeader() string {
	if w.SignatureHeaderName == "" {
		return DefaultWebhookSignatureHeader
	}
	return w.SignatureHeaderName
}

// MatchesEvent reports whether this webhook target is active for the event's tenant and event type.
func (w WebhookTarget) MatchesEvent(event NotificationEvent) bool {
	if !w.Active || w.TenantID != event.TenantID {
		return false
	}
	for _, pattern := range w.EventTypes {
		if MatchEventPattern(pattern, event.EventType) {
			return true
		}
	}
	return false
}

// DeterministicTaskID computes the idempotent webhook delivery task ID:
// sha256(tenant_id + ":" + event_id + ":" + target_id).
func DeterministicTaskID(tenantID, eventID, targetID string) string {
	sum := sha256.Sum256([]byte(tenantID + ":" + eventID + ":" + targetID))
	return hex.EncodeToString(sum[:])
}

// LogValue implements slog.LogValuer to enforce Secured Logging redaction of SecretRef.
func (w WebhookTarget) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("target_id", w.TargetID),
		slog.String("tenant_id", w.TenantID),
		slog.String("target_url", w.TargetURL),
		slog.String("secret_ref", "[REDACTED]"),
		slog.String("signature_header_name", w.EffectiveSignatureHeader()),
		slog.Bool("active", w.Active),
	)
}
