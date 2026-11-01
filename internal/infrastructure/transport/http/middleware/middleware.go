// Package middleware provides HTTP security, authentication, correlation tracking,
// rate limiting, and Secured Logging middlewares for rag-notification-dispatch-daemon.
package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/observability"
)

// CorrelationIDMiddleware injects or propagates X-Correlation-ID / trace ID across the request context.
func CorrelationIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		corrID := strings.TrimSpace(r.Header.Get("X-Correlation-ID"))
		if corrID == "" {
			corrID = strings.TrimSpace(r.Header.Get("X-Request-ID"))
		}
		if corrID == "" {
			var b [8]byte
			_, _ = rand.Read(b[:])
			corrID = fmt.Sprintf("req-%d-%s", time.Now().UnixNano(), hex.EncodeToString(b[:]))
		}

		ctx := context.WithValue(r.Context(), entity.ContextKeyTraceID, corrID)
		w.Header().Set("X-Correlation-ID", corrID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (rec *statusRecorder) WriteHeader(code int) {
	rec.statusCode = code
	rec.ResponseWriter.WriteHeader(code)
}

func (rec *statusRecorder) Flush() {
	if f, ok := rec.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// SecuredLoggingMiddleware logs incoming HTTP requests using structured slog JSON
// without leaking raw Authorization tokens or PII.
func SecuredLoggingMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	if logger == nil {
		logger = observability.NewSecuredLogger(nil, slog.LevelInfo)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}

			next.ServeHTTP(rec, r)

			// Exclude noisy operational probe logs unless debug is enabled.
			if (r.URL.Path == "/healthz" || r.URL.Path == "/ready" || r.URL.Path == "/metrics") && rec.statusCode == http.StatusOK {
				return
			}

			corrID, _ := r.Context().Value(entity.ContextKeyTraceID).(string)
			tenantID, _ := r.Context().Value(entity.ContextKeyTenantID).(string)

			logger.Info("http request completed",
				slog.String("trace_id", corrID),
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", rec.statusCode),
				slog.Duration("duration_ms", time.Since(start)),
				slog.String("tenant_id", tenantID),
			)
		})
	}
}

// PanicRecoveryMiddleware catches unhandled panics, sanitizes errors, and returns a 500 JSON response.
func PanicRecoveryMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	if logger == nil {
		logger = observability.NewSecuredLogger(nil, slog.LevelInfo)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					corrID, _ := r.Context().Value(entity.ContextKeyTraceID).(string)
					logger.Error("sanitized unhandled http panic recovered",
						slog.String("trace_id", corrID),
						slog.String("path", r.URL.Path),
						slog.String("error", fmt.Sprint(rec)),
					)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(`{"error":"internal_server_error"}`))
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// Authenticator validates Bearer tokens and extracts TenantID and UserID claims.
type Authenticator interface {
	Authenticate(token string) (tenantID string, userID string, err error)
}

// SimpleTokenAuthenticator parses structured development tokens or claims: "tenant_id[:user_id]".
type SimpleTokenAuthenticator struct{}

// Authenticate extracts claims from token format "Bearer <tenant_id>" or "Bearer <tenant_id>:<user_id>".
// Also supports standard dev tokens like "valid-token" -> "tnt_01".
func (a SimpleTokenAuthenticator) Authenticate(token string) (string, string, error) {
	trimmed := strings.TrimSpace(token)
	if trimmed == "" {
		return "", "", fmt.Errorf("empty bearer token")
	}

	// Simulated dev/test tokens
	if trimmed == "valid-token" || trimmed == "test-token" {
		return "tnt_01", "usr_01", nil
	}

	// Direct claims representation: "tnt_xxx:usr_yyy" or "tnt_xxx"
	parts := strings.SplitN(trimmed, ":", 2)
	tenantID := parts[0]
	var userID string
	if len(parts) == 2 {
		userID = parts[1]
	}

	if tenantID == "" {
		return "", "", fmt.Errorf("invalid token claims")
	}
	return tenantID, userID, nil
}

// AuthMiddleware enforces Bearer authentication and injects TenantID and UserID into the request context.
func AuthMiddleware(auth Authenticator) func(http.Handler) http.Handler {
	if auth == nil {
		auth = SimpleTokenAuthenticator{}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rawHeader := strings.TrimSpace(r.Header.Get("Authorization"))
			if !strings.HasPrefix(rawHeader, "Bearer ") {
				// Query parameter fallback for SSE EventSource clients that cannot set Authorization headers
				rawHeader = "Bearer " + strings.TrimSpace(r.URL.Query().Get("token"))
			}

			token := strings.TrimSpace(strings.TrimPrefix(rawHeader, "Bearer "))
			if token == "" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
				return
			}

			tenantID, userID, err := auth.Authenticate(token)
			if err != nil || tenantID == "" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
				return
			}

			ctx := context.WithValue(r.Context(), entity.ContextKeyTenantID, tenantID)
			if userID != "" {
				ctx = context.WithValue(ctx, entity.ContextKeyUserID, userID)
			}

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// TenantAuthorizationMiddleware enforces strict per-tenant isolation by ensuring
// query parameter tenant_id matches the authenticated tenant from the context (403 Forbidden).
func TenantAuthorizationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authedTenant, _ := r.Context().Value(entity.ContextKeyTenantID).(string)
		requestedTenant := strings.TrimSpace(r.URL.Query().Get("tenant_id"))

		if requestedTenant != "" && authedTenant != "" && requestedTenant != authedTenant {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":"forbidden_cross_tenant_access"}`))
			return
		}

		next.ServeHTTP(w, r)
	})
}

type tokenBucket struct {
	tokens     float64
	lastRefill time.Time
}

// ReconnectRateLimiter implements an in-memory token-bucket rate limiter per tenant/IP to protect against DDoS.
type ReconnectRateLimiter struct {
	mu       sync.Mutex
	capacity float64
	refill   float64 // tokens per second
	buckets  map[string]*tokenBucket
	maxKeys  int
}

// NewReconnectRateLimiter constructs a token-bucket rate limiter.
func NewReconnectRateLimiter(ratePerSec float64, burstCapacity int, maxKeys int) *ReconnectRateLimiter {
	if ratePerSec <= 0 {
		ratePerSec = 100
	}
	if burstCapacity <= 0 {
		burstCapacity = 200
	}
	if maxKeys <= 0 {
		maxKeys = 10000
	}
	return &ReconnectRateLimiter{
		capacity: float64(burstCapacity),
		refill:   ratePerSec,
		buckets:  make(map[string]*tokenBucket),
		maxKeys:  maxKeys,
	}
}

// Allow checks if a request from key is permitted under the rate limit.
func (rl *ReconnectRateLimiter) Allow(key string, now time.Time) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	b, ok := rl.buckets[key]
	if !ok {
		if len(rl.buckets) >= rl.maxKeys {
			// Evict arbitrary entry to protect against memory exhaustion
			for k := range rl.buckets {
				delete(rl.buckets, k)
				break
			}
		}
		rl.buckets[key] = &tokenBucket{
			tokens:     rl.capacity - 1,
			lastRefill: now,
		}
		return true
	}

	elapsed := now.Sub(b.lastRefill).Seconds()
	b.lastRefill = now
	b.tokens += elapsed * rl.refill
	if b.tokens > rl.capacity {
		b.tokens = rl.capacity
	}

	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		return true
	}

	return false
}

// RateLimitMiddleware enforces token-bucket limits on connection establishment.
func RateLimitMiddleware(limiter *ReconnectRateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if limiter != nil {
				key := r.RemoteAddr
				if t, ok := r.Context().Value(entity.ContextKeyTenantID).(string); ok && t != "" {
					key = t
				}
				if !limiter.Allow(key, time.Now().UTC()) {
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("Retry-After", "5")
					w.WriteHeader(http.StatusTooManyRequests)
					_ = json.NewEncoder(w).Encode(map[string]string{
						"error": "rate_limited",
					})
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}
