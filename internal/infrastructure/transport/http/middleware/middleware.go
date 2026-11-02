// Package middleware provides HTTP security, authentication, correlation tracking,
// rate limiting, and Secured Logging middlewares for rag-notification-dispatch-daemon.
package middleware

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
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

// Unwrap exposes the underlying ResponseWriter for http.ResponseController compatibility.
func (rec *statusRecorder) Unwrap() http.ResponseWriter {
	return rec.ResponseWriter
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

// JWTAuthenticator verifies JSON Web Tokens using a PEM-encoded public key.
type JWTAuthenticator struct {
	publicKey crypto.PublicKey
}

// NewJWTAuthenticator initializes a JWTAuthenticator by parsing a PEM-encoded RSA, ECDSA, or Ed25519 public key.
func NewJWTAuthenticator(publicKeyPEM string) (*JWTAuthenticator, error) {
	trimmed := strings.TrimSpace(publicKeyPEM)
	if trimmed == "" {
		return &JWTAuthenticator{}, nil
	}
	block, _ := pem.Decode([]byte(trimmed))
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block for jwt public key")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		if rsaPub, rsaErr := x509.ParsePKCS1PublicKey(block.Bytes); rsaErr == nil {
			return &JWTAuthenticator{publicKey: rsaPub}, nil
		}
		return nil, fmt.Errorf("parse jwt public key: %w", err)
	}
	return &JWTAuthenticator{publicKey: pub}, nil
}

// Authenticate verifies the JWT signature and extracts tenant_id and user_id claims.
func (a *JWTAuthenticator) Authenticate(token string) (string, string, error) {
	if a.publicKey == nil {
		return "", "", errors.New("jwt public key not configured")
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", "", errors.New("invalid jwt format")
	}

	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", "", fmt.Errorf("decode jwt header: %w", err)
	}
	var header struct {
		Alg string `json:"alg"`
	}
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return "", "", fmt.Errorf("unmarshal jwt header: %w", err)
	}

	sigBytes, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", "", fmt.Errorf("decode jwt signature: %w", err)
	}

	signingInput := []byte(parts[0] + "." + parts[1])

	switch header.Alg {
	case "RS256":
		rsaPub, ok := a.publicKey.(*rsa.PublicKey)
		if !ok {
			return "", "", errors.New("public key is not RSA for RS256")
		}
		h := sha256.Sum256(signingInput)
		if err := rsa.VerifyPKCS1v15(rsaPub, crypto.SHA256, h[:], sigBytes); err != nil {
			return "", "", fmt.Errorf("verify rs256 signature: %w", err)
		}
	case "ES256":
		ecdsaPub, ok := a.publicKey.(*ecdsa.PublicKey)
		if !ok {
			return "", "", errors.New("public key is not ECDSA for ES256")
		}
		h := sha256.Sum256(signingInput)
		if !ecdsa.VerifyASN1(ecdsaPub, h[:], sigBytes) {
			return "", "", errors.New("invalid es256 signature")
		}
	case "EdDSA":
		edPub, ok := a.publicKey.(ed25519.PublicKey)
		if !ok {
			return "", "", errors.New("public key is not Ed25519 for EdDSA")
		}
		if !ed25519.Verify(edPub, signingInput, sigBytes) {
			return "", "", errors.New("invalid eddsa signature")
		}
	default:
		return "", "", fmt.Errorf("unsupported jwt alg %q", header.Alg)
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", fmt.Errorf("decode jwt payload: %w", err)
	}

	var claims struct {
		TenantID string `json:"tenant_id"`
		UserID   string `json:"user_id"`
		Sub      string `json:"sub"`
		Exp      int64  `json:"exp"`
	}
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return "", "", fmt.Errorf("unmarshal jwt claims: %w", err)
	}

	if claims.Exp > 0 && time.Now().Unix() > claims.Exp {
		return "", "", errors.New("jwt expired")
	}

	tenantID := claims.TenantID
	if tenantID == "" {
		tenantID = claims.Sub
	}
	if tenantID == "" {
		return "", "", errors.New("missing tenant_id in jwt claims")
	}

	return tenantID, claims.UserID, nil
}

// AuthMiddleware enforces Bearer authentication and injects TenantID and UserID into the request context.
// Fails closed with 401 Unauthorized if auth is nil.
func AuthMiddleware(auth Authenticator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if auth == nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
				return
			}
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
			// Evict only a bucket that has refilled to capacity
			var evictKey string
			found := false
			for k, existing := range rl.buckets {
				elapsed := now.Sub(existing.lastRefill).Seconds()
				currentTokens := existing.tokens + elapsed*rl.refill
				if currentTokens >= rl.capacity {
					evictKey = k
					found = true
					break
				}
			}
			if !found {
				// No bucket has refilled to capacity; reject the new key without evicting an active/exhausted bucket
				return false
			}
			delete(rl.buckets, evictKey)
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
