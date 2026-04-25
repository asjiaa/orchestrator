package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/asjiaa/orchestrator/internal/queue"
	"github.com/asjiaa/orchestrator/internal/store"
)

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}

// Prevent collisions on unauthenticated tenants
type tenantCtxKey struct{}

func Logger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rw, r)

		slog.InfoContext(r.Context(), "request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rw.status,
			"latency_ms", time.Since(start).Milliseconds(),
			"remote_addr", r.RemoteAddr,
		)
	})
}

func TenantFromCtx(ctx context.Context) store.Tenant {
	t, extg := ctx.Value(tenantCtxKey{}).(store.Tenant)
	if !extg {
		panic("TenantFromCtx: no tenant in context") // catch no middleware
	}
	return t
}

func AuthMiddleware(st store.Store) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, auth := extractBearer(r)
			if !auth {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}

			sum := sha256.Sum256([]byte(raw))
			keyHash := hex.EncodeToString(sum[:])

			tenant, err := st.GetTenantByKeyHash(r.Context(), keyHash)

			if errors.Is(err, store.ErrNotFound) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}

			if err != nil {
				http.Error(w, "internal server error", http.StatusInternalServerError)
				return
			}

			ctx := context.WithValue(r.Context(), tenantCtxKey{}, *tenant)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func RateLimitMiddleware(rl *queue.RateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tenant := TenantFromCtx(r.Context())

			_, allowed, err := rl.Allow(r.Context(), tenant.ID, tenant.RateLimitRPS)
			if err != nil {
				slog.ErrorContext(r.Context(), "rate limit check failed",
					"tenant_id", tenant.ID, "error", err)
				http.Error(w, "internal server error", http.StatusInternalServerError)
				return
			}
			if !allowed {
				w.Header().Set("Retry-After", "1")
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func extractBearer(r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return "", false
	}
	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return "", false
	}
	token := strings.TrimSpace(parts[1])
	if token == "" {
		return "", false
	}
	return token, true
}
