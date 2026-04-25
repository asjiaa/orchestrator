package api

import (
	"net/http"

	"github.com/asjiaa/orchestrator/internal/queue"
	"github.com/asjiaa/orchestrator/internal/store"
	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
)

func NewRouter(h *Handler, s store.Store, rl *queue.RateLimiter) http.Handler {
	r := chi.NewRouter()

	// Run per request
	r.Use(chimiddleware.RequestID) // inject header to context
	r.Use(chimiddleware.Recoverer) // catch panic
	r.Use(Logger)                  // stuctured per request logging

	r.Use(AuthMiddleware(s))

	r.With(RateLimitMiddleware(rl)).Post("/jobs", h.CreateJob) // throttle pipeline entry

	r.Get("/jobs", h.ListJobs)
	r.Get("/jobs/{id}", h.GetJob)
	r.Post("/jobs/{id}/retry", h.RetryJob)

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	return r
}
