package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/asjiaa/orchestrator/internal/queue"
	"github.com/asjiaa/orchestrator/internal/storage"
	"github.com/asjiaa/orchestrator/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type Handler struct {
	queue       queue.Queue
	store       store.Store
	storage     *storage.Client
	idempotency *queue.IdempotencyStore
}

func NewHandler(q queue.Queue, s store.Store, st *storage.Client, idem *queue.IdempotencyStore) *Handler {
	return &Handler{queue: q, store: s, storage: st, idempotency: idem}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write json response", "error", err)
	}
}

func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	tenant := TenantFromCtx(r.Context())
	tenantID := tenant.ID
	jobID := uuid.New().String()

	if clientKey := r.Header.Get("X-Idempotency-Key"); clientKey != "" {
		err := h.idempotency.Claim(r.Context(), tenantID, clientKey, jobID)
		var dup *queue.ErrDuplicateRequest
		if errors.As(err, &dup) {
			writeJSON(w, http.StatusOK, map[string]any{
				"job_id": dup.ExistingJobID,
				"status": string(store.StatusPending),
				"note":   "duplicate request",
			})
			return
		}
		if err != nil {
			slog.ErrorContext(r.Context(), "idempotency claim",
				"error", err, "tenant_id", tenantID)
			writeJSON(w, http.StatusInternalServerError, map[string]string{
				"error": "failed to check idempotency",
			})
			return
		}
	}

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid multipart form",
		})
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "missing file field",
		})
		return
	}
	defer file.Close()

	inputKey := fmt.Sprintf("inputs/%s/%s", jobID, header.Filename)

	payload, err := json.Marshal(map[string]string{"input_key": inputKey})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to build payload"})
		return
	}

	job := store.Job{
		ID:       jobID,
		TenantID: tenantID,
		Status:   store.StatusPending,
		InputKey: &inputKey,
		Payload:  payload,
	}
	if err := h.store.CreateJob(r.Context(), job); err != nil {
		slog.ErrorContext(r.Context(), "create job record",
			"error", err,
			"job_id", jobID,
		)
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to create job",
		})
		return
	}

	if err := h.storage.Put(r.Context(), inputKey, header.Header.Get("Content-Type"), file); err != nil {
		slog.ErrorContext(r.Context(), "upload input file",
			"error", err,
			"job_id", jobID,
			"key", inputKey,
		)
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to store file",
		})
		return
	}

	qJob := queue.Job{
		ID:       jobID,
		TenantID: tenantID,
		Payload:  []byte(fmt.Sprintf(`{"input_key":%q}`, inputKey)),
	}
	if err := h.queue.Enqueue(r.Context(), qJob); err != nil {
		slog.ErrorContext(r.Context(), "enqueue job",
			"error", err,
			"job_id", jobID,
		)
		writeJSON(w, http.StatusAccepted, map[string]string{
			"job_id": jobID,
			"status": string(store.StatusPending),
			"note":   "queued via recovery path",
		})
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"job_id": jobID,
		"status": string(store.StatusPending),
	})
}

func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "id")
	if jobID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "missing job id",
		})
		return
	}

	job, err := h.store.GetJob(r.Context(), jobID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": "job not found",
		})
		return
	}

	resp := map[string]any{
		"job_id":                job.ID,
		"status":                string(job.Status),
		"created_at":            job.CreatedAt,
		"updated_at":            job.UpdatedAt,
		"processing_started_at": job.ProcessingStartedAt,
	}

	if job.Status == store.StatusComplete && job.ResultKey != nil {
		resultKey := *job.ResultKey
		if resultKey != "" {
			url, err := h.storage.PresignGet(r.Context(), resultKey, 15*time.Minute)
			if err != nil {
				slog.ErrorContext(r.Context(), "presign result",
					"error", err,
					"job_id", jobID,
					"result_key", resultKey,
				)
			} else {
				resp["result_url"] = url
				resp["result_url_expires_in"] = "15m"
			}
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) ListJobs(w http.ResponseWriter, r *http.Request) {
	tenant := TenantFromCtx(r.Context())
	status := store.JobStatus(r.URL.Query().Get("status"))
	if status == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "missing status query parameter",
		})
		return
	}
	if status != store.StatusDead {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "unsupported status filter",
		})
		return
	}

	jobs, err := h.store.ListJobsByStatus(r.Context(), tenant.ID, status)
	if err != nil {
		slog.ErrorContext(r.Context(), "list jobs by status",
			"error", err,
			"tenant_id", tenant.ID,
			"status", status,
		)
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to list jobs",
		})
		return
	}

	items := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		items = append(items, map[string]any{
			"job_id":     j.ID,
			"status":     string(j.Status),
			"attempts":   j.Attempts,
			"created_at": j.CreatedAt,
			"updated_at": j.UpdatedAt,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status": status,
		"jobs":   items,
	})
}

// Trigger attempt loop reset for dead job
func (h *Handler) RetryJob(w http.ResponseWriter, r *http.Request) {
	tenant := TenantFromCtx(r.Context())
	jobID := chi.URLParam(r, "id")
	if jobID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "missing job id",
		})
		return
	}

	job, err := h.store.RetryJob(r.Context(), jobID, tenant.ID)
	if errors.Is(err, store.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": "job not found or not retryable",
		})
		return
	}
	if err != nil {
		slog.ErrorContext(r.Context(), "retry job transition",
			"error", err,
			"job_id", jobID,
			"tenant_id", tenant.ID,
		)
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retry job",
		})
		return
	}

	qJob := queue.Job{
		ID:       job.ID,
		TenantID: job.TenantID,
		Payload:  job.Payload,
	}
	if err := h.queue.Enqueue(r.Context(), qJob); err != nil {
		slog.ErrorContext(r.Context(), "retry enqueue",
			"error", err,
			"job_id", job.ID,
			"tenant_id", job.TenantID,
		)
		writeJSON(w, http.StatusAccepted, map[string]any{
			"job_id": job.ID,
			"status": string(store.StatusPending),
			"note":   "retry queued via recovery path",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"job_id": job.ID,
		"status": string(store.StatusPending),
	})
}
