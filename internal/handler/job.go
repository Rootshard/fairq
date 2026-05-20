package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/Rootshard/fairq/internal/db"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type JobHandler struct {
	pool *pgxpool.Pool
}

func NewJobHandler(pool *pgxpool.Pool) *JobHandler {
	return &JobHandler{pool: pool}
}

func (h *JobHandler) GetJob(w http.ResponseWriter, r *http.Request) {
	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		http.Error(w, "missing X-Tenant-ID header", http.StatusUnauthorized)
		return
	}

	jobID := chi.URLParam(r, "jobID")

	job, err := db.GetJob(r.Context(), h.pool, jobID)
	if err != nil {
		if errors.Is(err, db.ErrJobNotFound) {
			slog.Warn("handler: job not found", "job_id", jobID, "tenant_id", tenantID)
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}
		slog.Error("handler: get job failed", "job_id", jobID, "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if job.TenantID != tenantID {
		slog.Warn("handler: tenant mismatch", "job_id", jobID, "tenant_id", tenantID)
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	slog.Info("handler: job fetched", "job_id", jobID, "tenant_id", tenantID, "status", job.Status)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}
