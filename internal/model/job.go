package model

import (
	"fmt"
	"time"
)

type Status string

const (
	StatusWaiting    Status = "waiting"
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

type Job struct {
	JobID          string         `json:"job_id"`
	Status      Status         `json:"status"`
	TenantID    string         `json:"tenant_id"`
	Module      string         `json:"module"`
	Pattern     string         `json:"pattern"`
	QueuedAt    time.Time      `json:"queued_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	AllocatedAt time.Time      `json:"allocated_at"`
	Meta        map[string]any `json:"meta,omitempty"`
}

func (s Status) IsStatusValid() bool {
	switch s {
	case StatusWaiting, StatusPending,
		StatusProcessing, StatusCompleted, StatusFailed:
		return true
	}
	return false
}

func (j *Job) SetJobStatus(s Status) error {
	if !s.IsStatusValid() {
		return fmt.Errorf("Invalid job status %s", s)
	}
	j.Status = s
	return nil
}

type Event struct {
	JobID string `json:"job_id"`
}
