package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Rootshard/fairq/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrJobNotFound = errors.New("job not found")

func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	return pool, nil
}

func UpdateJobStatus(ctx context.Context, pool *pgxpool.Pool, jobID string, status model.Status) error {
	query := `UPDATE jobs SET status = $1, updated_at = NOW()`

	if status == model.StatusProcessing {
		query += `, allocated_at = NOW()`
	}

	query += ` WHERE job_id = $2`

	tag, err := pool.Exec(ctx, query, string(status), jobID)
	if err != nil {
		return fmt.Errorf("update job status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrJobNotFound
	}
	return nil
}

func GetJob(ctx context.Context, pool *pgxpool.Pool, jobID string) (*model.Job, error) {
	row := pool.QueryRow(ctx, `
		SELECT job_id, status, tenant_id, module, pattern, queued_at, updated_at, allocated_at, meta
		FROM jobs WHERE job_id = $1
	`, jobID)

	var j model.Job
	var metaBytes []byte
	err := row.Scan(
		&j.JobID, &j.Status, &j.TenantID, &j.Module, &j.Pattern,
		&j.QueuedAt, &j.UpdatedAt, &j.AllocatedAt, &metaBytes,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrJobNotFound
		}
		return nil, fmt.Errorf("get job: %w", err)
	}
	if len(metaBytes) > 0 {
		if err := json.Unmarshal(metaBytes, &j.Meta); err != nil {
			return nil, fmt.Errorf("unmarshal meta: %w", err)
		}
	}
	return &j, nil
}
