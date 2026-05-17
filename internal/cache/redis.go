package cache

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func Connect(addr, password string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
	})
}

func Ping(ctx context.Context, client *redis.Client) error {
	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	return nil
}

func IncrPendingJobs(ctx context.Context, client *redis.Client, tenantID string) error {
	if err := client.Incr(ctx, pendingKey(tenantID)).Err(); err != nil {
		return fmt.Errorf("incr pending jobs: %w", err)
	}
	return nil
}

func DecrPendingJobs(ctx context.Context, client *redis.Client, tenantID string) error {
	if err := client.Decr(ctx, pendingKey(tenantID)).Err(); err != nil {
		return fmt.Errorf("decr pending jobs: %w", err)
	}
	return nil
}

func GetPendingJobsCount(ctx context.Context, client *redis.Client, tenantID string) (int64, error) {
	count, err := client.Get(ctx, pendingKey(tenantID)).Int64()
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		return 0, fmt.Errorf("get pending jobs count: %w", err)
	}
	return count, nil
}

func pendingKey(tenantID string) string {
	return tenantID + ":pending_jobs_count"
}
