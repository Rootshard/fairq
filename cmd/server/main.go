package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/Rootshard/fairq/internal/cache"
	"github.com/Rootshard/fairq/internal/config"
	"github.com/Rootshard/fairq/internal/consumer"
	"github.com/Rootshard/fairq/internal/db"
	"github.com/Rootshard/fairq/internal/handler"
	"github.com/Rootshard/fairq/internal/model"
	"github.com/Rootshard/fairq/internal/publisher"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func main() {
	cfg := config.Load()
	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid config:\n%v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()

	redisClient := cache.Connect(cfg.RedisAddr, cfg.RedisPassword)
	if err := cache.Ping(ctx, redisClient); err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer redisClient.Close()

	modules := config.BuildModuleConfigs(cfg)

	var sqsPub *publisher.SQSPublisher
	if modules.HasSQS() {
		sqsPub, err = publisher.NewSQSPublisher(ctx, cfg.AWSRegion)
		if err != nil {
			log.Fatalf("sqs publisher: %v", err)
		}
	}

	var rmqPub *publisher.RMQPublisher
	if modules.HasRMQ() {
		rmqPub = publisher.NewRMQPublisher(cfg.RabbitMQURL)
	}

	router := publisher.NewRouter(modules, sqsPub, rmqPub)

	process := func(ctx context.Context, event *model.Event) error {
		job, err := db.GetJob(ctx, pool, event.JobID)
		if err != nil {
			if errors.Is(err, db.ErrJobNotFound) {
				slog.Warn("process: job not found — skipping", "job_id", event.JobID)
				return nil
			}
			return err
		}

		slog.Info("process: job received", "job_id", job.JobID, "tenant_id", job.TenantID, "module", job.Module, "status", job.Status)

		if err := db.UpdateJobStatus(ctx, pool, job.JobID, model.StatusProcessing); err != nil {
			return err
		}
		slog.Info("process: job status updated", "job_id", job.JobID, "status", model.StatusProcessing)

		if err := cache.DecrPendingJobs(ctx, redisClient, job.TenantID); err != nil {
			slog.Warn("process: redis decr failed", "tenant_id", job.TenantID, "error", err)
		}

		return router.Publish(ctx, job.Module, event)
	}

	if cfg.SQSEnabled() {
		sqsCons, err := consumer.NewSQSConsumer(ctx, cfg.AWSRegion, cfg.SQSInboundQueueURL)
		if err != nil {
			log.Fatalf("sqs consumer: %v", err)
		}
		go sqsCons.Run(ctx, process)
	}

	if cfg.RMQEnabled() {
		go consumer.NewRMQConsumer(cfg.RabbitMQURL, cfg.RMQInboundQueue).Run(ctx, process)
	}

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	jobHandler := handler.NewJobHandler(pool)
	r.Get("/jobs/{jobID}", jobHandler.GetJob)

	srv := &http.Server{Addr: ":" + cfg.Port, Handler: r}
	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()

	log.Printf("server listening on :%s", cfg.Port)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server: %v", err)
	}
}
