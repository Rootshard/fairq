package config

import (
	"strings"
	"testing"
)

var validBase = Config{
	PostgresDSN:        "postgres://localhost/test",
	RedisAddr:          "localhost:6379",
	SQSInboundQueueURL: "https://sqs.us-east-1.amazonaws.com/123/queue",
	IngestQueueURL:     "https://sqs.us-east-1.amazonaws.com/123/ingest",
	NotifyQueueURL:     "notify-queue",
}

func TestValidate(t *testing.T) {
	t.Run("valid config passes", func(t *testing.T) {
		if err := validBase.Validate(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("missing PostgresDSN", func(t *testing.T) {
		c := validBase
		c.PostgresDSN = ""
		err := c.Validate()
		if err == nil || !strings.Contains(err.Error(), "POSTGRES_DSN") {
			t.Errorf("expected POSTGRES_DSN error, got %v", err)
		}
	})

	t.Run("missing RedisAddr", func(t *testing.T) {
		c := validBase
		c.RedisAddr = ""
		err := c.Validate()
		if err == nil || !strings.Contains(err.Error(), "REDIS_ADDR") {
			t.Errorf("expected REDIS_ADDR error, got %v", err)
		}
	})

	t.Run("no inbound queue set", func(t *testing.T) {
		c := validBase
		c.SQSInboundQueueURL = ""
		c.RMQInboundQueue = ""
		err := c.Validate()
		if err == nil || !strings.Contains(err.Error(), "SQS_INBOUND_QUEUE_URL") {
			t.Errorf("expected inbound queue error, got %v", err)
		}
	})

	t.Run("rmq-only inbound is valid", func(t *testing.T) {
		c := validBase
		c.SQSInboundQueueURL = ""
		c.RMQInboundQueue = "jobs"
		if err := c.Validate(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("multiple errors reported together", func(t *testing.T) {
		c := Config{}
		err := c.Validate()
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		msg := err.Error()
		if !strings.Contains(msg, "POSTGRES_DSN") || !strings.Contains(msg, "REDIS_ADDR") {
			t.Errorf("expected multiple errors, got: %v", err)
		}
	})
}

func TestSQSEnabled(t *testing.T) {
	if !(Config{SQSInboundQueueURL: "url"}).SQSEnabled() {
		t.Error("expected true when SQSInboundQueueURL set")
	}
	if (Config{}).SQSEnabled() {
		t.Error("expected false when SQSInboundQueueURL empty")
	}
}

func TestRMQEnabled(t *testing.T) {
	if !(Config{RMQInboundQueue: "queue"}).RMQEnabled() {
		t.Error("expected true when RMQInboundQueue set")
	}
	if (Config{}).RMQEnabled() {
		t.Error("expected false when RMQInboundQueue empty")
	}
}
