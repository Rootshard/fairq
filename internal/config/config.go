package config

import (
	"errors"
	"os"
	"strings"
)

type Config struct {
	Port string

	PostgresDSN string

	RedisAddr     string
	RedisPassword string

	AWSRegion   string
	RabbitMQURL string

	SQSInboundQueueURL string
	RMQInboundQueue    string

	IngestQueueURL string
	NotifyQueueURL string
}

func Load() Config {
	return Config{
		Port:               os.Getenv("PORT"),
		PostgresDSN:        os.Getenv("POSTGRES_DSN"),
		RedisAddr:          os.Getenv("REDIS_ADDR"),
		RedisPassword:      os.Getenv("REDIS_PASSWORD"),
		AWSRegion:          os.Getenv("AWS_REGION"),
		RabbitMQURL:        os.Getenv("RABBITMQ_URL"),
		SQSInboundQueueURL: os.Getenv("SQS_INBOUND_QUEUE_URL"),
		RMQInboundQueue:    os.Getenv("RMQ_INBOUND_QUEUE"),
		IngestQueueURL:     os.Getenv("MODULE_INGEST_QUEUE_URL"),
		NotifyQueueURL:     os.Getenv("MODULE_NOTIFY_QUEUE_URL"),
	}
}

func (c Config) Validate() error {
	var errs []string

	if c.PostgresDSN == "" {
		errs = append(errs, "POSTGRES_DSN is required")
	}
	if c.RedisAddr == "" {
		errs = append(errs, "REDIS_ADDR is required")
	}
	if c.SQSInboundQueueURL == "" && c.RMQInboundQueue == "" {
		errs = append(errs, "at least one of SQS_INBOUND_QUEUE_URL or RMQ_INBOUND_QUEUE must be set")
	}
	if err := ValidateModuleConfigs(BuildModuleConfigs(c)); err != nil {
		errs = append(errs, err.Error())
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "\n"))
	}
	return nil
}

func (c Config) SQSEnabled() bool { return c.SQSInboundQueueURL != "" }
func (c Config) RMQEnabled() bool { return c.RMQInboundQueue != "" }
