package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/Rootshard/fairq/internal/model"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RMQConsumer struct {
	url       string
	queueName string
}

func NewRMQConsumer(url, queueName string) *RMQConsumer {
	return &RMQConsumer{url: url, queueName: queueName}
}

func (c *RMQConsumer) Run(ctx context.Context, process ProcessFunc) {
	for {
		if err := c.consume(ctx, process); err != nil {
			slog.Error("rmq consumer error — reconnecting in 5s", "queue", c.queueName, "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

func (c *RMQConsumer) consume(ctx context.Context, process ProcessFunc) error {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(c.queueName, true, false, false, false, nil)
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	slog.Info("rmq consumer connected", "queue", c.queueName)

	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-msgs:
			if !ok {
				return nil
			}
			var event model.Event
			if err := json.Unmarshal(d.Body, &event); err != nil {
				slog.Warn("rmq bad message body — acking", "queue", c.queueName, "error", err)
				d.Ack(false)
				continue
			}
			slog.Info("rmq message received", "job_id", event.JobID, "queue", c.queueName)
			if err := process(ctx, &event); err != nil {
				slog.Error("rmq process error", "job_id", event.JobID, "error", err)
			} else {
				slog.Info("rmq message processed", "job_id", event.JobID)
			}
			d.Ack(false)
		}
	}
}
