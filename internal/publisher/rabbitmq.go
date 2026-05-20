package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/Rootshard/fairq/internal/model"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RMQPublisher struct {
	url  string
	conn *amqp.Connection
	ch   *amqp.Channel
	mu   sync.Mutex
}

func NewRMQPublisher(url string) *RMQPublisher {
	return &RMQPublisher{url: url}
}

func (p *RMQPublisher) Publish(_ context.Context, queueName string, event *model.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.ensureConnected(); err != nil {
		return fmt.Errorf("rmq connect: %w", err)
	}

	b, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	if _, err := p.ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		return fmt.Errorf("rmq queue declare: %w", err)
	}

	slog.Info("rmq publishing", "job_id", event.JobID, "queue", queueName)
	err = p.ch.Publish("", queueName, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        b,
	})
	if err != nil {
		p.reset()
		return fmt.Errorf("rmq publish: %w", err)
	}
	slog.Info("rmq published", "job_id", event.JobID, "queue", queueName)
	return nil
}

func (p *RMQPublisher) ensureConnected() error {
	if p.conn != nil && !p.conn.IsClosed() {
		return nil
	}
	conn, err := amqp.Dial(p.url)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return err
	}
	p.conn = conn
	p.ch = ch
	return nil
}

func (p *RMQPublisher) reset() {
	if p.ch != nil {
		p.ch.Close()
		p.ch = nil
	}
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
}
