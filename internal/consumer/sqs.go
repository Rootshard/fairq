package consumer

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/Rootshard/fairq/internal/model"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type ProcessFunc func(ctx context.Context, event *model.Event) error

type SQSConsumer struct {
	client   *sqs.Client
	queueURL string
}

func NewSQSConsumer(ctx context.Context, region, queueURL string) (*SQSConsumer, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}
	return &SQSConsumer{
		client:   sqs.NewFromConfig(cfg),
		queueURL: queueURL,
	}, nil
}

func (c *SQSConsumer) Run(ctx context.Context, process ProcessFunc) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		out, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.queueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20,
		})
		if err != nil {
			log.Printf("sqs receive error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		for _, msg := range out.Messages {
			var event model.Event
			if err := json.Unmarshal([]byte(aws.ToString(msg.Body)), &event); err != nil {
				log.Printf("sqs: bad message body: %v — deleting", err)
				c.delete(ctx, msg.ReceiptHandle)
				continue
			}

			if err := process(ctx, &event); err != nil {
				log.Printf("sqs: process error for job %s: %v", event.JobID, err)
			}
			c.delete(ctx, msg.ReceiptHandle)
		}
	}
}

func (c *SQSConsumer) delete(ctx context.Context, receiptHandle *string) {
	if _, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: receiptHandle,
	}); err != nil {
		log.Printf("sqs: delete failed: %v", err)
	}
}
