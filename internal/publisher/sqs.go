package publisher

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Rootshard/fairq/internal/model"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type SQSPublisher struct {
	client *sqs.Client
}

func NewSQSPublisher(ctx context.Context, region string) (*SQSPublisher, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("sqs publisher init: %w", err)
	}
	return &SQSPublisher{client: sqs.NewFromConfig(cfg)}, nil
}

func (p *SQSPublisher) Publish(ctx context.Context, queueURL string, event *model.Event) error {
	b, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	_, err = p.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(b)),
	})
	if err != nil {
		return fmt.Errorf("sqs send: %w", err)
	}
	return nil
}
