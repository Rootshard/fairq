package publisher

import (
	"context"
	"fmt"

	"github.com/Rootshard/fairq/internal/config"
	"github.com/Rootshard/fairq/internal/model"
)

type Router struct {
	modules config.ModuleConfigs
	sqs     *SQSPublisher
	rmq     *RMQPublisher
}

func NewRouter(modules config.ModuleConfigs, sqs *SQSPublisher, rmq *RMQPublisher) *Router {
	return &Router{modules: modules, sqs: sqs, rmq: rmq}
}

func (r *Router) Publish(ctx context.Context, module string, event *model.Event) error {
	cfg, ok := r.modules[module]
	if !ok {
		return fmt.Errorf("unknown module %q", module)
	}
	switch cfg.QueueType {
	case config.QueueTypeSQS:
		if r.sqs == nil {
			return fmt.Errorf("module %q requires SQS but no SQS publisher is configured", module)
		}
		return r.sqs.Publish(ctx, cfg.QueueURL, event)
	case config.QueueTypeRMQ:
		if r.rmq == nil {
			return fmt.Errorf("module %q requires RMQ but no RMQ publisher is configured", module)
		}
		return r.rmq.Publish(ctx, cfg.QueueURL, event)
	default:
		return fmt.Errorf("unsupported queue type %q for module %q", cfg.QueueType, module)
	}
}
