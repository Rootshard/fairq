package config

import (
	"errors"
	"fmt"
	"strings"
)

type QueueType string

const (
	QueueTypeSQS QueueType = "sqs"
	QueueTypeRMQ QueueType = "rmq"
)

type ModuleConfig struct {
	QueueURL  string
	QueueType QueueType
}

type ModuleConfigs map[string]ModuleConfig

func BuildModuleConfigs(c Config) ModuleConfigs {
	return ModuleConfigs{
		"ingest": {QueueURL: c.IngestQueueURL, QueueType: QueueTypeSQS},
		"notify": {QueueURL: c.NotifyQueueURL, QueueType: QueueTypeRMQ},
	}
}

func ValidateModuleConfigs(mc ModuleConfigs) error {
	var errs []string
	for module, cfg := range mc {
		if cfg.QueueURL == "" {
			errs = append(errs, fmt.Sprintf("module %q: queue URL is empty", module))
		}
		switch cfg.QueueType {
		case QueueTypeSQS, QueueTypeRMQ:
		default:
			errs = append(errs, fmt.Sprintf("module %q: invalid queue type %q (must be sqs or rmq)", module, cfg.QueueType))
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "\n"))
	}
	return nil
}
