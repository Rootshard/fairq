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
	all := map[string]ModuleConfig{
		"ingest": {QueueURL: c.IngestQueueURL, QueueType: QueueTypeSQS},
		"notify": {QueueURL: c.NotifyQueueURL, QueueType: QueueTypeRMQ},
	}
	mc := make(ModuleConfigs)
	for name, cfg := range all {
		if cfg.QueueURL != "" {
			mc[name] = cfg
		}
	}
	return mc
}

func (mc ModuleConfigs) HasSQS() bool {
	for _, cfg := range mc {
		if cfg.QueueType == QueueTypeSQS {
			return true
		}
	}
	return false
}

func (mc ModuleConfigs) HasRMQ() bool {
	for _, cfg := range mc {
		if cfg.QueueType == QueueTypeRMQ {
			return true
		}
	}
	return false
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
