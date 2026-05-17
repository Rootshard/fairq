package config

import (
	"strings"
	"testing"
)

func TestBuildModuleConfigs(t *testing.T) {
	c := Config{
		IngestQueueURL: "https://sqs.us-east-1.amazonaws.com/123/ingest",
		NotifyQueueURL: "notify-queue",
	}
	mc := BuildModuleConfigs(c)

	ingest, ok := mc["ingest"]
	if !ok {
		t.Fatal("missing ingest module")
	}
	if ingest.QueueURL != c.IngestQueueURL {
		t.Errorf("ingest QueueURL = %q, want %q", ingest.QueueURL, c.IngestQueueURL)
	}
	if ingest.QueueType != QueueTypeSQS {
		t.Errorf("ingest QueueType = %q, want %q", ingest.QueueType, QueueTypeSQS)
	}

	notify, ok := mc["notify"]
	if !ok {
		t.Fatal("missing notify module")
	}
	if notify.QueueURL != c.NotifyQueueURL {
		t.Errorf("notify QueueURL = %q, want %q", notify.QueueURL, c.NotifyQueueURL)
	}
	if notify.QueueType != QueueTypeRMQ {
		t.Errorf("notify QueueType = %q, want %q", notify.QueueType, QueueTypeRMQ)
	}
}

func TestValidateModuleConfigs(t *testing.T) {
	t.Run("valid configs pass", func(t *testing.T) {
		mc := ModuleConfigs{
			"ingest": {QueueURL: "https://sqs/queue", QueueType: QueueTypeSQS},
			"notify": {QueueURL: "notify-queue", QueueType: QueueTypeRMQ},
		}
		if err := ValidateModuleConfigs(mc); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("empty QueueURL returns error", func(t *testing.T) {
		mc := ModuleConfigs{
			"ingest": {QueueURL: "", QueueType: QueueTypeSQS},
		}
		err := ValidateModuleConfigs(mc)
		if err == nil || !strings.Contains(err.Error(), "queue URL is empty") {
			t.Errorf("expected queue URL error, got %v", err)
		}
	})

	t.Run("invalid QueueType returns error", func(t *testing.T) {
		mc := ModuleConfigs{
			"ingest": {QueueURL: "some-url", QueueType: "kafka"},
		}
		err := ValidateModuleConfigs(mc)
		if err == nil || !strings.Contains(err.Error(), "invalid queue type") {
			t.Errorf("expected queue type error, got %v", err)
		}
	})

	t.Run("multiple invalid modules all reported", func(t *testing.T) {
		mc := ModuleConfigs{
			"a": {QueueURL: "", QueueType: QueueTypeSQS},
			"b": {QueueURL: "", QueueType: QueueTypeSQS},
		}
		if err := ValidateModuleConfigs(mc); err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("empty map passes", func(t *testing.T) {
		if err := ValidateModuleConfigs(ModuleConfigs{}); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}
