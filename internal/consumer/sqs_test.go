package consumer

import (
	"context"
	"errors"
	"testing"

	"github.com/Rootshard/fairq/internal/model"
)

func TestProcessFunc(t *testing.T) {
	t.Run("receives correct event", func(t *testing.T) {
		var received *model.Event
		fn := ProcessFunc(func(_ context.Context, event *model.Event) error {
			received = event
			return nil
		})
		if err := fn(context.Background(), &model.Event{JobID: "job-123"}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if received == nil || received.JobID != "job-123" {
			t.Errorf("got event %v, want JobID job-123", received)
		}
	})

	t.Run("error propagates to caller", func(t *testing.T) {
		want := errors.New("process failed")
		fn := ProcessFunc(func(_ context.Context, _ *model.Event) error {
			return want
		})
		if err := fn(context.Background(), &model.Event{}); !errors.Is(err, want) {
			t.Errorf("expected %v, got %v", want, err)
		}
	})
}
