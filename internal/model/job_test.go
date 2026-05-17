package model

import "testing"

func TestIsStatusValid(t *testing.T) {
	tests := []struct {
		status Status
		want   bool
	}{
		{StatusWaiting, true},
		{StatusPending, true},
		{StatusProcessing, true},
		{StatusCompleted, true},
		{StatusFailed, true},
		{"", false},
		{"unknown", false},
		{"PENDING", false},
	}
	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			if got := tt.status.IsStatusValid(); got != tt.want {
				t.Errorf("IsStatusValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSetJobStatus(t *testing.T) {
	t.Run("valid status updates job", func(t *testing.T) {
		j := &Job{}
		if err := j.SetJobStatus(StatusPending); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if j.Status != StatusPending {
			t.Errorf("got status %q, want %q", j.Status, StatusPending)
		}
	})

	t.Run("invalid status returns error", func(t *testing.T) {
		j := &Job{Status: StatusPending}
		if err := j.SetJobStatus("bogus"); err == nil {
			t.Fatal("expected error, got nil")
		}
		if j.Status != StatusPending {
			t.Errorf("status mutated on error: got %q", j.Status)
		}
	})

	t.Run("each valid status can be set", func(t *testing.T) {
		for _, s := range []Status{StatusWaiting, StatusPending, StatusProcessing, StatusCompleted, StatusFailed} {
			j := &Job{}
			if err := j.SetJobStatus(s); err != nil {
				t.Errorf("SetJobStatus(%q) unexpected error: %v", s, err)
			}
			if j.Status != s {
				t.Errorf("got %q, want %q", j.Status, s)
			}
		}
	})
}
