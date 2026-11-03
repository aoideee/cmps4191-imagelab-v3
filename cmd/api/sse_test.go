package main

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aoideee/cmps4191-imagelab-v3/internal/data"
)

func TestWriteJobUpdateEventUsesNamedCompleteFrame(t *testing.T) {
	recorder := httptest.NewRecorder()
	job := &data.Job{
		PublicID:          "job-1",
		Status:            "processing",
		Version:           4,
		Stage:             "preview_ready",
		VariantsCompleted: 2,
	}

	if err := writeJobUpdateEvent(recorder, job); err != nil {
		t.Fatal(err)
	}

	body := recorder.Body.String()
	if !strings.HasPrefix(body, "event: job-update\ndata: {") {
		t.Fatalf("unexpected SSE frame: %q", body)
	}
	if !strings.HasSuffix(body, "\n\n") {
		t.Fatalf("SSE frame must end with a blank line: %q", body)
	}
	for _, field := range []string{
		`"id":"job-1"`,
		`"status":"processing"`,
		`"version":4`,
		`"stage":"preview_ready"`,
		`"variants_completed":2`,
	} {
		if !strings.Contains(body, field) {
			t.Errorf("frame is missing %s", field)
		}
	}
	if strings.Contains(body, "\nid:") {
		t.Fatal("v3 must not emit SSE id fields")
	}
}

func TestWriteSSEHeartbeatUsesCommentFrame(t *testing.T) {
	recorder := httptest.NewRecorder()
	if err := writeSSEHeartbeat(recorder); err != nil {
		t.Fatal(err)
	}
	if got := recorder.Body.String(); got != ": keep-alive\n\n" {
		t.Fatalf("heartbeat = %q", got)
	}
}
