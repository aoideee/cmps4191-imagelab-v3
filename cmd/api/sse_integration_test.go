package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aoideee/cmps4191-imagelab-v3/internal/data"
)

func TestSSEBroadcastsSnapshotsToEveryObserver(t *testing.T) {
	if os.Getenv("IMAGELAB_INTEGRATION_TESTS") != "1" {
		t.Skip("set IMAGELAB_INTEGRATION_TESTS=1 to run PostgreSQL tests")
	}

	db := startUploadTestDatabase(t)
	models := data.NewModels(db)
	app := &application{
		models:    models,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		observers: newObservationHub(),
	}

	image := &data.Image{
		OriginalFilename: "sse.png",
		StoredFilename:   "sse-source.png",
		MediaType:        "image/png",
		Size:             1,
	}
	if err := models.Images.Insert(image); err != nil {
		t.Fatal(err)
	}
	job := &data.Job{ImageID: &image.ID, JobType: "image_processing"}
	if err := models.Jobs.Insert(job); err != nil {
		t.Fatal(err)
	}

	results := make(chan *httptest.ResponseRecorder, 2)
	for range 2 {
		go func() {
			request := httptest.NewRequest(
				http.MethodGet,
				"/v1/jobs/"+job.PublicID+"/events",
				nil,
			)
			recorder := httptest.NewRecorder()
			app.routes().ServeHTTP(recorder, request)
			results <- recorder
		}()
	}

	waitForObservers(t, app.observers, job.PublicID, 2)
	claimed, err := models.Jobs.ClaimNext(context.Background(), "image_processing")
	if err != nil {
		t.Fatal(err)
	}
	app.notifyJobObservers(job.PublicID)

	if err := models.Jobs.MarkFailed(context.Background(), claimed.ID, "test failure"); err != nil {
		t.Fatal(err)
	}
	app.notifyJobObservers(job.PublicID)

	for range 2 {
		select {
		case recorder := <-results:
			if got := recorder.Header().Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
				t.Fatalf("Content-Type = %q", got)
			}
			body := recorder.Body.String()
			if strings.Count(body, "event: job-update\n") < 2 {
				t.Fatalf("expected initial and terminal events; got %q", body)
			}
			if !strings.Contains(body, `"status":"queued"`) ||
				!strings.Contains(body, `"status":"failed"`) {
				t.Fatalf("stream is missing queued or failed snapshot: %q", body)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("SSE observer did not receive terminal snapshot")
		}
	}

	if got := app.activeSSE.Load(); got != 0 {
		t.Fatalf("active SSE handlers = %d; want 0", got)
	}
}

func TestSSEUnknownJobReturns404BeforeStream(t *testing.T) {
	if os.Getenv("IMAGELAB_INTEGRATION_TESTS") != "1" {
		t.Skip("set IMAGELAB_INTEGRATION_TESTS=1 to run PostgreSQL tests")
	}

	db := startUploadTestDatabase(t)
	app := &application{
		models:    data.NewModels(db),
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		observers: newObservationHub(),
	}
	request := httptest.NewRequest(http.MethodGet, "/v1/jobs/missing/events", nil)
	recorder := httptest.NewRecorder()
	app.routes().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d; want 404", recorder.Code)
	}
	if strings.HasPrefix(recorder.Header().Get("Content-Type"), "text/event-stream") {
		t.Fatal("unknown jobs must not begin an SSE stream")
	}
}
