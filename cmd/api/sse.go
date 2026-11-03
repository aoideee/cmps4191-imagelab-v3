package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aoideee/cmps4191-imagelab-v3/internal/data"
)

const sseHeartbeatInterval = 15 * time.Second

func (app *application) getJobEventsHandler(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	startedAt := time.Now()
	databaseReads := 0
	eventsSent := 0
	heartbeatsSent := 0

	// Subscribe before the authoritative read. A commit that lands between
	// these operations closes the captured channel, so it cannot be missed.
	subscription := app.observers.subscribe(jobID)
	job, err := app.models.Jobs.GetByPublicID(jobID)
	databaseReads++
	if err != nil {
		subscription.release()
		app.writeJobLookupError(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	activeStreams := app.activeSSE.Add(1)
	for {
		peak := app.peakSSE.Load()
		if activeStreams <= peak || app.peakSSE.CompareAndSwap(peak, activeStreams) {
			break
		}
	}

	lastVersion := int64(0)
	heartbeat := time.NewTicker(sseHeartbeatInterval)
	defer heartbeat.Stop()
	defer func() {
		subscription.release()
		app.activeSSE.Add(-1)
		app.logger.Info(
			"SSE stream ended",
			"job_id", jobID,
			"database_reads", databaseReads,
			"job_events", eventsSent,
			"heartbeats", heartbeatsSent,
			"stream_ms", time.Since(startedAt).Milliseconds(),
			"peak_active_streams", app.peakSSE.Load(),
		)
	}()

	for {
		if job.Version > lastVersion {
			if err := writeJobUpdateEvent(w, job); err != nil {
				return
			}
			lastVersion = job.Version
			eventsSent++
		}

		if isTerminalJob(job) {
			return
		}

		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			if err := writeSSEHeartbeat(w); err != nil {
				return
			}
			heartbeatsSent++
		case <-subscription.C:
			// Register the next signal before reading PostgreSQL. Notifications
			// are wake-up hints only; every payload comes from current DB state.
			subscription.release()
			subscription = app.observers.subscribe(jobID)
			job, err = app.models.Jobs.GetByPublicID(jobID)
			databaseReads++
			if err != nil {
				app.logger.Error("SSE snapshot read failed", "job_id", jobID, "error", err)
				return
			}
		}
	}
}

func writeJobUpdateEvent(w http.ResponseWriter, job *data.Job) error {
	payload, err := json.Marshal(jobSnapshot(job))
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "event: job-update\ndata: %s\n\n", payload); err != nil {
		return err
	}
	return http.NewResponseController(w).Flush()
}

func writeSSEHeartbeat(w http.ResponseWriter) error {
	if _, err := io.WriteString(w, ": keep-alive\n\n"); err != nil {
		return err
	}
	return http.NewResponseController(w).Flush()
}

func isTerminalJob(job *data.Job) bool {
	return job.Status == "completed" || job.Status == "failed"
}
