# Assessment 3 evidence and reproduction

This folder contains the measured SSE versus long-poll comparison for ImageLab Version 3.

## Evidence files

- [`runs.csv`](runs.csv): all 18 measured runs.
- [`summary.csv`](summary.csv): median and minimum–maximum values by condition and method.
- [`graphs/`](graphs/): the two required labelled graphs generated from `runs.csv`.
- [`raw/`](raw/): browser console records, condition JSON, and server logs.
- [`report.md`](report.md): predictions, method, results, interpretation, recommendation, and limitations.
- [`presentation.html`](presentation.html): ten-slide keyboard-navigable in-class deck.

## Reproduce the experiment

Use the same database and frontend setup described in the repository README. Keep one observer open and confirm the worker queue is empty before each run. The automated browser runner alternates method order and performs six sequential runs per condition.

Start Chrome with a disposable debugging profile:

```bash
google-chrome --headless=new --no-sandbox --remote-debugging-port=9222 \
  --user-data-dir=/tmp/imagelab-assessment3-chrome about:blank
```

Serve the repository root on `http://127.0.0.1:5500`, then start the API with one condition's flags:

```bash
# A: quiet interval
go run ./cmd/api -db-dsn="$GATEKEEPER_DB_DSN" -job-delay=30s

# B: ordinary real progress
go run ./cmd/api -db-dsn="$GATEKEEPER_DB_DSN"

# C: frequent simulated updates, followed by real processing
go run ./cmd/api -db-dsn="$GATEKEEPER_DB_DSN" \
  -simulation-interval=200ms -simulation-duration=10s
```

For each condition, capture browser results while saving the matching API stdout as `raw/server-condition-A.log`, `B.log`, or `C.log`:

```bash
node scripts/capture-browser-condition.mjs A
node scripts/capture-browser-condition.mjs B
node scripts/capture-browser-condition.mjs C
```

Rebuild both CSV tables and graphs only after all 18 run records and three server logs exist:

```bash
node scripts/build-assessment3-evidence.mjs
```

The build script refuses to generate results unless it finds exactly 18 runs. It derives the graphs and summary from the raw JSON and enriches database-read, heartbeat, and stream-duration fields from the server logs.

## Live demonstration checklist

1. Open DevTools Network and filter for `events`.
2. Submit one image in SSE mode and inspect the pending GET plus named `job-update` frames.
3. Open the same job in a second tab and confirm both tabs receive newer versions.
4. Interrupt the connection and show the same `EventSource` reconnecting to the current snapshot.
5. Select **Stop observing**, then retrieve the same job and show that worker processing continued.
6. Repeat in long-poll mode and compare the sequence of GET responses.
7. Present `runs.csv`, `summary.csv`, and both graphs together.
