import { getState, setState } from "./state.js";
import {
  DataService,
  eventStreamUrl,
  validateJobSnapshot,
} from "./modules/data-service.js";
import { buildMeasurement } from "./modules/measurements.js";

const fileInput = document.getElementById("file-input");
const chooseImageBtn = document.getElementById("choose-image-btn");
const processBtn = document.getElementById("process-btn");
const tryAgainBtn = document.getElementById("try-again-btn");
const stopObservingBtn = document.getElementById("stop-observing-btn");
const resumeObservingBtn = document.getElementById("resume-observing-btn");
const modeInputs = document.querySelectorAll('input[name="observation-mode"]');

let selectionVersion = 0;
let observationGeneration = 0;

window.addEventListener("beforeunload", () => stopObservation(false));

for (const input of modeInputs) {
  input.addEventListener("change", () => {
    if (!input.checked) return;
    setState({ observationMode: input.value });
    const { activeJob } = getState();
    if (activeJob?.status_url && !isTerminal(activeJob.status)) {
      startObservation(activeJob.status_url);
    }
  });
}

fileInput.addEventListener("change", async (event) => {
  if (getState().isSubmitting) return;
  const file = event.target.files[0];
  if (!file) return;

  const thisSelection = ++selectionVersion;
  const maxImageSize = 10 * 1024 * 1024;
  const allowedTypes = ["image/jpeg", "image/png"];
  let error = null;
  if (file.size === 0) error = "This file is empty. Choose a JPEG or PNG image.";
  else if (!allowedTypes.includes(file.type)) error = "Choose a JPEG or PNG image.";
  else if (file.size > maxImageSize) {
    error = "This image exceeds the 10 MB limit. Choose a smaller image.";
  }

  if (error) {
    fileInput.value = "";
    setState({ selectedFile: null, isValidating: false, uploadError: error });
    return;
  }

  setState({ selectedFile: null, isValidating: true, uploadError: null });
  let checkUrl = null;
  try {
    checkUrl = URL.createObjectURL(file);
    const imageCheck = new Image();
    imageCheck.src = checkUrl;
    await imageCheck.decode();
  } catch {
    if (thisSelection !== selectionVersion) return;
    fileInput.value = "";
    setState({
      selectedFile: null,
      isValidating: false,
      uploadError:
        "This image could not be read. It may be damaged. Choose another JPEG or PNG.",
    });
    return;
  } finally {
    if (checkUrl !== null) URL.revokeObjectURL(checkUrl);
  }

  if (thisSelection !== selectionVersion) return;
  stopObservation(false);
  setState({
    selectedFile: file,
    isValidating: false,
    uploadError: null,
    activeJob: null,
    variants: [],
    lastObservedVersion: null,
    observationError: false,
    metrics: freshMetrics(),
  });
});

processBtn.addEventListener("click", async () => {
  const { isSubmitting, isValidating, selectedFile, activeJob } = getState();
  if (
    isSubmitting ||
    isValidating ||
    !selectedFile ||
    (activeJob && !isTerminal(activeJob.status))
  ) {
    return;
  }

  stopObservation(false);
  const requestStart = performance.now();
  setState({ isSubmitting: true, uploadError: null, observationError: false });
  try {
    const accepted = await DataService.uploadImage(selectedFile);
    setState({
      activeJob: accepted,
      lastObservedVersion: null,
      metrics: {
        ...freshMetrics(),
        requestStart,
        ackLatency: performance.now() - requestStart,
      },
    });
    startObservation(accepted.status_url);
  } catch (error) {
    setState({ uploadError: error.message || "Submission error" });
  } finally {
    setState({ isSubmitting: false });
  }
});

chooseImageBtn.addEventListener("click", () => {
  if (getState().isSubmitting) return;
  fileInput.value = "";
  fileInput.click();
});

tryAgainBtn.addEventListener("click", resumeObservation);
resumeObservingBtn.addEventListener("click", resumeObservation);
stopObservingBtn.addEventListener("click", () => stopObservation(true));

function resumeObservation() {
  const { activeJob } = getState();
  if (activeJob?.status_url && !isTerminal(activeJob.status)) {
    startObservation(activeJob.status_url);
  }
}

function freshMetrics() {
  return {
    requestStart: null,
    ackLatency: null,
    pollingCount: 0,
    observationGets: 0,
    sseStreamGets: 0,
    jobEventsReceived: 0,
    versionsReceived: [],
    versionTimeline: [],
    completionObservedAt: null,
  };
}

function isTerminal(status) {
  return status === "completed" || status === "failed";
}

function startObservation(statusUrl) {
  stopObservation(false);
  const generation = observationGeneration;
  setState({
    observationError: false,
    observationState: "connecting",
    isObserving: true,
  });

  if (getState().observationMode === "sse") {
    startEventStream(statusUrl, generation);
    return;
  }

  const controller = new AbortController();
  setState({ abortController: controller, pollingTimer: null });
  void runPollingObservation(statusUrl, controller, generation);
}

function stopObservation(explicit) {
  observationGeneration++;
  const { pollingTimer, abortController, eventSource, activeJob } = getState();
  if (pollingTimer !== null) clearTimeout(pollingTimer);
  abortController?.abort();
  eventSource?.close();
  setState({
    pollingTimer: null,
    abortController: null,
    eventSource: null,
    isObserving: false,
    observationState: explicit && activeJob ? "stopped" : "idle",
  });
  if (explicit && activeJob) {
    logObservation("observation_stopped", {
      job_id: activeJob.job_id ?? activeJob.id,
      job_status: activeJob.status,
    });
  }
}

function startEventStream(statusUrl, generation) {
  const source = new EventSource(eventStreamUrl(statusUrl));
  setState({ eventSource: source });

  const isCurrent = () =>
    generation === observationGeneration && getState().eventSource === source;

  source.onopen = () => {
    if (!isCurrent()) return;
    const metrics = getState().metrics;
    setState({
      observationState: "connected",
      metrics: {
        ...metrics,
        observationGets: metrics.observationGets + 1,
        sseStreamGets: metrics.sseStreamGets + 1,
      },
    });
    logObservation("sse_stream_opened", {
      mode: "sse",
      stream_number: getState().metrics.sseStreamGets,
      opened_at: Date.now(),
      url: eventStreamUrl(statusUrl),
    });
  };

  source.addEventListener("job-update", (event) => {
    if (!isCurrent()) return;
    const receivedAt = Date.now();
    try {
      const snapshot = validateJobSnapshot(JSON.parse(event.data));
      const metrics = getState().metrics;
      setState({
        metrics: {
          ...metrics,
          jobEventsReceived: metrics.jobEventsReceived + 1,
        },
      });
      acceptSnapshot(snapshot, statusUrl, receivedAt, {
        event: "sse_job_update",
        stream_number: getState().metrics.sseStreamGets,
      });
    } catch (error) {
      logObservation("sse_event_error", { error: error.message });
      stopObservation(false);
      setState({ observationError: true, observationState: "error" });
    }
  });

  source.onerror = () => {
    if (!isCurrent()) return;
    setState({ observationState: "reconnecting" });
    logObservation("sse_stream_interrupted", {
      mode: "sse",
      occurred_at: Date.now(),
      ready_state: source.readyState,
    });
    // Keep this EventSource object. The browser opens the reconnection GET.
  };
}

async function runPollingObservation(statusUrl, controller, generation) {
  const isCurrent = () =>
    generation === observationGeneration &&
    getState().abortController === controller &&
    !controller.signal.aborted;
  let initialSnapshot = true;
  let inFlight = null;

  try {
    while (isCurrent()) {
      const mode = getState().observationMode;
      if (!initialSnapshot && mode === "short") {
        await delay(1000, controller.signal, (timer) => {
          if (isCurrent()) setState({ pollingTimer: timer });
        });
        if (!isCurrent()) return;
        setState({ pollingTimer: null });
      }

      let requestUrl = statusUrl;
      if (!initialSnapshot && mode === "long") {
        requestUrl = `${statusUrl}?after=${encodeURIComponent(getState().lastObservedVersion)}&wait=20`;
      }

      const metrics = getState().metrics;
      setState({
        observationState: "connected",
        metrics: {
          ...metrics,
          pollingCount: metrics.pollingCount + 1,
          observationGets: metrics.observationGets + 1,
        },
      });
      inFlight = {
        request_number: getState().metrics.pollingCount,
        mode,
        request_started_at: Date.now(),
        url: requestUrl,
      };
      logObservation("request_started", inFlight);

      const snapshot = await DataService.fetchJobStatus(
        requestUrl,
        controller.signal,
        30000,
      );
      const receivedAt = Date.now();
      if (!isCurrent()) return;

      if (snapshot === null) {
        logObservation("response_received", {
          ...inFlight,
          response_received_at: receivedAt,
          http_status: 204,
          received_version: null,
        });
        inFlight = null;
        initialSnapshot = false;
        continue;
      }

      acceptSnapshot(snapshot, statusUrl, receivedAt, {
        event: "response_received",
        ...inFlight,
        http_status: 200,
      });
      inFlight = null;
      initialSnapshot = false;
    }
  } catch (error) {
    if (inFlight !== null) {
      logObservation(
        error.name === "AbortError" ? "request_cancelled" : "request_error",
        { ...inFlight, response_received_at: Date.now(), error: error.message },
      );
    }
    if (!isCurrent() || error.name === "AbortError") return;
    stopObservation(false);
    setState({ observationError: true, observationState: "error" });
  }
}

function acceptSnapshot(snapshot, statusUrl, receivedAt, logDetails) {
  const state = getState();
  const expectedJobId = state.activeJob?.job_id ?? state.activeJob?.id;
  if (snapshot.id !== expectedJobId) {
    throw new Error("Status response belongs to a different job");
  }
  if (
    state.lastObservedVersion !== null &&
    snapshot.version < state.lastObservedVersion
  ) {
    throw new Error("Status response moved to an older version");
  }

  const versions = new Set(state.metrics.versionsReceived);
  versions.add(snapshot.version);
  const timeline = [...state.metrics.versionTimeline];
  if (timeline.length === 0 || timeline[timeline.length - 1].version !== snapshot.version) {
    timeline.push({ version: snapshot.version, epoch_ms: receivedAt });
  }
  const mergedJob = { ...snapshot, status_url: statusUrl };
  const metrics = {
    ...state.metrics,
    versionsReceived: [...versions].sort((a, b) => a - b),
    versionTimeline: timeline,
    completionObservedAt:
      snapshot.status === "completed"
        ? receivedAt
        : state.metrics.completionObservedAt,
  };
  const renderStartedAt = performance.now();
  setState({
    activeJob: mergedJob,
    lastObservedVersion: snapshot.version,
    variants: snapshot.variants || [],
    metrics,
  });
  logObservation(logDetails.event, {
    ...logDetails,
    response_received_at: receivedAt,
    received_version: snapshot.version,
    rendering_ms: performance.now() - renderStartedAt,
  });

  if (isTerminal(snapshot.status)) {
    console.table([buildMeasurement(mergedJob, metrics)]);
    stopObservation(false);
    downloadRunLog(mergedJob, metrics, state.selectedFile, receivedAt);
  }
}

function logObservation(event, details) {
  console.info("IMAGELAB_OBSERVATION", JSON.stringify({ event, ...details }));
}

function delay(ms, signal, onTimer) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    onTimer(timer);

    function onAbort() {
      clearTimeout(timer);
      reject(new DOMException("Observation cancelled", "AbortError"));
    }

    signal.addEventListener("abort", onAbort, { once: true });
  });
}

function downloadRunLog(job, metrics, file, terminalReceiptMs) {
  const mode = getState().observationMode === "sse" ? "sse" : "long_poll";
  const meta = window.EXPERIMENT_META || {};
  const defaultId = meta.condition
    ? `${meta.condition}-${mode}-${meta.repetition ?? 1}`
    : `A-${mode}-1`;

  const runId = window.prompt(
    "Enter run ID for this completed run (e.g., A-sse-1):",
    defaultId,
  );
  if (!runId) return; // Cancel skips download for casual non-experiment runs

  const [cond = "", , repStr = "1"] = runId.split("-");
  const record = {
    run_id: runId,
    condition: meta.condition || cond,
    mode,
    repetition: Number(meta.repetition || repStr) || 1,
    job_id: job.job_id ?? job.id,
    image: file?.name ?? "unknown",
    // Keep polling GETs and SSE stream GETs strictly separate per MEAS-05
    observation_gets: mode === "sse" ? 0 : metrics.pollingCount,
    sse_stream_gets: mode === "sse" ? metrics.sseStreamGets : 0,
    job_events_received: metrics.jobEventsReceived,
    versions_list: metrics.versionsReceived,
    version_timeline: metrics.versionTimeline,
    terminal_receipt_epoch_ms: terminalReceiptMs,
  };

  const blob = new Blob([JSON.stringify(record, null, 2) + "\n"], {
    type: "application/json",
  });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `${runId}.json`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}