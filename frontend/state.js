import { emitter } from "./modules/event-emitter.js";

const state = {
  selectedFile: null,
  isValidating: false,
  isSubmitting: false, // Double-submit guard
  activeJob: null, // Stores status, job_id, image_id, timestamps
  uploadError: null,
  variants: [],
  observationMode: "long",
  lastObservedVersion: null,
  pollingTimer: null,
  abortController: null,
  eventSource: null,
  isObserving: false,
  observationState: "idle",
  observationError: false,
  metrics: {
    requestStart: null,
    ackLatency: null,
    pollingCount: 0,
    observationGets: 0,
    sseStreamGets: 0,
    jobEventsReceived: 0,
    versionsReceived: [],
    completionObservedAt: null,
  },
};

export function getState() {
  return state;
}

export function setState(updates) {
  Object.assign(state, updates);
  emitter.emit("stateChanged", state);
}
