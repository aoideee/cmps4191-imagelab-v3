import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const projectRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const evidenceDirectory = path.join(projectRoot, "docs", "assessment-3");
const rawDirectory = path.join(evidenceDirectory, "raw");
const graphDirectory = path.join(evidenceDirectory, "graphs");
await fs.mkdir(graphDirectory, { recursive: true });

const runs = [];
for (const condition of ["A", "B", "C"]) {
  const source = await fs.readFile(
    path.join(rawDirectory, `condition-${condition}.json`),
    "utf8",
  );
  runs.push(...JSON.parse(source));
}
if (runs.length !== 18) {
  throw new Error(`expected 18 measured runs; found ${runs.length}`);
}

const serverMetrics = await readServerMetrics(rawDirectory);
for (const run of runs) {
  const metrics = serverMetrics.get(run.job_id);
  if (metrics) {
    run.heartbeats = metrics.heartbeats;
    run.observation_db_reads = metrics.databaseReads;
    run.peak_active_streams = metrics.peakActiveStreams;
    if (run.mode === "sse") run.stream_seconds = metrics.streamMilliseconds / 1000;
  }
}
runs.sort((a, b) =>
  a.condition.localeCompare(b.condition) ||
  a.repetition - b.repetition ||
  a.mode.localeCompare(b.mode),
);

const columns = [
  "run_id",
  "condition",
  "mode",
  "repetition",
  "job_id",
  "image",
  "baseline_version",
  "terminal_version",
  "observation_gets",
  "sse_stream_gets",
  "job_events_received",
  "heartbeats",
  "versions_received",
  "version_gaps",
  "processing_ms",
  "detection_ms",
  "observation_db_reads",
  "stream_seconds",
  "peak_active_streams",
  "status",
  "notes",
];
await fs.writeFile(
  path.join(evidenceDirectory, "runs.csv"),
  toCSV(runs, columns),
);

const groups = [];
for (const condition of ["A", "B", "C"]) {
  for (const mode of ["sse", "long"]) {
    const selected = runs.filter(
      (run) => run.condition === condition && run.mode === mode,
    );
    groups.push({
      condition,
      mode,
      repetitions: selected.length,
      observation_gets_median: median(selected.map((run) => run.observation_gets)),
      observation_gets_min: Math.min(...selected.map((run) => run.observation_gets)),
      observation_gets_max: Math.max(...selected.map((run) => run.observation_gets)),
      detection_ms_median: median(selected.map((run) => run.detection_ms)),
      detection_ms_min: Math.min(...selected.map((run) => run.detection_ms)),
      detection_ms_max: Math.max(...selected.map((run) => run.detection_ms)),
    });
  }
}
await fs.writeFile(
  path.join(evidenceDirectory, "summary.csv"),
  toCSV(groups, Object.keys(groups[0])),
);

await fs.writeFile(
  path.join(graphDirectory, "observation-get-count.svg"),
  dotRangeChart({
    groups,
    runs,
    metric: "observation_gets",
    medianKey: "observation_gets_median",
    minKey: "observation_gets_min",
    maxKey: "observation_gets_max",
    title: "Observation GET count by condition and method",
    yTitle: "HTTP observation GETs per run",
  }),
);
await fs.writeFile(
  path.join(graphDirectory, "completion-detection-delay.svg"),
  dotRangeChart({
    groups,
    runs,
    metric: "detection_ms",
    medianKey: "detection_ms_median",
    minKey: "detection_ms_min",
    maxKey: "detection_ms_max",
    title: "Completion detection delay by condition and method",
    yTitle: "Detection delay (milliseconds)",
  }),
);

console.log(JSON.stringify({ runs: runs.length, groups: groups.length }));

async function readServerMetrics(directory) {
  const metrics = new Map();
  for (const condition of ["A", "B", "C"]) {
    const filename = path.join(directory, `server-condition-${condition}.log`);
    const text = await fs.readFile(filename, "utf8");
    for (const line of text.split("\n")) {
      const jobID = field(line, "job_id");
      if (!jobID) continue;
      const current = metrics.get(jobID) ?? {
        databaseReads: 0,
        heartbeats: 0,
        streamMilliseconds: 0,
        peakActiveStreams: 0,
      };
      if (line.includes('msg="job observation read"')) {
        current.databaseReads += Number(field(line, "database_reads") ?? 0);
      }
      if (line.includes('msg="SSE stream ended"')) {
        current.databaseReads += Number(field(line, "database_reads") ?? 0);
        current.heartbeats += Number(field(line, "heartbeats") ?? 0);
        current.streamMilliseconds += Number(field(line, "stream_ms") ?? 0);
        current.peakActiveStreams = Math.max(
          current.peakActiveStreams,
          Number(field(line, "peak_active_streams") ?? 0),
        );
      }
      metrics.set(jobID, current);
    }
  }
  return metrics;
}

function field(line, name) {
  return line.match(new RegExp(`${name}=(?:"([^"]*)"|([^\\s]+))`))?.slice(1).find(Boolean);
}

function median(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[middle - 1] + sorted[middle]) / 2
    : sorted[middle];
}

function toCSV(rows, headers) {
  const escape = (value) => {
    if (value === null || value === undefined) return "";
    const text = String(value);
    return /[",\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
  };
  return `${headers.join(",")}\n${rows
    .map((row) => headers.map((header) => escape(row[header])).join(","))
    .join("\n")}\n`;
}

function dotRangeChart({ groups, runs, metric, medianKey, minKey, maxKey, title, yTitle }) {
  const width = 1200;
  const height = 720;
  const margin = { top: 100, right: 70, bottom: 125, left: 115 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const maximum = Math.max(...groups.map((group) => group[maxKey]), 1);
  const yMaximum = Math.ceil(maximum * 1.15);
  const x = (index) => margin.left + ((index + 0.5) * plotWidth) / groups.length;
  const y = (value) => margin.top + plotHeight - (value / yMaximum) * plotHeight;
  const colors = { sse: "#2563EB", long: "#D97706" };
  const shapes = [];

  for (let tick = 0; tick <= 5; tick++) {
    const value = (yMaximum * tick) / 5;
    const position = y(value);
    shapes.push(`<line x1="${margin.left}" y1="${position}" x2="${width - margin.right}" y2="${position}" stroke="#D7DEE8" stroke-width="1"/>`);
    shapes.push(`<text x="${margin.left - 16}" y="${position + 6}" text-anchor="end" class="tick">${format(value)}</text>`);
  }

  groups.forEach((group, index) => {
    const center = x(index);
    const color = colors[group.mode];
    const values = runs
      .filter((run) => run.condition === group.condition && run.mode === group.mode)
      .map((run) => run[metric]);
    shapes.push(`<line x1="${center}" y1="${y(group[minKey])}" x2="${center}" y2="${y(group[maxKey])}" stroke="${color}" stroke-width="5"/>`);
    shapes.push(`<line x1="${center - 14}" y1="${y(group[minKey])}" x2="${center + 14}" y2="${y(group[minKey])}" stroke="${color}" stroke-width="4"/>`);
    shapes.push(`<line x1="${center - 14}" y1="${y(group[maxKey])}" x2="${center + 14}" y2="${y(group[maxKey])}" stroke="${color}" stroke-width="4"/>`);
    values.forEach((value, repetitionIndex) => {
      const offset = [-20, 0, 20][repetitionIndex];
      const fill = group.mode === "sse" ? color : "#FFFFFF";
      shapes.push(`<circle cx="${center + offset}" cy="${y(value)}" r="8" fill="${fill}" stroke="${color}" stroke-width="3"><title>Condition ${group.condition}, ${group.mode}, repetition ${repetitionIndex + 1}: ${format(value)}</title></circle>`);
    });
    shapes.push(`<line x1="${center - 24}" y1="${y(group[medianKey])}" x2="${center + 24}" y2="${y(group[medianKey])}" stroke="#111827" stroke-width="5"/>`);
    shapes.push(`<text x="${center}" y="${height - margin.bottom + 34}" text-anchor="middle" class="label">${group.condition}</text>`);
    shapes.push(`<text x="${center}" y="${height - margin.bottom + 62}" text-anchor="middle" class="sub">${group.mode === "sse" ? "SSE" : "Long poll"}</text>`);
  });

  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" role="img" aria-labelledby="title description">
  <title id="title">${title}</title>
  <desc id="description">Individual repetitions appear as circles. The dark horizontal mark is the median and the colored vertical line spans minimum to maximum.</desc>
  <style>.title{font:700 30px system-ui,sans-serif;fill:#111827}.subtitle{font:17px system-ui,sans-serif;fill:#4B5563}.tick,.sub{font:16px system-ui,sans-serif;fill:#4B5563}.label{font:700 18px system-ui,sans-serif;fill:#111827}.axis{font:700 18px system-ui,sans-serif;fill:#111827}</style>
  <rect width="100%" height="100%" fill="#FFFFFF"/>
  <text x="${margin.left}" y="45" class="title">${title}</text>
  <text x="${margin.left}" y="73" class="subtitle">Three repetitions per group; dark mark = median, colored range = minimum–maximum</text>
  ${shapes.join("\n  ")}
  <line x1="${margin.left}" y1="${margin.top + plotHeight}" x2="${width - margin.right}" y2="${margin.top + plotHeight}" stroke="#111827" stroke-width="2"/>
  <line x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${margin.top + plotHeight}" stroke="#111827" stroke-width="2"/>
  <text x="32" y="${margin.top + plotHeight / 2}" text-anchor="middle" transform="rotate(-90 32 ${margin.top + plotHeight / 2})" class="axis">${yTitle}</text>
  <circle cx="${width - 310}" cy="50" r="7" fill="#2563EB" stroke="#2563EB" stroke-width="3"/><text x="${width - 294}" y="56" class="tick">SSE</text>
  <circle cx="${width - 210}" cy="50" r="7" fill="#FFFFFF" stroke="#D97706" stroke-width="3"/><text x="${width - 194}" y="56" class="tick">Long polling</text>
</svg>`;
}

function format(value) {
  return Number.isInteger(value) ? String(value) : value.toFixed(1);
}
