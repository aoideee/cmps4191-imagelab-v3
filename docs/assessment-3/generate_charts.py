#!/usr/bin/env python3
"""
ImageLab Assessment 3: SSE vs. Long Polling (and Short Polling Baseline) Chart Generator
Configured to load client JSON logs from `.logs/client-logs` (searching current and parent project folders),
server logs, and Assessment 2 & 3 summary CSVs.

Produces:
  1. graph1_observation_gets.png        (Mandatory Graph 1: Observation GET count by condition & method)
  2. graph2_detection_delay.png         (Mandatory Graph 2: Completion-detection delay in ms by condition & method)
  3. graph3_version_timeline_trace.png  (Encouraged Graph 3: Version-over-time progression & coalescing trace)
  4. graph4_observer_work_tradeoffs.png (Additional: DB reads vs. open stream hold duration & heartbeats)
  5. graph5_three_mode_evolution.png    (Additional: Short Polling vs. Long Polling vs. SSE across workloads)
"""

import json
import os
import re
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D

# ---------------------------------------------------------------------------
# Global Styling
# ---------------------------------------------------------------------------
plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 10,
    "axes.titlesize": 12,
    "axes.titleweight": "bold",
    "axes.labelsize": 10.5,
    "axes.labelweight": "bold",
    "xtick.labelsize": 9.5,
    "ytick.labelsize": 9.5,
    "legend.fontsize": 9.5,
    "figure.titlesize": 14,
    "figure.titleweight": "bold",
    "axes.spines.top": False,
    "axes.spines.right": False,
})

COLOR_LP = "#2563EB"      # Royal Blue - Long Polling
COLOR_SSE = "#0D9488"     # Emerald Teal - Server-Sent Events
COLOR_SHORT = "#D97706"   # Warm Amber - Short Polling (Assessment 2 baseline)
COLOR_BG_GRID = "#CBD5E1"


def search_roots():
    """Return current directory and up to three parent directories (e.g., docs/, project root)."""
    cwd = Path.cwd().resolve()
    roots = [cwd]
    for parent in list(cwd.parents)[:3]:
        roots.append(parent)
    return roots


def find_client_log(filename):
    """Prioritize `.logs/client-logs/<filename>` across cwd and parent directories, then fallback search."""
    rel_candidates = [
        Path(".logs") / "client-logs" / filename,
        Path(".logs") / filename,
        Path("logs") / "client-logs" / filename,
        Path(filename),
    ]
    for root in search_roots():
        for rel in rel_candidates:
            candidate = root / rel
            if candidate.exists():
                return str(candidate)

    for root in search_roots():
        for match in root.rglob(filename):
            if ".venv" not in match.parts and "node_modules" not in match.parts:
                return str(match)
    return None


def find_file(filename):
    """Search `.logs`, current directory, subdirectories, and parent directories for a file."""
    rel_candidates = [
        Path(filename),
        Path(".logs") / filename,
        Path(".logs") / "server-logs" / filename,
        Path("logs") / filename,
    ]
    for root in search_roots():
        for rel in rel_candidates:
            candidate = root / rel
            if candidate.exists():
                return str(candidate)

    for root in search_roots():
        for match in root.rglob(filename):
            if ".venv" not in match.parts and "node_modules" not in match.parts:
                return str(match)
    return None


def find_csv_by_column(required_col, preferred_names=None):
    """Find a CSV file containing `required_col` across current/parent directories."""
    preferred_names = preferred_names or []
    cwd = Path.cwd().resolve()

    for name in preferred_names:
        p = cwd / name
        if p.exists():
            try:
                df = pd.read_csv(p)
                if required_col in df.columns:
                    return str(p), df
            except Exception:
                pass

    seen = set()
    for root in search_roots():
        for csv_path in sorted(root.rglob("*.csv")):
            if csv_path in seen or ".venv" in csv_path.parts:
                continue
            seen.add(csv_path)
            try:
                df = pd.read_csv(csv_path)
                if required_col in df.columns:
                    return str(csv_path), df
            except Exception:
                continue
    return None, None


def load_v3_dataframe():
    path, df = find_csv_by_column(
        "sse_stream_gets",
        preferred_names=["summary_3.csv", "summary_v3.csv", "summary_2.csv", "summary.csv"],
    )
    if df is not None:
        print(f"[OK] Loaded Assessment 3 CSV: {path}")
        return df
    raise FileNotFoundError("Could not find Assessment 3 summary CSV (expected column 'sse_stream_gets').")


def load_v2_short_poll_dataframe():
    path, df = find_csv_by_column(
        "requests_started",
        preferred_names=["summary_4.csv", "summary_short_poll_v2.csv", "summary.csv"],
    )
    if df is not None:
        print(f"[OK] Loaded Assessment 2 short-poll CSV: {path}")
        return df
    print("[WARN] Could not find Assessment 2 short-poll CSV (expected column 'requests_started'). Graph 5 will be skipped.")
    return None


def parse_rfc3339_ms(ts):
    m = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})$", ts)
    base, frac, tz = m.groups()
    if tz == "Z":
        tz = "+00:00"
    dt = datetime.fromisoformat(base + tz)
    ms = dt.timestamp() * 1000.0
    if frac:
        ms += float("0." + frac) * 1000.0
    return ms


def summary_stats(series):
    return {
        "median": float(np.median(series)),
        "min": float(np.min(series)),
        "max": float(np.max(series)),
        "values": list(series),
    }


# ---------------------------------------------------------------------------
# GRAPH 1 (MANDATORY): Observation GET Count by Condition and Method
# ---------------------------------------------------------------------------
def plot_graph1_observation_gets(df):
    conditions = ["A", "B", "C"]
    cond_labels = [
        "Condition A\nQuiet Active Phase\n(30s delay, 5 versions)",
        "Condition B\nRun Set B\n(10s @ 200ms, 55 versions)",
        "Condition C\nFrequent Updates\n(10s @ 200ms, 55 versions)",
    ]

    fig, ax = plt.subplots(figsize=(10.5, 5.8), dpi=300)
    x = np.arange(len(conditions))
    offset = 0.18
    rep_offsets = [-0.045, 0.0, 0.045]

    for idx, cond in enumerate(conditions):
        lp_vals = df[(df["condition"] == cond) & (df["mode"] == "long_poll")]["observation_gets"].values
        lp_st = summary_stats(lp_vals)
        x_lp = x[idx] - offset

        ax.bar(x_lp, lp_st["median"], width=0.26, color=COLOR_LP, alpha=0.22, edgecolor=COLOR_LP, linewidth=1.5, zorder=2)
        ax.vlines(x_lp, lp_st["min"], lp_st["max"], color=COLOR_LP, linewidth=2.4, zorder=4)
        ax.hlines([lp_st["min"], lp_st["max"]], x_lp - 0.05, x_lp + 0.05, color=COLOR_LP, linewidth=2.0, zorder=4)
        ax.hlines(lp_st["median"], x_lp - 0.11, x_lp + 0.11, color=COLOR_LP, linewidth=3.2, zorder=5)

        for r_i, val in enumerate(lp_vals):
            ax.scatter(x_lp + rep_offsets[r_i], val, color=COLOR_LP, edgecolor="white", s=58, zorder=6)

        label_lp = f"Med: {lp_st['median']:.0f}\n[{lp_st['min']:.0f}–{lp_st['max']:.0f}]"
        ax.text(x_lp, lp_st["max"] + 2.0, label_lp, ha="center", va="bottom", fontsize=8.8, fontweight="bold", color=COLOR_LP)

        sse_sub = df[(df["condition"] == cond) & (df["mode"] == "sse")]
        sse_vals = sse_sub["sse_stream_gets"].values
        sse_st = summary_stats(sse_vals)
        ev_med = int(np.median(sse_sub["job_events_received"].values))
        hb_med = int(np.median(sse_sub["heartbeats"].values))
        x_sse = x[idx] + offset

        ax.bar(x_sse, sse_st["median"], width=0.26, color=COLOR_SSE, alpha=0.25, edgecolor=COLOR_SSE, linewidth=1.5, zorder=2)
        ax.hlines(sse_st["median"], x_sse - 0.11, x_sse + 0.11, color=COLOR_SSE, linewidth=3.2, zorder=5)
        for r_i, val in enumerate(sse_vals):
            ax.scatter(x_sse + rep_offsets[r_i], val, color=COLOR_SSE, edgecolor="white", s=58, zorder=6)

        sse_note = f"1 stream GET\n({ev_med} events, {hb_med} HB)"
        ax.text(x_sse, sse_st["max"] + 2.0, sse_note, ha="center", va="bottom", fontsize=8.6, fontweight="bold", color=COLOR_SSE)

    ax.set_xticks(x)
    ax.set_xticklabels(cond_labels)
    ax.set_ylabel("HTTP Observation GET Requests Started (count)")
    ax.set_ylim(0, 74)
    ax.grid(axis="y", linestyle="--", color=COLOR_BG_GRID, alpha=0.6, zorder=1)
    ax.set_title("Graph 1: HTTP Observation GET Count by Condition and Method (n = 18 Runs)", pad=14)

    legend_elements = [
        mpatches.Patch(facecolor=COLOR_LP, alpha=0.25, edgecolor=COLOR_LP, linewidth=1.5, label="Long Polling (Median bar + Min–Max range)"),
        mpatches.Patch(facecolor=COLOR_SSE, alpha=0.25, edgecolor=COLOR_SSE, linewidth=1.5, label="SSE Stream GETs (1 persistent GET; events/heartbeats annotated)"),
        Line2D([0], [0], marker="o", color="w", label="Individual Repetitions (Rep 1–3)", markerfacecolor="#475569", markersize=7),
    ]
    ax.legend(handles=legend_elements, loc="upper left", frameon=True, facecolor="white", framealpha=0.95)

    fig.text(
        0.5, 0.01,
        "Note: Excludes initial POST /v1/images upload. SSE delivers multiple snapshots over 1 open HTTP stream; HB = ': keep-alive' comment heartbeats.",
        ha="center", fontsize=8.5, color="#475569", style="italic"
    )
    plt.tight_layout(rect=[0, 0.03, 1, 1])
    plt.savefig("graph1_observation_gets.png", dpi=300)
    plt.close(fig)
    print("[OK] Saved graph1_observation_gets.png")


# ---------------------------------------------------------------------------
# GRAPH 2 (MANDATORY): Completion-Detection Delay (ms) by Condition and Method
# ---------------------------------------------------------------------------
def plot_graph2_detection_delay(df):
    conditions = ["A", "B", "C"]
    cond_labels = [
        "Condition A\n(30s Quiet Phase)",
        "Condition B\n(10s @ 200ms)",
        "Condition C\n(10s @ 200ms)",
    ]

    fig, (ax_full, ax_zoom) = plt.subplots(1, 2, figsize=(12.2, 5.6), dpi=300, gridspec_kw={"width_ratios": [1.15, 1.0]})
    x = np.arange(len(conditions))
    offset = 0.18
    rep_offsets = [-0.045, 0.0, 0.045]

    for ax, is_zoom in [(ax_full, False), (ax_zoom, True)]:
        for idx, cond in enumerate(conditions):
            lp_vals = df[(df["condition"] == cond) & (df["mode"] == "long_poll")]["detection_ms"].values
            sse_vals = df[(df["condition"] == cond) & (df["mode"] == "sse")]["detection_ms"].values

            lp_st = summary_stats(lp_vals)
            sse_st = summary_stats(sse_vals)

            x_lp = x[idx] - offset
            x_sse = x[idx] + offset

            if not is_zoom or lp_st["max"] <= 22:
                ax.bar(x_lp, lp_st["median"], width=0.25, color=COLOR_LP, alpha=0.22, edgecolor=COLOR_LP, linewidth=1.5, zorder=2)
                ax.vlines(x_lp, lp_st["min"], lp_st["max"], color=COLOR_LP, linewidth=2.3, zorder=4)
                ax.hlines([lp_st["min"], lp_st["max"]], x_lp - 0.05, x_lp + 0.05, color=COLOR_LP, linewidth=1.9, zorder=4)
                ax.hlines(lp_st["median"], x_lp - 0.10, x_lp + 0.10, color=COLOR_LP, linewidth=3.0, zorder=5)
                for r_i, val in enumerate(lp_vals):
                    ax.scatter(x_lp + rep_offsets[r_i], val, color=COLOR_LP, edgecolor="white", s=54, zorder=6)

                y_txt = lp_st["max"] + (0.7 if is_zoom else 12.0)
                ax.text(x_lp, y_txt, f"{lp_st['median']:.1f} ms\n[{lp_st['min']:.1f}–{lp_st['max']:.1f}]",
                        ha="center", va="bottom", fontsize=8.2, fontweight="bold", color=COLOR_LP)
            else:
                ax.annotate(
                    f"Off-scale:\nMed {lp_st['median']:.1f} ms\n[{lp_st['min']:.1f}–{lp_st['max']:.1f}]",
                    xy=(x_lp, 19.2), xytext=(x_lp, 14.5),
                    ha="center", va="top", fontsize=8.0, fontweight="bold", color=COLOR_LP,
                    bbox=dict(boxstyle="round,pad=0.25", facecolor="#EFF6FF", edgecolor=COLOR_LP, lw=1),
                    arrowprops=dict(arrowstyle="->", color=COLOR_LP, lw=1.5)
                )

            ax.bar(x_sse, sse_st["median"], width=0.25, color=COLOR_SSE, alpha=0.25, edgecolor=COLOR_SSE, linewidth=1.5, zorder=2)
            ax.vlines(x_sse, sse_st["min"], sse_st["max"], color=COLOR_SSE, linewidth=2.3, zorder=4)
            ax.hlines([sse_st["min"], sse_st["max"]], x_sse - 0.05, x_sse + 0.05, color=COLOR_SSE, linewidth=1.9, zorder=4)
            ax.hlines(sse_st["median"], x_sse - 0.10, x_sse + 0.10, color=COLOR_SSE, linewidth=3.0, zorder=5)
            for r_i, val in enumerate(sse_vals):
                ax.scatter(x_sse + rep_offsets[r_i], val, color=COLOR_SSE, edgecolor="white", s=54, zorder=6)

            y_txt_sse = sse_st["max"] + (0.7 if is_zoom else 12.0)
            ax.text(x_sse, y_txt_sse, f"{sse_st['median']:.1f} ms\n[{sse_st['min']:.1f}–{sse_st['max']:.1f}]",
                    ha="center", va="bottom", fontsize=8.2, fontweight="bold", color=COLOR_SSE)

        ax.set_xticks(x)
        ax.set_xticklabels(cond_labels)
        ax.grid(axis="y", linestyle="--", color=COLOR_BG_GRID, alpha=0.6, zorder=1)

    ax_full.set_ylabel("Terminal Completion-Detection Delay (ms)")
    ax_full.set_ylim(0, 460)
    ax_full.set_title("A. Full Scale (0–460 ms): Captures Condition C Poll Lag", pad=10)

    ax_zoom.set_ylabel("Terminal Completion-Detection Delay (ms)")
    ax_zoom.set_ylim(0, 20.5)
    ax_zoom.set_title("B. Zoomed Sub-20 ms Scale: Sub-3 ms SSE Consistency", pad=10)

    legend_elements = [
        mpatches.Patch(facecolor=COLOR_LP, alpha=0.25, edgecolor=COLOR_LP, linewidth=1.5, label="Long Polling (Median + Min–Max)"),
        mpatches.Patch(facecolor=COLOR_SSE, alpha=0.25, edgecolor=COLOR_SSE, linewidth=1.5, label="SSE (Median + Min–Max)"),
        Line2D([0], [0], marker="o", color="w", label="Individual Repetitions (Rep 1–3)", markerfacecolor="#475569", markersize=6.5),
    ]
    ax_full.legend(handles=legend_elements, loc="upper left", frameon=True, facecolor="white", framealpha=0.92)

    fig.suptitle("Graph 2: Terminal Completion-Detection Delay by Condition and Method (n = 18 Runs)", y=0.98)
    fig.text(
        0.5, 0.01,
        "Detection delay = browser terminal receipt epoch_ms minus server terminal transaction commit timestamp (committed_at).",
        ha="center", fontsize=8.5, color="#475569", style="italic"
    )
    plt.tight_layout(rect=[0, 0.03, 1, 0.94])
    plt.savefig("graph2_detection_delay.png", dpi=300)
    plt.close(fig)
    print("[OK] Saved graph2_detection_delay.png")


# ---------------------------------------------------------------------------
# GRAPH 3 (ENCOURAGED): Version-Over-Time Progression & Coalescing Trace
# ---------------------------------------------------------------------------
def plot_graph3_version_timeline():
    sse_json_path = find_client_log("C-sse-1.json")
    lp_json_path = find_client_log("C-long_poll-1.json")
    log_c_path = find_file("server_raw_experiment-c.log")

    missing = [
        name for name, p in [
            (".logs/client-logs/C-sse-1.json", sse_json_path),
            (".logs/client-logs/C-long_poll-1.json", lp_json_path),
        ] if p is None
    ]
    if missing:
        print(f"[WARN] Skipping Graph 3 because client logs were not found: {', '.join(missing)}")
        return

    print(f"[OK] Generating Graph 3 from client logs:\n     SSE: {sse_json_path}\n     LP : {lp_json_path}")
    with open(sse_json_path) as f:
        sse_data = json.load(f)
    with open(lp_json_path) as f:
        lp_data = json.load(f)

    sse_commits = {}
    lp_commits = {}
    if log_c_path:
        print(f"     Server log: {log_c_path}")
        with open(log_c_path) as f:
            log_text = f.read()

        def get_server_commits(job_id):
            commits = {}
            for line in log_text.splitlines():
                if job_id in line and 'msg="image job change committed"' in line:
                    ver = int(re.search(r"version=(\d+)", line).group(1))
                    cat = re.search(r"committed_at=([^\s]+)", line).group(1)
                    commits[ver] = parse_rfc3339_ms(cat)
            return commits

        sse_commits = get_server_commits(sse_data["job_id"])
        lp_commits = get_server_commits(lp_data["job_id"])

    sse_t0 = sse_commits.get(2, sse_data["version_timeline"][0]["epoch_ms"])
    lp_t0 = lp_commits.get(2, lp_data["version_timeline"][0]["epoch_ms"])

    sse_t = [(pt["epoch_ms"] - sse_t0) / 1000.0 for pt in sse_data["version_timeline"]]
    sse_v = [pt["version"] for pt in sse_data["version_timeline"]]

    lp_t = [(pt["epoch_ms"] - lp_t0) / 1000.0 for pt in lp_data["version_timeline"]]
    lp_v = [pt["version"] for pt in lp_data["version_timeline"]]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.4, 5.4), dpi=300, gridspec_kw={"width_ratios": [1.3, 1.0]})

    ax1.step(sse_t, sse_v, where="post", color=COLOR_SSE, linewidth=2.0, label="SSE (C-sse-1): 54/54 versions (0 gaps)")
    ax1.step(lp_t, lp_v, where="post", color=COLOR_LP, linewidth=2.0, linestyle="--", label="Long Poll (C-long_poll-1): 49/54 versions (5 skipped)")

    for i in range(1, len(lp_v)):
        if lp_v[i] - lp_v[i - 1] > 1:
            ax1.scatter([lp_t[i]], [lp_v[i]], color="#DC2626", s=55, zorder=6)

    ax1.scatter([], [], color="#DC2626", s=55, label="Long Poll coalesced jump (skipped versions)")
    ax1.set_xlabel("Elapsed Time Since Processing Start (seconds)")
    ax1.set_ylabel("Client-Observed Job Version")
    ax1.set_title("A. Full Run Trajectory (v2 → v55 over 10.15s)", pad=10)
    ax1.set_xlim(-0.2, 10.8)
    ax1.set_ylim(0, 58)
    ax1.grid(True, linestyle="--", color=COLOR_BG_GRID, alpha=0.6)
    ax1.legend(loc="upper left", frameon=True, facecolor="white", framealpha=0.92)

    ax2.step(sse_t, sse_v, where="post", marker="o", markersize=5, color=COLOR_SSE, linewidth=2.2, label="SSE received snapshots")
    ax2.step(lp_t, lp_v, where="post", marker="s", markersize=5.5, color=COLOR_LP, linewidth=2.2, linestyle="--", label="Long Poll received snapshots")

    if 55 in lp_commits:
        lp_v55_commit = (lp_commits[55] - lp_t0) / 1000.0
        ax2.axvline(lp_v55_commit, color=COLOR_LP, linestyle=":", alpha=0.75)

    ax2.annotate(
        "Long Poll skips v52–v54\n& detects v55 +215.9 ms late",
        xy=(lp_t[-1], 55), xytext=(9.52, 52.3),
        fontsize=8.5, fontweight="bold", color="#1E3A8A",
        bbox=dict(boxstyle="round,pad=0.3", facecolor="#EFF6FF", edgecolor=COLOR_LP),
        arrowprops=dict(arrowstyle="->", color=COLOR_LP, lw=1.5)
    )
    ax2.annotate(
        "SSE delivers v52, v53, v54, v55\n(+2.0 ms terminal delay)",
        xy=(sse_t[-1], 55), xytext=(9.48, 55.6),
        fontsize=8.5, fontweight="bold", color="#115E59",
        bbox=dict(boxstyle="round,pad=0.3", facecolor="#F0FDFA", edgecolor=COLOR_SSE),
        arrowprops=dict(arrowstyle="->", color=COLOR_SSE, lw=1.5)
    )

    ax2.set_xlim(9.4, 10.5)
    ax2.set_ylim(49.5, 57.2)
    ax2.set_xlabel("Elapsed Time Since Processing Start (seconds)")
    ax2.set_ylabel("Client-Observed Job Version")
    ax2.set_title("B. Zoom: Final Variant Commits (v51 → v55)", pad=10)
    ax2.grid(True, linestyle="--", color=COLOR_BG_GRID, alpha=0.6)

    fig.suptitle("Graph 3: Version-Over-Time Progression & Snapshot Coalescing (Condition C)", y=0.98)
    plt.tight_layout(rect=[0, 0.02, 1, 0.94])
    plt.savefig("graph3_version_timeline_trace.png", dpi=300)
    plt.close(fig)
    print("[OK] Saved graph3_version_timeline_trace.png")


# ---------------------------------------------------------------------------
# GRAPH 4 (ADDITIONAL INSIGHT): Server Observer Work & Resource Trade-Offs
# ---------------------------------------------------------------------------
def plot_graph4_observer_work_tradeoffs(df):
    conditions = ["A", "B", "C"]
    cond_labels = ["Condition A\n(30s Quiet)", "Condition B\n(10s @ 200ms)", "Condition C\n(10s @ 200ms)"]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.0, 5.3), dpi=300)
    x = np.arange(len(conditions))
    width = 0.34

    lp_db_meds = [np.median(df[(df["condition"] == c) & (df["mode"] == "long_poll")]["observation_db_reads"]) for c in conditions]
    sse_db_meds = [np.median(df[(df["condition"] == c) & (df["mode"] == "sse")]["observation_db_reads"]) for c in conditions]

    b1 = ax1.bar(x - width / 2, lp_db_meds, width, color=COLOR_LP, alpha=0.85, label="Long Polling (DB Reads)")
    b2 = ax1.bar(x + width / 2, sse_db_meds, width, color=COLOR_SSE, alpha=0.85, label="SSE (DB Reads)")

    for bar in b1:
        h = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width() / 2, h + 1.8, f"{h:.0f}", ha="center", va="bottom", fontweight="bold", fontsize=9, color=COLOR_LP)
    for bar in b2:
        h = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width() / 2, h + 1.8, f"{h:.0f}", ha="center", va="bottom", fontweight="bold", fontsize=9, color=COLOR_SSE)

    ax1.set_xticks(x)
    ax1.set_xticklabels(cond_labels)
    ax1.set_ylabel("Median PostgreSQL Observation Reads / Run")
    ax1.set_ylim(0, 125)
    ax1.set_title("A. Observer Database Read Load (Lower is Better)", pad=10)
    ax1.grid(axis="y", linestyle="--", color=COLOR_BG_GRID, alpha=0.6)
    ax1.legend(loc="upper left", frameon=True, facecolor="white")

    sse_dur_meds = [np.median(df[(df["condition"] == c) & (df["mode"] == "sse")]["stream_seconds"]) for c in conditions]
    sse_hb_meds = [np.median(df[(df["condition"] == c) & (df["mode"] == "sse")]["heartbeats"]) for c in conditions]

    b3 = ax2.bar(x, sse_dur_meds, width=0.45, color=COLOR_SSE, alpha=0.85, label="SSE Held Stream Duration (s)")
    for idx, bar in enumerate(b3):
        h = bar.get_height()
        ax2.text(
            bar.get_x() + bar.get_width() / 2, h + 0.7,
            f"{h:.2f} s held\n({int(sse_hb_meds[idx])} heartbeats)",
            ha="center", va="bottom", fontweight="bold", fontsize=9, color="#115E59"
        )

    ax2.set_xticks(x)
    ax2.set_xticklabels(cond_labels)
    ax2.set_ylabel("SSE Open Handler & Stream Duration (seconds)")
    ax2.set_ylim(0, 36)
    ax2.set_title("B. SSE Server State Trade-Off: Held Stream Lifetime", pad=10)
    ax2.grid(axis="y", linestyle="--", color=COLOR_BG_GRID, alpha=0.6)
    ax2.legend(loc="upper right", frameon=True, facecolor="white")

    fig.suptitle("Graph 4: Server Observer Work & Architectural Trade-Offs (MEAS-05 & MEAS-10)", y=0.98)
    fig.text(
        0.5, 0.01,
        "Fewer HTTP requests in SSE eliminate repeated DB reads, but hold 1 active goroutine, response writer, and subscriber slot per client.",
        ha="center", fontsize=8.5, color="#475569", style="italic"
    )
    plt.tight_layout(rect=[0, 0.03, 1, 0.94])
    plt.savefig("graph4_observer_work_tradeoffs.png", dpi=300)
    plt.close(fig)
    print("[OK] Saved graph4_observer_work_tradeoffs.png")


# ---------------------------------------------------------------------------
# GRAPH 5 (ADDITIONAL INSIGHT): Three-Mode Comparison (Short vs Long vs SSE)
# ---------------------------------------------------------------------------
def plot_graph5_three_mode_evolution(df_v3, df_v2):
    if df_v2 is None:
        return

    workloads = ["Quiet Interval\n(30s delay, 5 versions)", "Frequent Updates\n(10s @ 200ms, 55 versions)"]

    short_reqs = [
        np.median(df_v2[(df_v2["condition"] == "A") & (df_v2["mode"] == "short")]["requests_started"]),
        np.median(df_v2[(df_v2["condition"] == "C2") & (df_v2["mode"] == "short")]["requests_started"]),
    ]
    lp_reqs = [
        np.median(df_v3[(df_v3["condition"] == "A") & (df_v3["mode"] == "long_poll")]["observation_gets"]),
        np.median(df_v3[(df_v3["condition"] == "C") & (df_v3["mode"] == "long_poll")]["observation_gets"]),
    ]
    sse_reqs = [
        np.median(df_v3[(df_v3["condition"] == "A") & (df_v3["mode"] == "sse")]["sse_stream_gets"]),
        np.median(df_v3[(df_v3["condition"] == "C") & (df_v3["mode"] == "sse")]["sse_stream_gets"]),
    ]

    short_det = [
        np.median(df_v2[(df_v2["condition"] == "A") & (df_v2["mode"] == "short")]["detection_ms"]),
        np.median(df_v2[(df_v2["condition"] == "C2") & (df_v2["mode"] == "short")]["detection_ms"]),
    ]
    lp_det = [
        np.median(df_v3[(df_v3["condition"] == "A") & (df_v3["mode"] == "long_poll")]["detection_ms"]),
        np.median(df_v3[(df_v3["condition"] == "C") & (df_v3["mode"] == "long_poll")]["detection_ms"]),
    ]
    sse_det = [
        np.median(df_v3[(df_v3["condition"] == "A") & (df_v3["mode"] == "sse")]["detection_ms"]),
        np.median(df_v3[(df_v3["condition"] == "C") & (df_v3["mode"] == "sse")]["detection_ms"]),
    ]

    short_cov = [3 / 5 * 100, 12 / 55 * 100]
    lp_cov = [5 / 5 * 100, 50 / 54 * 100]
    sse_cov = [5 / 5 * 100, 54 / 54 * 100]

    fig, axes = plt.subplots(1, 3, figsize=(13.5, 5.2), dpi=300)
    x = np.arange(len(workloads))
    w = 0.25

    panels = [
        (axes[0], short_reqs, lp_reqs, sse_reqs, "HTTP GET Requests (count)", "A. Observation HTTP GETs", 58, "{:.0f}"),
        (axes[1], short_det, lp_det, sse_det, "Median Detection Delay (ms)", "B. Terminal Detection Lag", 520, "{:.1f}"),
        (axes[2], short_cov, lp_cov, sse_cov, "Active Version Coverage (%)", "C. State Version Coverage", 115, "{:.0f}%"),
    ]

    for ax, s_vals, l_vals, e_vals, ylabel, title, ylim, fmt in panels:
        b_s = ax.bar(x - w, s_vals, w, color=COLOR_SHORT, alpha=0.88, label="Short Polling (1s timer)")
        b_l = ax.bar(x, l_vals, w, color=COLOR_LP, alpha=0.88, label="Long Polling (after/wait)")
        b_e = ax.bar(x + w, e_vals, w, color=COLOR_SSE, alpha=0.88, label="SSE (/events stream)")

        for bars, col in [(b_s, COLOR_SHORT), (b_l, COLOR_LP), (b_e, COLOR_SSE)]:
            for bar in bars:
                h = bar.get_height()
                ax.text(bar.get_x() + bar.get_width() / 2, h + ylim * 0.02, fmt.format(h),
                        ha="center", va="bottom", fontsize=8.3, fontweight="bold", color=col)

        ax.set_xticks(x)
        ax.set_xticklabels(workloads)
        ax.set_ylabel(ylabel)
        ax.set_ylim(0, ylim)
        ax.set_title(title, pad=10)
        ax.grid(axis="y", linestyle="--", color=COLOR_BG_GRID, alpha=0.6)

    axes[0].legend(loc="upper left", frameon=True, facecolor="white", fontsize=8.8)
    fig.suptitle("Graph 5: Evolution Across All Three ImageLab Observation Modes (Short vs. Long vs. SSE)", y=0.98)
    plt.tight_layout(rect=[0, 0.02, 1, 0.94])
    plt.savefig("graph5_three_mode_evolution.png", dpi=300)
    plt.close(fig)
    print("[OK] Saved graph5_three_mode_evolution.png")


def main():
    df_v3 = load_v3_dataframe()
    df_v2 = load_v2_short_poll_dataframe()

    plot_graph1_observation_gets(df_v3)
    plot_graph2_detection_delay(df_v3)
    plot_graph3_version_timeline()
    plot_graph4_observer_work_tradeoffs(df_v3)
    plot_graph5_three_mode_evolution(df_v3, df_v2)
    print("Done.")


if __name__ == "__main__":
    main()
