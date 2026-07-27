import csv
import os
import re
import statistics
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd
import altair as alt
alt.renderers.enable("browser")
alt.data_transformers.disable_max_rows()

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

FOLDER = "data/latency_logs"
PROCESSED_FILE = "analysis/latency_processed.csv"
ENTRIES_TO_SKIP = 350  # per file, for warmup
BIN_SIZE = 200  # ms
CLIP_MAX = 3000  # ms

TIME_WINDOWS = [
    (8, 30, "08:30 AM Rush"),
    (10, 0, "10:00 AM Low"),
    (12, 0, "12:00 PM Lunch"),
    (14, 0, "02:00 PM Low"),
    (17, 0, "05:00 PM Rush"),
]
WINDOW_DURATION_MIN = 30


# ─────────────────────────────────────────────
# File Parsing
# ─────────────────────────────────────────────

def parse_filename(filename):
    """Extract UDID from filename."""
    match = re.match(r"latency_log_(.+)_(\d{8}T\d{6})\.csv", filename)
    if not match:
        return None
    return match.group(1)


def group_files_by_udid(files):
    """Group filenames by UDID."""
    groups = {}
    for f in files:
        udid = parse_filename(f)
        if udid:
            if udid not in groups:
                groups[udid] = []
            groups[udid].append(f)
    return groups


# ─────────────────────────────────────────────
# Data Loading & Processing
# ─────────────────────────────────────────────

def load_single_file(filepath, entries_to_skip=ENTRIES_TO_SKIP):
    """Load latency + recv_time from a single CSV, skipping warmup."""
    rows = []
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for _ in range(entries_to_skip):
            next(reader, None)
        for row in reader:
            try:
                latency = float(row["latency_ms"])
                recv_time = datetime.fromisoformat(row["recv_time"])
                if recv_time.tzinfo is None:
                    recv_time = recv_time.replace(tzinfo=timezone.utc)
                rows.append({"latency_ms": latency, "recv_time": recv_time})
            except (ValueError, KeyError):
                continue
    return rows


def load_udid_data(filenames):
    """Load and combine all files for one UDID."""
    all_rows = []
    for filename in sorted(filenames):
        filepath = os.path.join(FOLDER, filename)
        rows = load_single_file(filepath)
        all_rows.extend(rows)
        print(f"    {filename}: {len(rows)} samples")
    all_rows.sort(key=lambda r: r["recv_time"])
    return all_rows


def find_midnight_window(rows):
    """Find a full 24-hr period starting at midnight (local time)."""
    if not rows:
        return [], None

    first_time = rows[0]["recv_time"].astimezone()
    last_time = rows[-1]["recv_time"].astimezone()

    midnight = first_time.replace(hour=0, minute=0, second=0, microsecond=0)
    if midnight < first_time:
        midnight += timedelta(days=1)

    end = midnight + timedelta(hours=24)

    if end > last_time:
        print(f"    WARNING: Data ends before 24hr window completes")

    filtered = [r for r in rows if midnight <= r["recv_time"].astimezone() < end]
    return filtered, midnight


def extract_time_window(rows, midnight, hour, minute):
    """Extract samples within a 30-min window."""
    window_start = midnight + timedelta(hours=hour, minutes=minute)
    window_end = window_start + timedelta(minutes=WINDOW_DURATION_MIN)
    return [r for r in rows if window_start <= r["recv_time"].astimezone() < window_end]


def process_and_save(folder, output_file):
    """Process all raw CSVs, assign windows, save to processed CSV."""
    files = sorted([f for f in os.listdir(folder) if f.endswith(".csv")])
    groups = group_files_by_udid(files)

    print(f"  Files found: {len(files)}")
    print(f"  UDIDs detected: {sorted(groups.keys())}")

    plot_data = []

    for udid in sorted(groups.keys()):
        print(f"\n  Processing UDID: {udid} ({len(groups[udid])} files)")

        all_rows = load_udid_data(groups[udid])
        if not all_rows:
            continue

        day_rows, midnight = find_midnight_window(all_rows)
        print(f"    24hr window: {midnight}")
        print(f"    Samples in window: {len(day_rows)}")

        if not day_rows:
            continue

        # Full 24hr
        for r in day_rows:
            plot_data.append({
                "udid": udid,
                "window": "Full 24hr",
                "latency_ms": r["latency_ms"],
            })

        # Per-window
        for hour, minute, label in TIME_WINDOWS:
            window_rows = extract_time_window(day_rows, midnight, hour, minute)
            for r in window_rows:
                plot_data.append({
                    "udid": udid,
                    "window": label,
                    "latency_ms": r["latency_ms"],
                })

    # Save
    df = pd.DataFrame(plot_data)
    df.to_csv(output_file, index=False)
    print(f"\n  Saved {len(df)} records to {output_file}")
    return df


# ─────────────────────────────────────────────
# Statistics
# ─────────────────────────────────────────────

def compute_stats(samples):
    """Compute summary statistics."""
    if len(samples) < 2:
        return None
    s = sorted(samples)
    n = len(s)
    return {
        "count": n,
        "mean": statistics.mean(s),
        "median": statistics.median(s),
        "stdev": statistics.stdev(s),
        "min": s[0],
        "max": s[-1],
        "p5": s[int(n * 0.05)],
        "p25": s[int(n * 0.25)],
        "p75": s[int(n * 0.75)],
        "p95": s[int(n * 0.95)],
        "p99": s[int(n * 0.99)],
    }


def print_stats(label, stats):
    """Print formatted statistics."""
    print(f"\n  {label}")
    print(f"  {'-' * 50}")
    print(f"  {'Samples:':<12}{stats['count']:>10}")
    print(f"  {'Mean:':<12}{stats['mean']:>10.1f} ms")
    print(f"  {'Median:':<12}{stats['median']:>10.1f} ms")
    print(f"  {'Std Dev:':<12}{stats['stdev']:>10.1f} ms")
    print(f"  {'Min:':<12}{stats['min']:>10.1f} ms")
    print(f"  {'Max:':<12}{stats['max']:>10.1f} ms")
    print(f"  {'P5:':<12}{stats['p5']:>10.1f} ms")
    print(f"  {'P25:':<12}{stats['p25']:>10.1f} ms")
    print(f"  {'P75:':<12}{stats['p75']:>10.1f} ms")
    print(f"  {'P95:':<12}{stats['p95']:>10.1f} ms")
    print(f"  {'P99:':<12}{stats['p99']:>10.1f} ms")


# ─────────────────────────────────────────────
# Plotting
# ─────────────────────────────────────────────

def plot_latency(df):
    """Generate two separate histogram plots."""
    df = df.copy()
    df["latency_plot"] = df["latency_ms"].clip(upper=CLIP_MAX)

    row_order = [label for _, _, label in TIME_WINDOWS]

    # Plot 1: 30-min windows
    time_df = df[df["window"] != "Full 24hr"]
    time_chart = (
        alt.Chart(time_df)
        .mark_bar()
        .encode(
            x=alt.X(
                "latency_plot:Q",
                bin=alt.Bin(step=BIN_SIZE),
                title="Latency (ms)",
                scale=alt.Scale(domain=[0, CLIP_MAX]),
            ),
            y=alt.Y("count()", title="Count"),
        )
        .properties(width=200, height=100)
        .facet(
            row=alt.Row("window:N", sort=row_order, title="Time Window"),
            column=alt.Column("udid:N", title="UDID"),
        )
        .resolve_scale(y="independent")
    )
    time_chart.save("analysis/plots/latency_windows.svg")
    print(f"  30-min windows plot saved to analysis/plots/latency_windows.svg")

    # Plot 2: Full 24hr
    overall_df = df[df["window"] == "Full 24hr"]
    overall_chart = (
        alt.Chart(overall_df)
        .mark_bar()
        .encode(
            x=alt.X(
                "latency_plot:Q",
                bin=alt.Bin(step=BIN_SIZE),
                title="Latency (ms)",
                scale=alt.Scale(domain=[0, CLIP_MAX]),
            ),
            y=alt.Y("count()", title="Count"),
        )
        .properties(width=200, height=100)
        .facet(
            column=alt.Column("udid:N", title="UDID"),
            spacing=0,
        )
    )
    overall_chart.save("analysis/plots/latency_overall.svg")
    print(f"  Full 24hr plot saved to analysis/plots/latency_overall.svg")

    # Report clipping
    n_clipped = (df["latency_ms"] > CLIP_MAX).sum()
    pct_clipped = n_clipped / len(df) * 100
    print(f"  Bin size: {BIN_SIZE} ms")
    print(f"  Last bin ({CLIP_MAX}+ ms) contains {n_clipped} samples ({pct_clipped:.1f}%)")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

if __name__ == "__main__":
    reprocess = "--reprocess" in sys.argv

    print("=" * 60)
    print(f"{'24-HOUR LATENCY ANALYSIS':^60}")
    print("=" * 60)

    # Load from cache or process raw data
    if os.path.exists(PROCESSED_FILE) and not reprocess:
        print(f"\n  Loading cached data: {PROCESSED_FILE}")
        df = pd.read_csv(PROCESSED_FILE)
        print(f"  {len(df)} records loaded")
    else:
        print(f"\n  Processing raw data from {FOLDER}/")
        df = process_and_save(FOLDER, PROCESSED_FILE)
        if df is None or len(df) == 0:
            print("  No data found.")
            exit()

    # Print stats
    for udid in sorted(df["udid"].unique()):
        print(f"\n{'─' * 60}")
        print(f"  UDID: {udid}")
        print(f"{'─' * 60}")

        for window in [label for _, _, label in TIME_WINDOWS] + ["Full 24hr"]:
            samples = df[(df["udid"] == udid) & (df["window"] == window)]["latency_ms"].tolist()
            stats = compute_stats(samples)
            if stats:
                print_stats(window, stats)

    # Plot
    plot_latency(df)
