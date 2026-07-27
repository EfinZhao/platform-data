import os
import re
import statistics
import sys

import altair as alt
import pandas as pd

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

FOLDER = "cellular_logs/"
PROCESSED_FILE = "analysis/cellular_logs/cellular_latency_parsed.csv"
BIN_SIZE = 5  # ms
CLIP_MAX = 200  # ms

alt.data_transformers.disable_max_rows()


# ─────────────────────────────────────────────
# Data Loading
# ─────────────────────────────────────────────

def parse_latency_file(filepath):
    """
    Parse a log file for lines ending with 'Latency: Xms'.
    Returns list of latency values in ms.
    """
    values = []
    with open(filepath, "r") as f:
        for line in f:
            match = re.search(r"Latency:\s*([\d.]+)\s*ms", line)
            if match:
                values.append(float(match.group(1)))
    return values


def load_all_files(folder):
    """Load all .txt files in folder, combine latency values."""
    all_values = []

    files = sorted([f for f in os.listdir(folder) if f.endswith(".txt")])

    if not files:
        print(f"  No .txt files found in {folder}/")
        return []

    for filename in files:
        filepath = os.path.join(folder, filename)
        values = parse_latency_file(filepath)
        all_values.extend(values)
        print(f"    {filename}: {len(values)} samples")

    print(f"\n  Total samples across all files: {len(all_values)}")
    return all_values


def process_and_save(folder, output_file):
    """Load all log files, save combined to CSV."""
    values = load_all_files(folder)

    if not values:
        return None

    df = pd.DataFrame({"latency_ms": values})
    df.to_csv(output_file, index=False)
    print(f"  Saved {len(df)} records to {output_file}")
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
    print(f"  {'Mean:':<12}{stats['mean']:>10.2f} ms")
    print(f"  {'Median:':<12}{stats['median']:>10.2f} ms")
    print(f"  {'Std Dev:':<12}{stats['stdev']:>10.2f} ms")
    print(f"  {'Min:':<12}{stats['min']:>10.2f} ms")
    print(f"  {'Max:':<12}{stats['max']:>10.2f} ms")
    print(f"  {'P5:':<12}{stats['p5']:>10.2f} ms")
    print(f"  {'P25:':<12}{stats['p25']:>10.2f} ms")
    print(f"  {'P75:':<12}{stats['p75']:>10.2f} ms")
    print(f"  {'P95:':<12}{stats['p95']:>10.2f} ms")
    print(f"  {'P99:':<12}{stats['p99']:>10.2f} ms")


# ─────────────────────────────────────────────
# Plotting
# ─────────────────────────────────────────────

def plot_latency(df):
    """Generate histogram and time series."""
    df = df.copy()
    df["latency_plot"] = df["latency_ms"].clip(upper=CLIP_MAX)
    df["sample_num"] = range(len(df))

    # Histogram
    hist = (
        alt.Chart(df)
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
        .properties(width=500, height=250, title="Cellular Latency Distribution")
    )

    # Time series
    timeseries = (
        alt.Chart(df)
        .mark_line(strokeWidth=0.5)
        .encode(
            x=alt.X("sample_num:Q", title="Sample #"),
            y=alt.Y("latency_ms:Q", title="Latency (ms)"),
        )
        .properties(width=500, height=200, title="Latency Over Time")
    )

    chart = alt.vconcat(hist, timeseries)
    chart.save("cellular_latency_analysis.svg")

    n_clipped = (df["latency_ms"] > CLIP_MAX).sum()
    pct_clipped = n_clipped / len(df) * 100
    print(f"\n  Plot saved to cellular_latency_analysis.svg")
    print(f"  Bin size: {BIN_SIZE} ms")
    print(f"  Last bin ({CLIP_MAX}+ ms) contains {n_clipped} samples ({pct_clipped:.1f}%)")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

if __name__ == "__main__":
    reprocess = "--reprocess" in sys.argv

    print("=" * 60)
    print(f"{'CELLULAR LATENCY ANALYSIS':^60}")
    print("=" * 60)

    if os.path.exists(PROCESSED_FILE) and not reprocess:
        print(f"\n  Loading cached data: {PROCESSED_FILE}")
        df = pd.read_csv(PROCESSED_FILE)
        print(f"  {len(df)} records loaded")
    else:
        print(f"\n  Processing logs from {FOLDER}/")
        df = process_and_save(FOLDER, PROCESSED_FILE)
        if df is None or len(df) == 0:
            print("  No data found.")
            exit()

    print(f"  Total samples: {len(df)}")

    # Stats
    stats = compute_stats(df["latency_ms"].tolist())
    if stats:
        print_stats("CELLULAR LATENCY", stats)

    # Plot
    plot_latency(df)
