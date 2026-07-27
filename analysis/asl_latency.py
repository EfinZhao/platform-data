import json
import os
import statistics
import sys

import altair as alt
import pandas as pd

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

FOLDER = "/Volumes/Grabbag/ugrad/joshutn3/ASLTests"
PROCESSED_FILE = "analysis/asl_time_processed.csv"
BIN_SIZE = 0.05  # s
CLIP_MAX = 0.6  # s

alt.data_transformers.disable_max_rows()


# ─────────────────────────────────────────────
# Data Loading
# ─────────────────────────────────────────────

def find_jsonl_files(folder):
    """Recursively search all subdirectories for *_full.jsonl files."""
    files = []
    for root, dirs, filenames in os.walk(folder):
        for f in filenames:
            if f.endswith("_full.jsonl"):
                files.append(os.path.join(root, f))
    return sorted(files)


def load_all_asl_times(files):
    """
    Extract ASLTIME from all files into one combined list.
    Key path: line['asl message']['ASLTIME']
    """
    all_values = []

    for filepath in files:
        count = 0
        with open(filepath, "r") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    val = obj["asl message"]["ASLTIME"]
                    all_values.append(float(val))
                    count += 1
                except (json.JSONDecodeError, KeyError, ValueError, TypeError):
                    pass

        rel_path = os.path.relpath(filepath, FOLDER)
        print(f"    {rel_path}: {count} samples")

    print(f"\n  Total samples: {len(all_values)}")
    return all_values


def process_and_save(folder, output_file):
    """Load all JSONL files, extract ASLTIME, save combined to CSV."""
    files = find_jsonl_files(folder)

    if not files:
        print(f"No *_full.jsonl files found in {folder}/")
        return None

    print(f"  Processing {len(files)} files...")
    values = load_all_asl_times(files)

    if not values:
        print("  No ASLTIME values found.")
        return None

    df = pd.DataFrame({"asl_time_ms": values})
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

def plot_asl_times(df):
    """Generate a single histogram of all ASLTIME values combined."""
    df = df.copy()
    df["asl_time_plot"] = df["asl_time_ms"].clip(upper=CLIP_MAX)

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(
                "asl_time_plot:Q",
                bin=alt.Bin(step=BIN_SIZE),
                title="ASL Processing Time (s)",
                scale=alt.Scale(domain=[0, CLIP_MAX]),
            ),
            y=alt.Y("count()", title="Count"),
        )
        .properties(width=500, height=300, title="ASL Request to Response Time Distribution")
    )

    chart.save("analysis/plots/asl_time_analysis.svg")

    n_clipped = (df["asl_time_ms"] > CLIP_MAX).sum()
    pct_clipped = n_clipped / len(df) * 100
    print(f"\n  Plot saved to asl_time_analysis.svg")
    print(f"  Bin size: {BIN_SIZE} ms")
    print(f"  Last bin ({CLIP_MAX}+ ms) contains {n_clipped} samples ({pct_clipped:.1f}%)")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

if __name__ == "__main__":
    reprocess = "--reprocess" in sys.argv

    print("=" * 60)
    print(f"{'ASL PROCESSING TIME ANALYSIS':^60}")
    print("=" * 60)

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

    # Stats
    stats = compute_stats(df["asl_time_ms"].tolist())
    if stats:
        print_stats("ALL DATA COMBINED", stats)

    # Plot
    plot_asl_times(df)
