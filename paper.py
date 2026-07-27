# %% Imports & renderer
import os

import altair as alt
import numpy as np
import pandas as pd

alt.renderers.enable("browser")
alt.data_transformers.disable_max_rows()


# configuration and constants

GPS_BOUNDS = {
    "lat_min": 33.6308,
    "lat_max": 33.6683,
    "lon_min": -117.8556,
    "lon_max": -117.8204,
}

COL_TIME = "time"
COL_SPEED = "avg_vehicle_speed_kmh"
COL_POWER = "power_W"
COL_LAT = "phn_GPS_lat_deg"
COL_LON = "phn_GPS_lng_deg"
COL_TSTEP = "timestep_s"
COL_DIST = "dist_m"
COL_ENERGY = "eng_kWh"

MAX_GAP_S = 10  # timesteps larger than this (s) zero out dist/energy
IDLE_SPEED_KMH = 0.5  # speed threshold below which vehicle is considered stopped


# data processing
def filter_time(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    """Slice dataframe to a time window [start, end)."""
    return df[(df[COL_TIME] >= start) & (df[COL_TIME] < end)].copy()


# summary statistics
def compute_summary(df: pd.DataFrame, label: str = "") -> dict:
    """
    Compute key summary metrics for a vehicle or fleet DataFrame.

    Returns a dict suitable for building a summary table.
    """
    dist_km = df[COL_DIST].sum() / 1_000
    energy_kWh = df[COL_ENERGY].sum()
    avg_speed = df[COL_SPEED].mean()
    stopped_s = df.loc[
        (df[COL_POWER] == 0) & (df[COL_SPEED] <= IDLE_SPEED_KMH), COL_TSTEP
    ].sum()

    return {
        "label": label,
        "distance_km": dist_km,
        "energy_kWh": energy_kWh,
        "efficiency_kWh_km": energy_kWh / dist_km if dist_km > 0 else float("nan"),
        "avg_speed_kmh": avg_speed,
        "time_stopped_s": stopped_s,
    }


def build_summary_table(vehicle_dfs: dict) -> pd.DataFrame:
    """
    Return a tidy DataFrame of summary stats for all vehicles
    plus EV6 fleet, iQ EV fleet, and all-vehicles totals.
    """
    rows = [compute_summary(df, label=vid) for vid, df in vehicle_dfs.items()]

    ev6_df = pd.concat([vehicle_dfs[k] for k in ["ev133", "ev136", "ev140"]])
    iq_df = pd.concat([vehicle_dfs[k] for k in ["iq676", "iq677", "iq681"]])
    all_df = pd.concat([ev6_df, iq_df])

    rows += [
        compute_summary(ev6_df, "KIA EV6 Fleet"),
        compute_summary(iq_df, "Scion iQ EV Fleet"),
        compute_summary(all_df, "All Vehicles"),
    ]
    return pd.DataFrame(rows).set_index("label")


# plotting


def plot_speed_time(df: pd.DataFrame, title: str = "Speed vs. Time") -> alt.Chart:
    """Interactive line chart: speed over time, colored by vehicle."""
    return (
        alt.Chart(df, title=title)
        .mark_line(strokeWidth=1, opacity=0.8)
        .encode(
            x=alt.X(
                f"{COL_TIME}:T",
                title="Time",
                axis=alt.Axis(format="%m/%d %H:%M", labelAngle=-30),
            ),
            y=alt.Y(
                f"{COL_SPEED}:Q", title="Speed (km/h)", scale=alt.Scale(domainMin=0)
            ),
            color=alt.Color("vehicle_id:N", title="Vehicle"),
            tooltip=[
                alt.Tooltip(f"{COL_TIME}:T", title="Time", format="%Y-%m-%d %H:%M:%S"),
                alt.Tooltip(f"{COL_SPEED}:Q", title="Speed (km/h)", format=".1f"),
                alt.Tooltip("vehicle_id:N", title="Vehicle"),
            ],
        )
        .properties(width=720, height=350)
        .interactive()
    )


def plot_power_time(df: pd.DataFrame, title: str = "Power vs. Time") -> alt.Chart:
    """Interactive line chart: traction power over time, colored by vehicle."""
    return (
        alt.Chart(df, title=title)
        .mark_line(strokeWidth=1, opacity=0.8)
        .encode(
            x=alt.X(
                f"{COL_TIME}:T",
                title="Time",
                axis=alt.Axis(format="%m/%d %H:%M", labelAngle=-30),
            ),
            y=alt.Y(f"{COL_POWER}:Q", title="Power (W)"),
            color=alt.Color("vehicle_id:N", title="Vehicle"),
            tooltip=[
                alt.Tooltip(f"{COL_TIME}:T", title="Time", format="%Y-%m-%d %H:%M:%S"),
                alt.Tooltip(f"{COL_POWER}:Q", title="Power (W)", format=".0f"),
                alt.Tooltip("vehicle_id:N", title="Vehicle"),
            ],
        )
        .properties(width=720, height=350)
        .interactive()
    )


def plot_gps_speed(df: pd.DataFrame, title: str = "GPS Track — Speed") -> alt.Chart:
    """
    GPS scatter plot colored by speed.

    Note: Altair does not natively support tile map backgrounds.
    For tiled maps (OSM / Carto), use a Vega spec via alt.Chart.from_dict()
    or the altair_tiles package. This plot uses a plain lat/lon coordinate
    system with equal-scale axes to preserve geographic proportions.
    """
    lon_range = [GPS_BOUNDS["lon_min"], GPS_BOUNDS["lon_max"]]
    lat_range = [GPS_BOUNDS["lat_min"], GPS_BOUNDS["lat_max"]]

    return (
        alt.Chart(df, title=title)
        .mark_circle(size=5, opacity=0.7)
        .encode(
            x=alt.X(
                f"{COL_LON}:Q",
                title="Longitude",
                scale=alt.Scale(domain=lon_range, zero=False),
            ),
            y=alt.Y(
                f"{COL_LAT}:Q",
                title="Latitude",
                scale=alt.Scale(domain=lat_range, zero=False),
            ),
            color=alt.Color(
                f"{COL_SPEED}:Q",
                title="Speed (km/h)",
                scale=alt.Scale(scheme="viridis", domainMin=0, domainMax=80),
            ),
            tooltip=[
                alt.Tooltip(f"{COL_LAT}:Q", title="Lat", format=".5f"),
                alt.Tooltip(f"{COL_LON}:Q", title="Lon", format=".5f"),
                alt.Tooltip(f"{COL_SPEED}:Q", title="Speed (km/h)", format=".1f"),
                alt.Tooltip(f"{COL_POWER}:Q", title="Power (W)", format=".0f"),
                alt.Tooltip("vehicle_id:N", title="Vehicle"),
                alt.Tooltip(f"{COL_TIME}:T", title="Time", format="%Y-%m-%d %H:%M:%S"),
            ],
        )
        .properties(width=600, height=520)
        .interactive()
    )


def plot_efficiency_bar(summary_df: pd.DataFrame) -> alt.Chart:
    """Bar chart of energy efficiency per vehicle and fleet group."""
    df = summary_df.reset_index()
    return (
        alt.Chart(df, title="Energy Efficiency by Vehicle / Fleet")
        .mark_bar()
        .encode(
            x=alt.X("label:N", title=None, sort=None, axis=alt.Axis(labelAngle=-30)),
            y=alt.Y("efficiency_kWh_km:Q", title="Efficiency (kWh/km)"),
            color=alt.Color("label:N", legend=None),
            tooltip=[
                alt.Tooltip("label:N", title="Vehicle"),
                alt.Tooltip("efficiency_kWh_km:Q", title="kWh/km", format=".4f"),
                alt.Tooltip("distance_km:Q", title="Dist (km)", format=".2f"),
                alt.Tooltip("energy_kWh:Q", title="Energy (kWh)", format=".2f"),
                alt.Tooltip("avg_speed_kmh:Q", title="Avg spd (km/h)", format=".1f"),
            ],
        )
        .properties(width=600, height=350)
    )


def plot_speed_histogram(
    df: pd.DataFrame, title: str = "Speed Distribution", n_bins: int = 60
) -> alt.Chart:
    """
    Pre-aggregate speed data into bins using pandas before passing to Altair.
    This keeps the HTML output small regardless of input dataset size.
    """
    # Clean input
    speed = df[[COL_SPEED, "make_model"]].dropna()
    speed = speed[np.isfinite(speed[COL_SPEED])]

    # Bin with pandas
    speed["speed_bin"] = pd.cut(speed[COL_SPEED], bins=n_bins)

    # Aggregate to counts per bin per make_model
    hist_data = (
        speed.groupby(["speed_bin", "make_model"], observed=True)
        .size()
        .reset_index(name="count")
    )

    # Extract bin edges for Altair's binned bar encoding
    hist_data["speed_left"] = hist_data["speed_bin"].apply(lambda b: b.left)
    hist_data["speed_right"] = hist_data["speed_bin"].apply(lambda b: b.right)
    hist_data = hist_data.drop(columns="speed_bin")

    return (
        alt.Chart(hist_data, title=title)
        .mark_bar(opacity=0.5, binSpacing=0)
        .encode(
            x=alt.X(
                "speed_left:Q", title="Speed (km/h)", bin=alt.BinParams(binned=True)
            ),
            x2=alt.X2("speed_right:Q"),
            y=alt.Y("count:Q", title="Count", stack=None),
            column=alt.Column("make_model:N"),
            color=alt.Color("make_model:N", title="Make/Model"),
            tooltip=[
                alt.Tooltip("make_model:N", title="Make/Model"),
                alt.Tooltip("speed_left:Q", title="Bin start (km/h)", format=".1f"),
                alt.Tooltip("speed_right:Q", title="Bin end (km/h)", format=".1f"),
                alt.Tooltip("count:Q", title="Count", format=","),
            ],
        )
        .properties(width=600, height=300)
    )


# %% Main Workflow

# load pre-processed fleet Parquet files
ev_fleet = pd.read_parquet("fleet/ev_fleet.parquet")
iq_fleet = pd.read_parquet("fleet/iq_fleet.parquet")
all_fleet = pd.read_parquet("fleet/all_fleet.parquet")

# reconstruct per-vehicle DataFrames by filtering on vehicle_id
vehicles = {
    vid: all_fleet[all_fleet["vehicle_id"] == vid]
    for vid in all_fleet["vehicle_id"].unique()
}

# vehicle aliases
ev133, ev136, ev140 = vehicles["ev133"], vehicles["ev136"], vehicles["ev140"]
iq676, iq677, iq681 = vehicles["iq676"], vehicles["iq677"], vehicles["iq681"]

# summary statistics
summary = build_summary_table(vehicles)
print(summary.to_string())

# plot_speed_time(ev_fleet, title="KIA EV6 Fleet — Speed vs. Time").show()
# plot_speed_time(iq_fleet,  title="Scion iQ EV Fleet — Speed vs. Time").show()
# plot_power_time(all_fleet, title="All Vehicles — Power vs. Time").show()
# plot_gps_speed(
#     all_fleet.sample(frac=0.01, random_state=42), title="All Vehicles — GPS Track"
# ).show()
# plot_efficiency_bar(summary).show()
# plot_speed_histogram(all_fleet, title="Speed Distribution by Make/Model").show()

# 5. Example: single-vehicle time window drill-down
ev133_slice = filter_time(ev133, "2024-02-27T13:25:00", "2024-02-27T13:45:00")
plot_speed_time(ev133_slice, title="ev133 — 2024-02-27 13:25–13:45").show()
plot_gps_speed(
    ev133_slice.sample(frac=0.001, random_state=42),
    title="ev133 — GPS Track 2024-02-27 13:25–13:45",
).show()
print(compute_summary(ev133_slice, label="ev133 slice"))
