from sqlite3.dbapi2 import DateFromTicks

import pandas as pd
from core_safety_loader import load_data

import altair as alt
alt.renderers.enable("browser")

def metric_total_count(start, end):
    sensors = pd.read_csv("udid.csv")
    if "UDID" not in sensors.columns:
        sys.exit("CSV must contain a 'UDID' column.")
    udids = sensors["UDID"].astype(str).tolist()
    start = start       # start date (YYYY-MM-DD)
    end   = end         # end date (YYYY-MM-DD)

    tmc = load_data("tmc", udids=udids, start=start, end=end)

    if tmc.empty:
        total_counts = 0
        counts_by_class = pd.Series(dtype="float64")
    else:
        # Total across every class and movement
        total_counts = int(tmc["count"].sum())
        # Optional breakdown per vehicle class (useful sanity check)
        counts_by_class = (
            tmc.groupby("vehicle_class")["count"].sum().sort_values(ascending=False)
        )
    print(total_counts, counts_by_class)
    return

def metric_near_miss(start, end):
        sensors = pd.read_csv("udid.csv")
        if "UDID" not in sensors.columns:
            sys.exit("CSV must contain a 'UDID' column.")
        udids = sensors["UDID"].astype(str).tolist()
        start = "2024-05-01"       # start date (YYYY-MM-DD)
        end   = "2026-06-30"       # end date (YYYY-MM-DD)

        pet = load_data("pet", udids=udids, start=start, end=end)

        if pet.empty:
            total_near_misses = 0
        else:
            pet["PETValue"] = pd.to_numeric(pet["PETValue"], errors="coerce")
            total_near_misses = int((pet["PETValue"] <= 2).sum())

        print(f"\nTotal near-misses (PET <= 2s): {total_near_misses:,}")
        return

def plot_counts():
    # load TMC data
    df = load_data(
        "tmc",
        udids="BCT_3D_4G_0207003",
        start="2024-06-01",
        end="2024-06-30"
    )

    # map each 2-letter motorized movement code to (approach, turn type)
    MOVEMENT_MAP = {
        # North approach (southbound)
        "NS": ("N", "Through"), "NE": ("N", "Left"),  "NW": ("N", "Right"),
        # South approach (northbound)
        "SN": ("S", "Through"), "SW": ("S", "Left"),  "SE": ("S", "Right"),
        # East approach (westbound)
        "EW": ("E", "Through"), "ES": ("E", "Left"),  "EN": ("E", "Right"),
        # West approach (eastbound)
        "WE": ("W", "Through"), "WN": ("W", "Left"),  "WS": ("W", "Right"),
    }
    # For cyclists/micromobility: crosswalk crossing == through movement on the perpendicular axis
    CROSSWALK_MAP = {
        "NRL": ("E", "Through"),  # N-leg crosswalk, R->L (E->W)  == East approach through
        "NLR": ("W", "Through"),  # N-leg crosswalk, L->R (W->E)  == West approach through
        "SRL": ("W", "Through"),  # S-leg crosswalk, R->L (W->E)  == West approach through
        "SLR": ("E", "Through"),  # S-leg crosswalk, L->R (E->W)  == East approach through
        "ERL": ("S", "Through"),  # E-leg crosswalk, R->L (S->N)  == South approach through
        "ELR": ("N", "Through"),  # E-leg crosswalk, L->R (N->S)  == North approach through
        "WRL": ("N", "Through"),  # W-leg crosswalk, R->L (N->S)  == North approach through
        "WLR": ("S", "Through"),  # W-leg crosswalk, L->R (S->N)  == South approach through
    }

    FULL_MAP = {**MOVEMENT_MAP, **CROSSWALK_MAP}

    # --- Prepare the data --------------------------------------------------------

    # 1) Exclude pedestrians (keep everything else, incl. micromobility classes)
    df = df[df["vehicle_class"].astype(str).str.lower() != "pedestrian"]

    # 2) Map codes -> (approach, movement). Codes not in FULL_MAP become NaN + dropped.
    codes = df["movement"].astype(str).str.upper()
    df["approach"]  = codes.map(lambda c: FULL_MAP.get(c, (None, None))[0])
    df["turn_type"] = codes.map(lambda c: FULL_MAP.get(c, (None, None))[1])
    df = df.dropna(subset=["approach", "turn_type"])

    # 3) Aggregate
    agg_df = (
        df.groupby(["approach", "turn_type", "vehicle_class"], as_index=False)
          .agg(total_count=("count", "sum"))
    )
    agg_df["total_count"] = agg_df["total_count"].astype(float)

    # --- Chart -------------------------------------------------------------------

    turn_order = ["Left", "Through", "Right", "Crosswalk L→R", "Crosswalk R→L"]
    approach_order = ["N", "S", "E", "W"]

    base = alt.Chart(agg_df).encode(
        x=alt.X("turn_type:N", title="Movement", sort=turn_order),
        xOffset=alt.XOffset("vehicle_class:N"),
        y=alt.Y("total_count:Q", title="Total Count"),
    )

    bars = base.mark_bar().encode(
        color=alt.Color("vehicle_class:N", title="Vehicle Class"),
        tooltip=["approach", "turn_type", "vehicle_class", "total_count"],
    )

    text = base.mark_text(align="center", baseline="bottom", dy=-3, fontSize=9).encode(
        text=alt.Text("total_count:Q", format=".0f"),
    )

    chart = (
        alt.layer(bars, text)
        .properties(width=220, height=300)
        .facet(column=alt.Column("approach:N", title="Approach", sort=approach_order))
        .properties(title="Count totals at Campus and E Peltason from 1-30 June 2024")
    )

    return chart

def main():
    start = "2024-05-01"       # start date (YYYY-MM-DD)
    end   = "2026-06-30"       # end date (YYYY-MM-DD)

    # metric_total_count(start, end)
    metric_near_miss(start, end)

    # counts_chart = plot_counts()
    # counts_chart.save('plots/counts.html')


if __name__ == "__main__":
    main()
