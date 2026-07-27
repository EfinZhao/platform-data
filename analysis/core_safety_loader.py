"""
Loader for BlueCity Parquet data stored as:
    data/core_safety/<endpoint>/<UDID>/<year>/<month>/<endpoint>_<UDID>_<YYYY-MM-DD>.parquet

Usage:
    from bluecity_loader import load_data
    df = load_data("spd", udids=["12345"], start="2024-01-01", end="2024-01-31")
"""

import datetime as dt
from pathlib import Path

import pandas as pd

DATA_DIR = Path("data/core_safety")
VALID_ENDPOINTS = {"pet", "spd", "tmc"}

# Column in each endpoint's DataFrame that holds the (tz-aware) event time.
# Adjust these if your saved column names differ.
TIME_COL = {
    "tmc": "datetime",        # from flatten_tmc: the TMC "values" key
    "spd": "datetime",        # from flatten_spd: the SPD "values" key
    "pet": "timestamp_local", # PET events carry a local timestamp
}


def _coerce_date(value):
    """Accept a date, datetime, or 'YYYY-MM-DD' string -> datetime.date."""
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])


def _daterange(start, end):
    d = start
    while d <= end:
        yield d
        d += dt.timedelta(days=1)


def _list_udids(endpoint=None):
    """All UDIDs that have a folder on disk.

    With the <endpoint>/<UDID>/... layout, UDID folders live one level
    below each endpoint. If an endpoint is given, list only its UDIDs;
    otherwise, gather the union across all endpoints.
    """
    if not DATA_DIR.exists():
        return []
    endpoints = [endpoint] if endpoint else VALID_ENDPOINTS
    udids = set()
    for ep in endpoints:
        ep_dir = DATA_DIR / ep
        if ep_dir.exists():
            for u in ep_dir.iterdir():
                if u.is_dir():
                    udids.add(u.name)
    return sorted(udids)


def _file_path(endpoint, udid, d):
    return (DATA_DIR / endpoint / str(udid) / f"{d.year:04d}" / f"{d.month:02d}"
            / f"{endpoint}_{udid}_{d.isoformat()}.parquet")


def load_data(api_call, udids=None, start=None, end=None,
              movements=None, tz="US/Pacific", parse_time=True):
    """
    Load BlueCity Parquet data into a single DataFrame.

    Parameters
    ----------
    api_call : str
        One of 'pet', 'spd', 'tmc' (case-insensitive).
    udids : str | list[str] | None
        UDID or list of UDIDs. If None, loads ALL UDIDs found on disk
        for the selected endpoint.
    start, end : str | date | datetime | None
        Inclusive date range. Accepts 'YYYY-MM-DD'. If None, uses the
        earliest / latest available for the selected UDIDs.
    movements : list[str] | None
        Optional filter on turning-movement codes (e.g., ['NS', 'SE']).
        Case-insensitive. If None, keeps all movements.
    tz : str
        Timezone to localize/convert the time column to (default US/Pacific).
    parse_time : bool
        If True, parse the endpoint's time column to tz-aware datetimes,
        sort by it, and set it as the index.

    Returns
    -------
    pandas.DataFrame
    """
    endpoint = api_call.lower().strip()
    if endpoint not in VALID_ENDPOINTS:
        raise ValueError(f"api_call must be one of {sorted(VALID_ENDPOINTS)}")

    # Normalize UDID selection
    if udids is None:
        udids = _list_udids(endpoint)
    elif isinstance(udids, str):
        udids = [udids]
    udids = [str(u) for u in udids]

    # Determine date range
    start = _coerce_date(start) or dt.date(2020, 1, 1)
    end = _coerce_date(end) or dt.date.today()
    if start > end:
        raise ValueError("start date must be <= end date")

    # Gather matching files (skip-if-missing keeps this robust to gaps)
    frames = []
    for udid in udids:
        for d in _daterange(start, end):
            fp = _file_path(endpoint, udid, d)
            if fp.exists():
                df = pd.read_parquet(fp)
                if "UDID" not in df.columns:
                    df["UDID"] = udid   # ensure UDID present for grouping
                frames.append(df)

    if not frames:
        return pd.DataFrame()  # nothing found for the request

    data = pd.concat(frames, ignore_index=True)

    # Optional movement filter (TMC 'movement', SPD 'movement', PET 'turningMovement')
    if movements:
        wanted = {m.lower() for m in movements}
        mv_col = "turningMovement" if endpoint == "pet" else "movement"
        if mv_col in data.columns:
            data = data[data[mv_col].astype(str).str.lower().isin(wanted)]

    # Parse / normalize the time column
    tcol = TIME_COL[endpoint]
    if parse_time and tcol in data.columns:
        ts = pd.to_datetime(data[tcol], utc=True, errors="coerce")
        # Convert to the desired timezone (API can return offset-aware stamps)
        data[tcol] = ts.dt.tz_convert(tz)
        data = data.sort_values(tcol).set_index(tcol)

    return data
