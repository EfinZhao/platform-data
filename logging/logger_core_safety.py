#!/usr/bin/env python3
"""
scrape.py — Bulk-download BlueCity Analytics data (TMC, avg speed, PET)
for all sensors listed in a UDID CSV, at the highest resolution, lossless,
stored as Zstandard-compressed Parquet.

Endpoints (per BlueCity Analytics API Integration Guide):
  - TMC : GET https://core.api.bluecity.ai/api/v2/turningmovement/
  - SPD : GET https://safety.api.bluecity.ai/api/analytics/avgspeed
  - PET : GET https://safety.api.bluecity.ai/api/analytics/PET
All analytics endpoints cap each request at a 24-hour range, so we page day-by-day.
"""

import base64
import datetime as dt
import json
import os
import sys
import time
import tomllib
from pathlib import Path

import pandas as pd
import requests

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------
with open(".secrets.toml", "rb") as file:
    secrets = tomllib.load(file)

USERNAME = secrets["username"]
PASSWORD = secrets["password"]

UDID_CSV = "../udid.csv"  # headers: UDID,major,minor,authority,lat,lon,rotation
OUTPUT_DIR = Path("data/core_safety")
TIMEZONE = "US/Pacific"

# Go as far back as requested (older than data likely exists; empty days are skipped).
START_DATE = dt.date(2024, 8, 1)
END_DATE = dt.date.today()  # inclusive up to today

AGGREGATION = 1  # 1 = 15-minute resolution (highest for TMC & SPD)
SIMPLIFIED = False  # False = TMC counts categorized by vehicle class (all fields)
# PETMargin: left as API default (10). Set to a value if you want to override.
PET_MARGIN = None

# Endpoints
TOKEN_URL = "https://core.api.bluecity.ai/api/token/"
TOKEN_REFRESH_URL = "https://core.api.bluecity.ai/api/token/refresh/"
TMC_URL = "https://core.api.bluecity.ai/api/v2/turningmovement/"
SPD_URL = "https://safety.api.bluecity.ai/api/analytics/avgspeed"
PET_URL = "https://safety.api.bluecity.ai/api/analytics/PET"

# HTTP behavior
MAX_RETRIES = 5
BACKOFF_BASE = 2.0  # seconds
REQUEST_TIMEOUT = 60  # seconds
PARQUET_COMPRESSION = "zstd"


# ----------------------------------------------------------------------------
# Authentication / token handling
# ----------------------------------------------------------------------------
class TokenManager:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.access = None
        self.refresh = None
        self._login()

    def _login(self):
        r = requests.post(
            TOKEN_URL,
            json={"username": self.username, "password": self.password},
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        self.access = data["access"]
        self.refresh = data.get("refresh")
        print(f"[auth] Obtained token; expires {self._expiry(self.access)}")

    def _refresh_token(self):
        if not self.refresh:
            self._login()
            return
        try:
            r = requests.post(
                TOKEN_REFRESH_URL,
                json={"refresh": self.refresh},
                timeout=REQUEST_TIMEOUT,
            )
            r.raise_for_status()
            self.access = r.json()["access"]
            print(f"[auth] Refreshed token; expires {self._expiry(self.access)}")
        except Exception as e:
            print(f"[auth] Refresh failed ({e}); re-logging in.")
            self._login()

    @staticmethod
    def _expiry(token):
        try:
            payload = token.split(".")[1]
            payload += "=" * (-len(payload) % 4)  # pad base64
            body = json.loads(base64.urlsafe_b64decode(payload))
            if "exp" in body:
                return dt.datetime.utcfromtimestamp(body["exp"]).isoformat() + "Z"
        except Exception:
            pass
        return "unknown"

    def header(self):
        return {"Authorization": f"Bearer {self.access}"}


# ----------------------------------------------------------------------------
# HTTP GET with retry + auto token refresh
# ----------------------------------------------------------------------------
def api_get(url, params, tokens):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(
                url, params=params, headers=tokens.header(), timeout=REQUEST_TIMEOUT
            )
            if r.status_code == 401:
                print("[http] 401 Unauthorized -> refreshing token")
                tokens._refresh_token()
                continue
            if r.status_code == 429 or r.status_code >= 500:
                wait = BACKOFF_BASE**attempt
                print(f"[http] {r.status_code} on {url}; retry in {wait:.0f}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            if not r.text.strip():
                return None
            return r.json()
        except requests.RequestException as e:
            wait = BACKOFF_BASE**attempt
            print(f"[http] error {e}; retry {attempt}/{MAX_RETRIES} in {wait:.0f}s")
            time.sleep(wait)
    print(f"[http] FAILED after {MAX_RETRIES} attempts: {url} {params}")
    return None


# ----------------------------------------------------------------------------
# Response flattening (lossless, long format) -> DataFrame
# ----------------------------------------------------------------------------
def flatten_tmc(resp):
    """TMC values: {datetime: {VehicleClass: {movement: count}}} (simplified=false)."""
    rows = []
    if not resp:
        return pd.DataFrame()
    udid = resp.get("UDID")
    agg = resp.get("aggregation")
    for ts, classes in (resp.get("values") or {}).items():
        if isinstance(classes, dict):
            for vclass, movements in classes.items():
                if isinstance(movements, dict):
                    for movement, count in movements.items():
                        rows.append(
                            {
                                "UDID": udid,
                                "aggregation": agg,
                                "datetime": ts,
                                "vehicle_class": vclass,
                                "movement": movement,
                                "count": count,
                            }
                        )
                else:
                    rows.append(
                        {
                            "UDID": udid,
                            "aggregation": agg,
                            "datetime": ts,
                            "vehicle_class": vclass,
                            "movement": None,
                            "count": movements,
                        }
                    )
    return pd.DataFrame(rows)


def flatten_spd(resp):
    """SPD values: {datetime: {movement: speed_kmh}}."""
    rows = []
    if not resp:
        return pd.DataFrame()
    udid = resp.get("UDID")
    agg = resp.get("aggregation")
    for ts, movements in (resp.get("values") or {}).items():
        if isinstance(movements, dict):
            for movement, speed in movements.items():
                rows.append(
                    {
                        "UDID": udid,
                        "aggregation": agg,
                        "datetime": ts,
                        "movement": movement,
                        "speed_kmh": speed,
                    }
                )
    return pd.DataFrame(rows)


def flatten_pet(resp):
    """PET is a list of event dicts; keep every field as-is."""
    if not resp:
        return pd.DataFrame()
    if isinstance(resp, list):
        return pd.DataFrame(resp)
    if isinstance(resp, dict) and "results" in resp:
        return pd.DataFrame(resp["results"])
    return pd.DataFrame([resp])


# ----------------------------------------------------------------------------
# Per-day fetch wrappers
# ----------------------------------------------------------------------------
def daterange(start, end):
    d = start
    while d <= end:
        yield d
        d += dt.timedelta(days=1)


def day_bounds(d):
    """Local-day ISO 8601 start/end strings (00:00:00 to 23:59:59)."""
    fdate = f"{d.isoformat()}T00:00:00"
    tdate = f"{d.isoformat()}T23:59:59"
    return fdate, tdate


def fetch_tmc(udid, d, tokens):
    fdate, tdate = day_bounds(d)
    params = {
        "UDID": udid,
        "fdate": fdate,
        "tdate": tdate,
        "a": AGGREGATION,
        "simplified": str(SIMPLIFIED).lower(),
        "timezone": TIMEZONE,
    }
    # 'f' omitted -> defaults to all movements
    return flatten_tmc(api_get(TMC_URL, params, tokens))


def fetch_spd(udid, d, tokens):
    fdate, tdate = day_bounds(d)
    params = {
        "UDID": udid,
        "fdate": fdate,
        "tdate": tdate,
        "aggregation": AGGREGATION,
        "timezone": TIMEZONE,
    }
    # 'turningMovement' omitted -> all movements
    return flatten_spd(api_get(SPD_URL, params, tokens))


def fetch_pet(udid, d, tokens):
    fdate, tdate = day_bounds(d)
    params = {"UDID": udid, "fdate": fdate, "tdate": tdate, "timezone": TIMEZONE}
    if PET_MARGIN is not None:
        params["PETMargin"] = PET_MARGIN
    # 'turningMovement' omitted -> all movements; PET has no aggregation (raw events)
    return flatten_pet(api_get(PET_URL, params, tokens))


# ----------------------------------------------------------------------------
# Output path + write (resumable: skip existing files)
# ----------------------------------------------------------------------------
def out_path(endpoint, udid, d):
    p = OUTPUT_DIR / endpoint / str(udid) / f"{d.year:04d}" / f"{d.month:02d}"
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{endpoint}_{udid}_{d.isoformat()}.parquet"


def write_parquet(df, path):
    if df is None or df.empty:
        return False
    df.to_parquet(path, compression=PARQUET_COMPRESSION, index=False)
    return True


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main():
    if not USERNAME or not PASSWORD:
        sys.exit("Set USERNAME and PASSWORD vars.")

    sensors = pd.read_csv(UDID_CSV)
    if "UDID" not in sensors.columns:
        sys.exit("CSV must contain a 'UDID' column.")
    udids = sensors["UDID"].astype(str).tolist()
    print(f"Loaded {len(udids)} sensors from {UDID_CSV}")

    tokens = TokenManager(USERNAME, PASSWORD)

    fetchers = {"tmc": fetch_tmc, "spd": fetch_spd, "pet": fetch_pet}

    for udid in udids:
        print(f"\n=== Sensor {udid} ===")
        for d in daterange(START_DATE, END_DATE):
            for endpoint, fetch in fetchers.items():
                path = out_path(endpoint, udid, d)
                if path.exists():
                    continue  # resumable: already downloaded
                df = fetch(udid, d, tokens)
                if write_parquet(df, path):
                    print(f"[ok] {endpoint} {udid} {d} -> {len(df)} rows")
                # else: no data that day; nothing written
    print("\nDone.")


if __name__ == "__main__":
    main()
