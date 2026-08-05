import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

import database
import requests
from config import BCT_BEACON_ADDRESS, BCT_PASSWORD, BCT_USERNAME

_LOGGING_DIR = str(Path(__file__).resolve().parent.parent / "logging")
if _LOGGING_DIR not in sys.path:
    sys.path.insert(0, _LOGGING_DIR)

import logger_rt  # path must be set up before this import

AUTH_URL = "https://core.api.bluecity.ai/api/token/"
FRESHNESS_TTL_S = 1800  # how long a last-seen message still counts as "reporting"


def _get_token() -> str | None:
    try:
        res = requests.post(
            AUTH_URL,
            json={"username": BCT_USERNAME, "password": BCT_PASSWORD},
            timeout=5,
        )
        if res.status_code == 200:
            return res.json()["access"]
    except requests.RequestException:
        pass
    return None


def get_status(udid: str) -> bool:
    token = _get_token()
    if token is None:
        raise RuntimeError("Authentication failed — check BCT credentials")
    try:
        res = requests.post(
            BCT_BEACON_ADDRESS + "route",
            json={"UDID": udid, "type": "2", "token": token},
            timeout=5,
        )
        return res.status_code == 200
    except requests.RequestException:
        return False


def _fresh(ts) -> bool:
    if ts is None:
        return False
    return (datetime.now(timezone.utc) - ts).total_seconds() <= FRESHNESS_TTL_S


def _check_and_update_sync(udid: str):
    status = get_status(udid)
    frame_status = False
    phase_status = False
    ttc_status = False

    if status:
        lgr = logger_rt.RUNNING_LOGGERS.get(udid)
        if lgr is None:
            print(f"[ERROR] {udid}: no running logger — sensor not tracked by the ingest side")
        else:
            frame_status = _fresh(lgr.last_frame_ts)
            phase_status = _fresh(lgr.last_phase_ts)
            ttc_status = _fresh(lgr.last_ttc_ts)

    database.update_sensor_status(udid, status, frame_status, phase_status, ttc_status)


async def check_and_update(udid: str):
    await asyncio.to_thread(lambda: _check_and_update_sync(udid))
