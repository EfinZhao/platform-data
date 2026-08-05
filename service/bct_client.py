import asyncio
import queue
import time
from datetime import datetime

import database
import requests
from google.protobuf.json_format import MessageToDict
from realtime_subscriber.Realtime_subscriber_api import BCTWSConnection

from config import BCT_USERNAME, BCT_PASSWORD, BCT_BEACON_ADDRESS

AUTH_URL = "https://core.api.bluecity.ai/api/token/"
FRAME_TIMEOUT = 10  # seconds to wait before declaring the sensor unreachable
PHASE_CACHE_TTL = 1800

_streams: dict[str, BCTWSConnection] = {}
_frame_cache: dict[str, dict] = {}


_SUBSCRIPTIONS = [
    BCTWSConnection.subscriptionOption.FRAME,
    BCTWSConnection.subscriptionOption.PHASE_CHANGE,
    BCTWSConnection.subscriptionOption.PHASE_TIME_TO_CHANGE,
]


def _get_or_create_stream(udid: str) -> BCTWSConnection:
    if udid not in _streams:
        _streams[udid] = BCTWSConnection(
            UDID=udid,
            username=BCT_USERNAME,
            password=BCT_PASSWORD,
            beaconAddress=BCT_BEACON_ADDRESS,
            singleton=True,
            subscriptions=_SUBSCRIPTIONS,
        )
    return _streams[udid]


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


def _check_and_update_sync(udid: str):
    status = get_status(udid)
    frame_status = False
    phase_status = False
    ttc_status = False

    if status:
        try:
            frame_data = get_frame(udid)
            frame_status = bool(frame_data.get("frame"))
            phase_status = bool(_is_phase_change_valid(frame_data.get("phase_change", {})))
            ttc_status = bool(frame_data.get("phase_time_to_change"))
        except Exception as e:
            print(f"[ERROR] {udid}: {type(e).__name__}: {e}")

    database.update_sensor_status(udid, status, frame_status, phase_status, ttc_status)


def _latest_phase_ts(phase_dict: dict) -> float:
    timestamps = [p["timestamp"] for p in phase_dict.get("phases", []) if "timestamp" in p]
    if not timestamps:
        return 0.0
    return datetime.fromisoformat(max(timestamps)).timestamp()


def _is_phase_change_valid(data):
    if not data.get("phases"):
        return False
    for phase in data["phases"]:
        if int(phase["phaseNumber"]) > 8:
            break
        if phase.get("status") == 999:
            return False
    return True


async def check_and_update(udid: str):
    await asyncio.to_thread(lambda: _check_and_update_sync(udid))


def get_frame(udid: str) -> dict:
    stream = _get_or_create_stream(udid)

    try:
        data = stream.queue.get(timeout=FRAME_TIMEOUT)
    except queue.Empty:
        raise TimeoutError(
            f"Sensor {udid} did not respond within {FRAME_TIMEOUT}s — beacon may be unreachable"
        )

    cache = _frame_cache.setdefault(udid, {
        "frame": {},
        "phase_change": {},
        "phase_time_to_change": {},
    })

    def _update_cache(msg):
        frame_dict = MessageToDict(msg.frame)
        phase_dict = MessageToDict(msg.phaseChange)
        ttc_dict = MessageToDict(msg.phaseTimeToChange)
        if frame_dict:
            cache["frame"] = frame_dict
        if phase_dict.get("phases") and phase_dict.get("absolute"):
            cache["phase_change"] = phase_dict
        if ttc_dict.get("phases"):
            cache["phase_time_to_change"] = ttc_dict

    _update_cache(data)
    while True:
        try:
            _update_cache(stream.queue.get_nowait())
        except queue.Empty:
            break

    phase_fresh = time.time() - _latest_phase_ts(cache["phase_change"]) <= PHASE_CACHE_TTL
    return {
        "frame": cache["frame"],
        "phase_change": cache["phase_change"] if phase_fresh else {},
        "phase_time_to_change": cache["phase_time_to_change"] if phase_fresh else {},
    }
