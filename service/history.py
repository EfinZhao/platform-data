import json
from datetime import datetime

import database
import emailer

RETENTION_OPTIONS: frozenset[int] = frozenset({24, 96, 192, 480, 672})  # 6h 24h 2d 5d 1w
DEFAULT_RETENTION = 96
RETENTION_KEY = "retention_entries"

_SERVICE_FIELDS: list[tuple[str, str]] = [
    ("frame_status", "Frame"),
    ("phase_status", "Phase Change"),
    ("ttc_status", "Time to Change"),
]


def get_retention() -> int:
    raw = database.get_setting(RETENTION_KEY)
    if raw is not None:
        try:
            n = int(raw)
            if n in RETENTION_OPTIONS:
                return n
        except ValueError:
            pass
    return DEFAULT_RETENTION


def set_retention(n: int) -> None:
    if n not in RETENTION_OPTIONS:
        raise ValueError(
            f"Invalid retention {n}. Must be one of: {sorted(RETENTION_OPTIONS)}"
        )
    database.set_setting(RETENTION_KEY, str(n))


def build_snapshot() -> dict:
    with database.get_connection() as conn:
        rows = conn.execute(
            "SELECT udid, major, minor, status, frame_status, phase_status, ttc_status"
            " FROM sensors"
        ).fetchall()

    return {
        row["udid"]: {
            "name": f"{row['major']}_{row['minor']}",
            "status": row["status"],
            "frame_status": row["frame_status"],
            "phase_status": row["phase_status"],
            "ttc_status": row["ttc_status"],
        }
        for row in rows
    }


def diff_snapshots(prev2: dict, prev: dict, curr: dict) -> dict:
    sensor_up: list[dict] = []
    sensor_down: list[dict] = []
    service_up: list[dict] = []
    service_down: list[dict] = []

    common = set(prev2) & set(prev) & set(curr)

    for udid in common:
        s2 = prev2[udid]
        s1 = prev[udid]
        s0 = curr[udid]
        name = s0["name"]

        if s2["status"] == 1 and s1["status"] == 0 and s0["status"] == 0:
            sensor_down.append({"udid": udid, "name": name})
        elif s2["status"] == 0 and s1["status"] == 1 and s0["status"] == 1:
            sensor_up.append({"udid": udid, "name": name})

        if s2["status"] == 1 and s1["status"] == 1 and s0["status"] == 1:
            for field, label in _SERVICE_FIELDS:
                v2, v1, v0 = s2[field], s1[field], s0[field]
                if v2 == 1 and v1 == 0 and v0 == 0:
                    service_down.append({"udid": udid, "name": name, "service": label})
                elif v2 == 0 and v1 == 1 and v0 == 1:
                    service_up.append({"udid": udid, "name": name, "service": label})

    return {
        "sensor_up": sensor_up,
        "sensor_down": sensor_down,
        "service_up": service_up,
        "service_down": service_down,
    }


def record_and_alert() -> None:
    curr = build_snapshot()
    recent = database.get_recent_snapshots(2)
    database.insert_and_trim_snapshot(
        datetime.now().isoformat(), json.dumps(curr), get_retention()
    )

    if len(recent) < 2:
        return

    prev = json.loads(recent[0]["data"])
    prev2 = json.loads(recent[1]["data"])

    changes = diff_snapshots(prev2, prev, curr)
    if any(changes.values()):
        emailer.send_change_alert(changes)
