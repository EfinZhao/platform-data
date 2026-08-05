import csv
from pathlib import Path

import bct_client
from database import get_connection, init_db, update_sensor_status

UDID_CSV = Path(__file__).resolve().parent.parent / "udid.csv"


def load_sensors() -> list[tuple[str, str, str, str]]:
    with open(UDID_CSV, newline="") as f:
        return [
            (row["UDID"], row["major"], row["minor"], row["authority"])
            for row in csv.DictReader(f)
        ]


if __name__ == "__main__":
    sensors = load_sensors()

    init_db()
    with get_connection() as conn:
        conn.executemany(
            "INSERT OR REPLACE INTO sensors (udid, major, minor, authority) VALUES (?, ?, ?, ?)",
            sensors,
        )

    for udid, *_ in sensors:
        update_sensor_status(udid, bct_client.get_status(udid))

    print(f"Seeded {len(sensors)} sensors.")
