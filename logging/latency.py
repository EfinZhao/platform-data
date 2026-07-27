import csv
import threading
import tomllib
from datetime import datetime, timedelta, timezone

from realtime_subscriber.Realtime_subscriber_api import BCTWSConnection

UDID_LIST = [
    "BCT_3D_4G_0207025", # E Peltason / Anteater (UCI cell Puck)
    "OBC-AG-SA-C251065", # University / Harvard (COI hardwire OS)
    "BCT_3D_4G_0207055", # Culver / University (COI hardwire Puck)
    "BCT_3D_4G_0207012", # Culver / Harvard (COI cell Puck)
]

FLUSH_INTERVAL = 60  # seconds

with open(".secrets.toml", "rb") as file:
    secrets = tomllib.load(file)


def run_logger(udid):
    """Run a latency logger for a single UDID. One per thread."""

    buffer = []
    total_samples = 0
    last_flush = datetime.now()
    start_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    output_file = f"data/latency_logs/latency_log_{udid}_{start_timestamp}.csv"

    # Write header
    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["latency_ms", "recv_time", "msg_timestamp"])

    def flush_if_ready():
        nonlocal last_flush, total_samples
        now = datetime.now()
        if now - last_flush >= timedelta(seconds=FLUSH_INTERVAL) and len(buffer) > 0:
            with open(output_file, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(buffer)
            total_samples += len(buffer)
            print(f"[{udid}] Total samples: {total_samples}")
            buffer.clear()
            last_flush = now

    def final_flush():
        nonlocal total_samples
        if len(buffer) > 0:
            with open(output_file, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(buffer)
            total_samples += len(buffer)
            print(f"[{udid}] Final total: {total_samples} samples saved")
            buffer.clear()

    stream = BCTWSConnection(
        UDID=udid,
        username=secrets["username"],
        password=secrets["password"],
        beaconAddress="https://realtime.us.beacon.1.api.bluecity.ai/",
        singleton=True,
        subscriptions=[
            BCTWSConnection.subscriptionOption.FRAME,
            BCTWSConnection.subscriptionOption.PHASE_CHANGE,
            BCTWSConnection.subscriptionOption.PHASE_TIME_TO_CHANGE,
        ],
    )

    print(f"[{udid}] Started logging to {output_file}")

    try:
        while True:
            recv_time = datetime.now(timezone.utc)

            data = stream.get_hyperparameter()
            msg_time = datetime.fromisoformat(data.timestamp)

            if msg_time.tzinfo is None:
                msg_time = msg_time.replace(tzinfo=timezone.utc)

            latency = (recv_time - msg_time).total_seconds() * 1000  # ms

            buffer.append([latency, recv_time, msg_time])
            flush_if_ready()

    except Exception as e:
        print(f"[{udid}] Error: {e}")
        final_flush()


if __name__ == "__main__":
    threads = []

    for udid in UDID_LIST:
        t = threading.Thread(target=run_logger, args=(udid,), daemon=True)
        threads.append(t)
        t.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("\nStopping all loggers...")
