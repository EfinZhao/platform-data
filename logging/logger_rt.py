import logging
import os
import threading
import time
import tomllib
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from realtime_subscriber.Realtime_subscriber_api import BCTWSConnection

_MODULE_DIR = Path(__file__).resolve().parent

with open(_MODULE_DIR / ".secrets.toml", "rb") as file:
    secrets = tomllib.load(file)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("logger")


# ==========================================================
# CONFIG — edit these for your environment
# ==========================================================
BEACON_ADDRESS = "https://realtime.us.beacon.1.api.bluecity.ai/"
BASE_DIR = str(_MODULE_DIR / "data" / "rt")
FLUSH_INTERVAL_S = 60
COMPRESSION = "zstd"
COMPRESSION_LEVEL = 3


# ==========================================================
# SCHEMAS — raw fields only, matching your protobuf exactly
# ==========================================================
OBJECT_SCHEMA = pa.schema([
    ("intersection_id", pa.string()),
    ("recv_timestamp", pa.string()),
    ("frame_timestamp", pa.string()),
    ("object_id", pa.string()),
    ("center_x", pa.float64()),
    ("center_y", pa.float64()),
    ("width", pa.float64()),
    ("length", pa.float64()),
    ("rotation", pa.float64()),
    ("class_type", pa.string()),
    ("objects_in_frame", pa.uint32()),
])

PHASE_CHANGE_SCHEMA = pa.schema([
    ("intersection_id", pa.string()),
    ("recv_timestamp", pa.string()),
    ("phase_number", pa.string()),
    ("status", pa.uint32()),
    ("phase_timestamp", pa.string()),
])

OCCUPANCY_CHANGE_SCHEMA = pa.schema([
    ("intersection_id", pa.string()),
    ("recv_timestamp", pa.string()),
    ("phase_number", pa.string()),
    ("status", pa.bool_()),
    ("occupancy_timestamp", pa.string()),
])

PHASE_TTC_SCHEMA = pa.schema([
    ("intersection_id", pa.string()),
    ("recv_timestamp", pa.string()),
    ("phase_number", pa.string()),
    ("min_time_to_change", pa.float64()),
    ("max_time_to_change", pa.float64()),
])


# ==========================================================
# REGISTRY — running loggers, keyed by UDID.
# Lets other processes (e.g. the dashboard service) read live
# state without opening a second BCTWSConnection.
# ==========================================================
RUNNING_LOGGERS: dict[str, "IntersectionLogger"] = {}


# ==========================================================
# BUFFER — accumulates rows, writes to Parquet on flush
# ==========================================================
class Buffer:
    def __init__(self, schema, name, intersection_id):
        self.schema = schema
        self.name = name
        self.intersection_id = intersection_id
        self.lock = threading.Lock()
        self.rows = 0
        self._cols = {f.name: [] for f in schema}

    def add(self, row):
        with self.lock:
            for col in self._cols:
                self._cols[col].append(row.get(col))
            self.rows += 1

    def flush(self):
        with self.lock:
            if self.rows == 0:
                return None
            data = self._cols
            count = self.rows
            self._cols = {f.name: [] for f in self.schema}
            self.rows = 0

        table = pa.table(data, schema=self.schema)
        now = datetime.now(timezone.utc)

        dir_path = os.path.join(
            BASE_DIR, self.intersection_id, self.name,
            f"date={now.strftime('%Y-%m-%d')}",
            f"hour={now.strftime('%H')}"
        )
        os.makedirs(dir_path, exist_ok=True)

        filename = (
            f"{self.intersection_id}_{self.name}_"
            f"{now.strftime('%Y%m%d_%H%M%S')}_{count}.parquet"
        )
        filepath = os.path.join(dir_path, filename)

        pq.write_table(
            table, filepath,
            compression=COMPRESSION,
            compression_level=COMPRESSION_LEVEL,
            write_statistics=True,
        )
        size_kb = os.path.getsize(filepath) / 1024
        log.info(
            f"[{self.name}] {count} rows -> {filepath} "
            f"({size_kb:.1f} KB)"
        )
        return filepath


# ==========================================================
# LOGGER
# ==========================================================
class IntersectionLogger:

    def __init__(self, intersection_id, udid):
        self.intersection_id = intersection_id
        self.udid = udid

        self.objects = Buffer(OBJECT_SCHEMA, "objects", intersection_id)
        self.phase_changes = Buffer(PHASE_CHANGE_SCHEMA, "phase_changes", intersection_id)
        self.occupancy_changes = Buffer(OCCUPANCY_CHANGE_SCHEMA, "occupancy_changes", intersection_id)
        self.phase_ttc = Buffer(PHASE_TTC_SCHEMA, "phase_ttc", intersection_id)
        self.running = False

        # Counters for monitoring
        self.msg_count = 0
        self.frame_count = 0
        self.phase_change_count = 0
        self.occupancy_change_count = 0
        self.phase_ttc_count = 0

        # Last-seen timestamps per message type — lets other
        # processes derive live status without a second connection.
        self.last_frame_ts = None
        self.last_phase_ts = None
        self.last_ttc_ts = None

    def start(self):
        self.running = True

        log.info(
            f"Connecting to {BEACON_ADDRESS} "
            f"(UDID={self.udid})..."
        )
        self.stream = BCTWSConnection(
            UDID=self.udid,
            username=secrets["username"],
            password=secrets["password"],
            beaconAddress=BEACON_ADDRESS,
            singleton=True,
            subscriptions=[
                BCTWSConnection.subscriptionOption.FRAME,
                BCTWSConnection.subscriptionOption.PHASE_CHANGE,
                BCTWSConnection.subscriptionOption.PHASE_TIME_TO_CHANGE,
                BCTWSConnection.subscriptionOption.LOOP_CHANGE,
            ],
        )
        log.info("Connected. Receiving messages...")

        self.flush_thread = threading.Thread(
            target=self._flush_loop, daemon=True
        )
        self.flush_thread.start()

        self._receive_loop()

    def stop(self):
        self.running = False
        self._flush_all()
        log.info(
            f"Stopped. Totals: {self.msg_count} msgs, "
            f"{self.frame_count} frames, "
            f"{self.phase_change_count} phase changes, "
            f"{self.occupancy_change_count} occupancy changes, "
            f"{self.phase_ttc_count} phase TTCs"
        )

    # ------------------------------------------------------
    # Receive loop
    # ------------------------------------------------------
    def _receive_loop(self):
        while self.running:
            try:
                msg = self.stream.get_hyperparameter()
                self._route(msg)

                self.msg_count += 1
                if self.msg_count % 1000 == 0:
                    log.info(
                        f"Messages: {self.msg_count} "
                        f"(frames={self.frame_count}, "
                        f"phaseChanges={self.phase_change_count}, "
                        f"phaseChanges={self.occupancy_change_count}, "
                        f"phaseTTC={self.phase_ttc_count})"
                    )

            except Exception as e:
                log.error(f"Error receiving: {e}")
                time.sleep(1)

    # ------------------------------------------------------
    # Route each HyperParameter message by checking which
    # fields have actual data in them.
    #
    # We use try/except rather than HasField because it's
    # more robust across different protobuf versions.
    # Each message can contain ANY COMBINATION of these —
    # they're not mutually exclusive.
    # ------------------------------------------------------
    def _route(self, msg):
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat()

        # Get the top-level timestamp from the message.
        # Fall back to our receive time if it's missing.
        frame_ts = msg.timestamp if msg.timestamp else now

        # --- Frame (object detections) ---
        try:
            objects = msg.frame.objects
            if len(objects) > 0:
                num_objects = len(objects)
                for obj in objects:
                    self.objects.add({
                        "intersection_id": self.intersection_id,
                        "recv_timestamp": now,
                        "frame_timestamp": frame_ts,
                        "object_id": obj.id,
                        "center_x": obj.centerX,
                        "center_y": obj.centerY,
                        "width": obj.width,
                        "length": obj.length,
                        "rotation": obj.rotation,
                        "class_type": obj.classType,
                        "objects_in_frame": num_objects,
                    })
                self.frame_count += 1
                self.last_frame_ts = now_dt
        except AttributeError:
            pass

        # --- Phase Change Events ---
        try:
            phases = msg.phaseChange.phases
            if len(phases) > 0:
                for phase in phases:
                    self.phase_changes.add({
                        "intersection_id": self.intersection_id,
                        "recv_timestamp": now,
                        "phase_number": phase.phaseNumber,
                        "status": phase.status,
                        "phase_timestamp": (
                            phase.timestamp
                            if phase.timestamp
                            else now
                        ),
                    })
                self.phase_change_count += 1
                self.last_phase_ts = now_dt
        except AttributeError:
            pass

        # --- Occupancy Change Events ---
        try:
            occupancies = msg.occupancyChange.occupancies
            if len(occupancies) > 0:
                for occupancy in occupancies:
                    self.occupancy_changes.add({
                        "intersection_id": self.intersection_id,
                        "recv_timestamp": now,
                        "phase_number": occupancy.phaseLabel,
                        "status": occupancy.status,
                        "phase_timestamp": (
                            occupancy.timestamp
                            if occupancy.timestamp
                            else now
                        ),
                    })
                self.occupancy_change_count += 1
        except AttributeError:
            pass

        # --- Phase Time to Change ---
        try:
            phases = msg.phaseTimeToChange.phases
            if len(phases) > 0:
                for phase in phases:
                    self.phase_ttc.add({
                        "intersection_id": self.intersection_id,
                        "recv_timestamp": now,
                        "phase_number": phase.phaseNumber,
                        "min_time_to_change": (
                            phase.minTimeToChange
                        ),
                        "max_time_to_change": (
                            phase.maxTimeToChange
                        ),
                    })
                self.phase_ttc_count += 1
                self.last_ttc_ts = now_dt
        except AttributeError:
            pass

    # ------------------------------------------------------
    # Periodic flush
    # ------------------------------------------------------
    def _flush_loop(self):
        while self.running:
            time.sleep(FLUSH_INTERVAL_S)
            if self.running:
                self._flush_all()

    def _flush_all(self):
        self.objects.flush()
        self.occupancy_changes.flush()
        self.phase_changes.flush()
        self.phase_ttc.flush()


def logger(intersection, udid):
    lgr = IntersectionLogger(intersection, udid)
    RUNNING_LOGGERS[udid] = lgr
    try:
        lgr.start()
    except KeyboardInterrupt:
        log.info("Shutdown requested...")
        lgr.stop()
        log.info("Done.")
