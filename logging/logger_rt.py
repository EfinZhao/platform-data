import logging
import os
import threading
import time
from collections import deque
import tomllib
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

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

# Ceiling on rows held in one Buffer between flushes. Normal peak is phase_ttc
# at ~35,000 rows per 60s cycle, so this is roughly three cycles of headroom.
#
# It exists for the case where writes stop succeeding -- disk full, a bad mount,
# permissions. flush() logs and returns, so without a cap the buffers grow until
# the process is OOM-killed and everything buffered is lost. At the cap the
# oldest rows are shed instead, which bounds the loss to the oldest data and
# keeps the most recent.
MAX_BUFFER_ROWS = 100_000


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
RUNNING_LOGGERS: dict[str, "ingest.Collector"] = {}


# ==========================================================
# BUFFER — accumulates rows, writes to Parquet on flush
# ==========================================================
class Buffer:
    def __init__(self, schema, name, intersection_id, max_rows=MAX_BUFFER_ROWS):
        self.schema = schema
        self.name = name
        self.intersection_id = intersection_id
        self.max_rows = max_rows
        self.lock = threading.Lock()
        self.rows = 0
        self.dropped_rows = 0
        # deque(maxlen) evicts the oldest on overflow, in O(1).
        self._cols = {f.name: deque(maxlen=max_rows) for f in schema}
        self._last_drop_log = 0.0

    def _new_cols(self):
        return {f.name: deque(maxlen=self.max_rows) for f in self.schema}

    def add(self, row):
        with self.lock:
            at_cap = self.rows >= self.max_rows
            for col in self._cols:
                self._cols[col].append(row.get(col))
            if at_cap:
                self.dropped_rows += 1
                now = time.monotonic()
                if now - self._last_drop_log > 30:
                    self._last_drop_log = now
                    log.error(
                        f"[{self.intersection_id}/{self.name}] buffer at cap "
                        f"({self.max_rows} rows) -- shedding oldest, "
                        f"{self.dropped_rows} dropped so far. Writes are failing "
                        f"or the writer is not draining."
                    )
            else:
                self.rows += 1

    def flush(self):
        with self.lock:
            if self.rows == 0:
                return None
            data = {name: list(col) for name, col in self._cols.items()}
            count = self.rows
            self._cols = self._new_cols()
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

        # Write to a temp name and rename into place. os.replace is atomic on
        # the same filesystem, so a crash mid-write leaves no half-written
        # parquet for downstream readers to trip over.
        tmp_path = os.path.join(dir_path, f".{filename}.tmp")

        write_start = time.perf_counter()
        pq.write_table(
            table, tmp_path,
            compression=COMPRESSION,
            compression_level=COMPRESSION_LEVEL,
            write_statistics=True,
        )
        os.replace(tmp_path, filepath)
        write_seconds = time.perf_counter() - write_start

        size_bytes = os.path.getsize(filepath)
        log.info(
            f"[{self.name}] {count} rows -> {filepath} "
            f"({size_bytes / 1024:.1f} KB, {write_seconds * 1000:.1f} ms)"
        )
        return filepath
