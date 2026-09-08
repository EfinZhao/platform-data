import logging
import threading
import time
from datetime import datetime, timezone

import logger_rt
from logger_rt import (
    OBJECT_SCHEMA,
    OCCUPANCY_CHANGE_SCHEMA,
    PHASE_CHANGE_SCHEMA,
    PHASE_TTC_SCHEMA,
    RUNNING_LOGGERS,
    Buffer,
)
from realtime_subscriber.Realtime_subscriber_api import BCTWSConnection

log = logging.getLogger("logger")

STREAM_SCHEMAS = {
    "objects": OBJECT_SCHEMA,
    "phase_changes": PHASE_CHANGE_SCHEMA,
    "occupancy_changes": OCCUPANCY_CHANGE_SCHEMA,
    "phase_ttc": PHASE_TTC_SCHEMA,
}

DEFAULT_DRAIN_TIMEOUT_S = 30.0


class WriterService:
    def __init__(self):
        self._buffers: dict[tuple[str, str], Buffer] = {}
        self._lock = threading.Lock()

    def register(self, intersection_id: str) -> None:
        with self._lock:
            for stream, schema in STREAM_SCHEMAS.items():
                key = (intersection_id, stream)
                if key not in self._buffers:
                    self._buffers[key] = Buffer(schema, stream, intersection_id)

    def add(self, intersection_id: str, stream: str, row: dict) -> None:
        self._buffers[(intersection_id, stream)].add(row)

    def flush_intersection(self, intersection_id: str) -> None:
        for stream in STREAM_SCHEMAS:
            buffer = self._buffers.get((intersection_id, stream))
            if buffer is not None:
                buffer.flush()

    def flush_all(self) -> None:
        with self._lock:
            keys = list(self._buffers)
        for key in keys:
            try:
                self._buffers[key].flush()
            except Exception as e:
                log.error(f"Flush failed for {key}: {e}")

    def pending_rows(self) -> int:
        with self._lock:
            return sum(buffer.rows for buffer in self._buffers.values())


class Collector:
    def __init__(self, intersection_id: str, udid: str, writer: WriterService):
        self.intersection_id = intersection_id
        self.udid = udid
        self.writer = writer
        self.running = False
        self.stream = None
        self.flush_thread = None

        self.msg_count = 0
        self.frame_count = 0
        self.phase_change_count = 0
        self.occupancy_change_count = 0
        self.phase_ttc_count = 0

        self.last_frame_ts = None
        self.last_phase_ts = None
        self.last_ttc_ts = None

    # ------------------------------------------------------
    def start(self) -> None:
        self.running = True
        self.writer.register(self.intersection_id)

        log.info(
            f"Connecting to {logger_rt.BEACON_ADDRESS} (UDID={self.udid})..."
        )
        self.stream = BCTWSConnection(
            UDID=self.udid,
            username=logger_rt.secrets["username"],
            password=logger_rt.secrets["password"],
            beaconAddress=logger_rt.BEACON_ADDRESS,
            singleton=True,
            subscriptions=[
                BCTWSConnection.subscriptionOption.FRAME,
                BCTWSConnection.subscriptionOption.PHASE_CHANGE,
                BCTWSConnection.subscriptionOption.PHASE_TIME_TO_CHANGE,
                BCTWSConnection.subscriptionOption.LOOP_CHANGE,
            ],
        )
        log.info("Connected. Receiving messages...")

        self.flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self.flush_thread.start()

        self._receive_loop()

    def stop(self) -> None:
        self.running = False
        log.info(
            f"[{self.intersection_id}] stopped. Totals: {self.msg_count} msgs, "
            f"{self.frame_count} frames, "
            f"{self.phase_change_count} phase changes, "
            f"{self.occupancy_change_count} occupancy changes, "
            f"{self.phase_ttc_count} phase TTCs"
        )

    # ------------------------------------------------------
    def _receive_loop(self) -> None:
        while self.running:
            try:
                msg = self.stream.get_hyperparameter()
                self._route(msg)

                self.msg_count += 1
                if self.msg_count % 1000 == 0:
                    log.info(
                        f"[{self.intersection_id}] messages: {self.msg_count} "
                        f"(frames={self.frame_count}, "
                        f"phaseChanges={self.phase_change_count}, "
                        f"occupancyChanges={self.occupancy_change_count}, "
                        f"phaseTTC={self.phase_ttc_count})"
                    )

            except Exception as e:
                log.error(f"[{self.intersection_id}] error receiving: {e}")
                time.sleep(1)

    def _route(self, msg) -> None:
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat()

        # Top-level timestamp, falling back to receive time if missing.
        frame_ts = msg.timestamp if msg.timestamp else now

        # --- Frame (object detections) ---
        try:
            objects = msg.frame.objects
            if len(objects) > 0:
                num_objects = len(objects)
                for obj in objects:
                    self.writer.add(self.intersection_id, "objects", {
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
                    self.writer.add(self.intersection_id, "phase_changes", {
                        "intersection_id": self.intersection_id,
                        "recv_timestamp": now,
                        "phase_number": phase.phaseNumber,
                        "status": phase.status,
                        "phase_timestamp": (
                            phase.timestamp if phase.timestamp else now
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
                    self.writer.add(self.intersection_id, "occupancy_changes", {
                        "intersection_id": self.intersection_id,
                        "recv_timestamp": now,
                        "phase_number": occupancy.phaseLabel,
                        "status": occupancy.status,
                        "phase_timestamp": (
                            occupancy.timestamp if occupancy.timestamp else now
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
                    self.writer.add(self.intersection_id, "phase_ttc", {
                        "intersection_id": self.intersection_id,
                        "recv_timestamp": now,
                        "phase_number": phase.phaseNumber,
                        "min_time_to_change": phase.minTimeToChange,
                        "max_time_to_change": phase.maxTimeToChange,
                    })
                self.phase_ttc_count += 1
                self.last_ttc_ts = now_dt
        except AttributeError:
            pass

    # ------------------------------------------------------
    def _flush_loop(self) -> None:
        while self.running:
            time.sleep(logger_rt.FLUSH_INTERVAL_S)
            if self.running:
                self.writer.flush_intersection(self.intersection_id)


class IngestService:
    """Owns the writer and every collector. The single thing both entry points
    construct.

    start() returns immediately; sensors come up on a background launcher
    thread, staggered. stop() halts collectors and flushes what they have
    buffered, which is what keeps a restart from discarding up to a minute of
    data per sensor.
    """

    def __init__(self, sensors, *, stagger_seconds: float = 10.0):
        # sensors: iterable of (intersection_id, udid)
        self.sensors = list(sensors)
        self.stagger_seconds = stagger_seconds
        self.writer = WriterService()
        self.collectors: list[Collector] = []

        self._threads: list[threading.Thread] = []
        self._launcher = None
        self._stopped = threading.Event()

    # ------------------------------------------------------
    def start(self) -> None:
        self._launcher = threading.Thread(
            target=self._launch_all, name="ingest-launcher", daemon=True
        )
        self._launcher.start()

    def _launch_all(self) -> None:
        for index, (intersection_id, udid) in enumerate(self.sensors):
            if self._stopped.is_set():
                return
            self._spawn(intersection_id, udid)
            if self.stagger_seconds and index < len(self.sensors) - 1:
                if self._stopped.wait(self.stagger_seconds):
                    return
        log.info(f"All {len(self.sensors)} collectors launched.")

    def _spawn(self, intersection_id: str, udid: str) -> None:
        collector = Collector(intersection_id, udid, self.writer)
        self.collectors.append(collector)

        # Registered before start(), as the previous design did, so the status
        # path sees an object with empty timestamps rather than a missing key.
        RUNNING_LOGGERS[udid] = collector

        thread = threading.Thread(
            target=self._run_collector,
            args=(collector,),
            name=f"collector-{intersection_id}",
            daemon=True,
        )
        self._threads.append(thread)
        thread.start()

    def _run_collector(self, collector: Collector) -> None:
        try:
            collector.start()
        except Exception as e:
            log.error(f"[{collector.intersection_id}] collector died: {e}")

    # ------------------------------------------------------
    def stop(self, *, drain_timeout: float = DEFAULT_DRAIN_TIMEOUT_S) -> None:
        if self._stopped.is_set():
            return
        self._stopped.set()

        log.info("Stopping collectors...")
        for collector in self.collectors:
            collector.stop()

        # Receive loops block in get_hyperparameter, so give them a moment to
        # come back and notice running is False before the final flush.
        deadline = time.monotonic() + drain_timeout
        for thread in self._threads:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            thread.join(timeout=remaining)

        pending = self.writer.pending_rows()
        log.info(f"Final flush of {pending} buffered rows...")
        self.writer.flush_all()
        log.info("Ingest stopped.")

    def wait(self, timeout: float | None = None) -> bool:
        """Block until stop() is called. Returns True if it has been.

        With a timeout, this is the poll the standalone runner uses to print
        status between checks.
        """
        return self._stopped.wait(timeout)

    # ------------------------------------------------------
    def stats(self) -> dict:
        alive = sum(1 for t in self._threads if t.is_alive())
        return {
            "sensors_configured": len(self.sensors),
            "collectors_started": len(self.collectors),
            "collectors_alive": alive,
            "pending_rows": self.writer.pending_rows(),
            "messages": sum(c.msg_count for c in self.collectors),
            "frames": sum(c.frame_count for c in self.collectors),
            "phase_changes": sum(c.phase_change_count for c in self.collectors),
            "occupancy_changes": sum(c.occupancy_change_count for c in self.collectors),
            "phase_ttc": sum(c.phase_ttc_count for c in self.collectors),
        }


def sensors_from_csv(path) -> list[tuple[str, str]]:
    import pandas as pd

    frame = pd.read_csv(path)
    return [
        (f"{row.major}_{row.minor}", row.UDID)
        for row in frame.itertuples(index=False)
    ]
