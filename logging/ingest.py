import logging
import queue
import threading
import time
from collections import deque
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

# Batches, not rows. Measured steady-state depth with interleaved draining is
# tens of items, so this is a wide safety margin, not a working size.
QUEUE_MAXSIZE = 10000

# How long the writer blocks on an empty queue before re-checking the flush
# deadline. Bounds how late a flush can fire.
QUEUE_POLL_SECONDS = 0.1

# How often the supervisor checks that the writer thread is still alive.
WRITER_WATCHDOG_SECONDS = 5.0

# Collectors all start at once, but only this many may be *connecting* at a
# time. The stagger is gone because disk write concurrency is now 1 by
# construction; this limit exists for a different reason -- every connect POSTs
# core.api.bluecity.ai/api/token/ and the SDK retries in a blocking
# `while not self.getToken(): time.sleep(20)` loop, so 44 simultaneous auths
# risk being rate limited with no way to tell that is what happened.
CONNECT_CONCURRENCY = 8

# Cap on how long callers wait for every collector to finish connecting.
READY_TIMEOUT_S = 120.0

# Rolling window of queue-depth samples. Sampled after every drain, including
# between files during a flush pass, which is where the peaks are.
DEPTH_SAMPLE_WINDOW = 2000


class WriterService:
    # The single consumer. Collectors are producers and never touch a Buffer.
    # One thread drains the queue and, once per FLUSH_INTERVAL_S, writes every
    # buffer out. Disk write concurrency is 1 by construction, which is what
    # makes the startup stagger unnecessary. Buffer.add and Buffer.flush also
    # run on the same thread now, so they no longer contend.

    def __init__(self, *, queue_maxsize: int = QUEUE_MAXSIZE):
        self.queue: queue.Queue = queue.Queue(maxsize=queue_maxsize)
        self._buffers: dict[tuple[str, str], Buffer] = {}
        self._lock = threading.Lock()

        self._thread: threading.Thread | None = None
        self._running = threading.Event()

        self.dropped_batches = 0
        self.dropped_rows = 0
        self.flush_count = 0
        self.last_flush_seconds = 0.0
        self.max_flush_seconds = 0.0
        self.last_flush_files = 0
        self._last_drop_log = 0.0
        self._depth_samples: deque = deque(maxlen=DEPTH_SAMPLE_WINDOW)

    def register(self, intersection_id: str) -> None:
        with self._lock:
            for stream, schema in STREAM_SCHEMAS.items():
                key = (intersection_id, stream)
                if key not in self._buffers:
                    self._buffers[key] = Buffer(schema, stream, intersection_id)

    # ------------------------------------------------------
    # Producer side
    # ------------------------------------------------------
    def submit(self, intersection_id: str, stream: str, rows: list) -> bool:
        # One queue item per message, not per row, so queue traffic tracks the
        # message rate (~800/s at 44 sensors) not the row rate (~28,600/s).
        #
        # Never blocks: a blocked collector stalls get_hyperparameter, pushing
        # the loss upstream into the SDK cache where it cannot be counted.
        try:
            self.queue.put_nowait((intersection_id, stream, rows))
            return True
        except queue.Full:
            self.dropped_batches += 1
            self.dropped_rows += len(rows)
            now = time.monotonic()
            if now - self._last_drop_log > 10:
                self._last_drop_log = now
                log.error(
                    f"Writer queue full ({self.queue.qsize()} items) -- dropped "
                    f"{self.dropped_batches} batches / {self.dropped_rows} rows so far"
                )
            return False

    # ------------------------------------------------------
    # Consumer side
    # ------------------------------------------------------
    def start(self) -> None:
        if self.is_alive():
            return
        self._running.set()
        self._thread = threading.Thread(
            target=self._run, name="ingest-writer", daemon=True
        )
        self._thread.start()

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _run(self) -> None:
        next_flush = time.monotonic() + logger_rt.FLUSH_INTERVAL_S
        while self._running.is_set():
            try:
                self._drain(timeout=QUEUE_POLL_SECONDS)
                if time.monotonic() >= next_flush:
                    self.flush_all()
                    # Fixed cadence rather than "60s after the last flush ended",
                    # so a slow flush does not drag every later file later too.
                    next_flush += logger_rt.FLUSH_INTERVAL_S
                    if time.monotonic() >= next_flush:
                        next_flush = time.monotonic() + logger_rt.FLUSH_INTERVAL_S
            except Exception as e:
                # The single writer must not die on a transient error.
                log.error(f"Writer loop error: {e}", exc_info=True)
                time.sleep(0.5)

    def _drain(self, *, timeout: float | None = None) -> int:
        try:
            if timeout:
                item = self.queue.get(timeout=timeout)
            else:
                item = self.queue.get_nowait()
        except queue.Empty:
            self._depth_samples.append(0)
            return 0

        moved = 0
        while True:
            intersection_id, stream, rows = item
            buffer = self._buffers.get((intersection_id, stream))
            if buffer is not None:
                for row in rows:
                    buffer.add(row)
            moved += 1
            try:
                item = self.queue.get_nowait()
            except queue.Empty:
                self._depth_samples.append(self.queue.qsize())
                return moved

    def flush_all(self) -> None:
        started = time.perf_counter()
        with self._lock:
            keys = list(self._buffers)

        for key in keys:
            try:
                self._buffers[key].flush()
            except Exception as e:
                log.error(f"Flush failed for {key}: {e}")
            # Drain between files. Without this the queue has to absorb the
            # whole flush pass; with it, depth stays near one file's worth.
            self._drain()

        self.flush_count += 1
        self.last_flush_seconds = time.perf_counter() - started
        self.max_flush_seconds = max(self.max_flush_seconds, self.last_flush_seconds)
        self.last_flush_files = len(keys)
        log.info(
            f"Flushed {len(keys)} buffers in {self.last_flush_seconds:.2f}s "
            f"(queue depth {self.queue.qsize()})"
        )

    def stop(self, *, drain_timeout: float = 15.0) -> None:
        self._running.clear()
        if self._thread is not None:
            self._thread.join(timeout=drain_timeout)

        deadline = time.monotonic() + drain_timeout
        while self._drain() and time.monotonic() < deadline:
            pass
        self.flush_all()

    # ------------------------------------------------------
    def pending_rows(self) -> int:
        with self._lock:
            return sum(buffer.rows for buffer in self._buffers.values())

    def queue_depth(self) -> int:
        return self.queue.qsize()

    def queue_depth_percentiles(self) -> dict:
        samples = sorted(self._depth_samples)
        if not samples:
            return {"p50": 0, "p95": 0, "p99": 0, "max": 0, "samples": 0}

        def at(q):
            return samples[min(len(samples) - 1, int(len(samples) * q))]

        return {
            "p50": at(0.50), "p95": at(0.95), "p99": at(0.99),
            "max": samples[-1], "samples": len(samples),
        }

    def buffer_dropped_rows(self) -> int:
        with self._lock:
            return sum(b.dropped_rows for b in self._buffers.values())

    def buffer_max_rows(self) -> int:
        with self._lock:
            return max((b.rows for b in self._buffers.values()), default=0)


class Collector:
    def __init__(self, intersection_id: str, udid: str, writer: WriterService,
                 connect_semaphore: threading.Semaphore | None = None):
        self.intersection_id = intersection_id
        self.udid = udid
        self.writer = writer
        self.connect_semaphore = connect_semaphore
        self.running = False
        self.connected = False
        self.stream = None

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
            f"[{self.intersection_id}] connecting to {logger_rt.BEACON_ADDRESS} "
            f"(UDID={self.udid})..."
        )
        # Held only across the connect, never across the receive loop, which
        # never returns.
        if self.connect_semaphore is not None:
            self.connect_semaphore.acquire()
        try:
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
        finally:
            if self.connect_semaphore is not None:
                self.connect_semaphore.release()

        self.connected = True
        log.info(f"[{self.intersection_id}] connected. Receiving messages...")

        self._receive_loop()

    def stop(self) -> None:
        self.running = False
        self.connected = False
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
                self.writer.submit(self.intersection_id, "objects", [
                    {
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
                    }
                    for obj in objects
                ])
                self.frame_count += 1
                self.last_frame_ts = now_dt
        except AttributeError:
            pass

        # --- Phase Change Events ---
        try:
            phases = msg.phaseChange.phases
            if len(phases) > 0:
                self.writer.submit(self.intersection_id, "phase_changes", [
                    {
                        "intersection_id": self.intersection_id,
                        "recv_timestamp": now,
                        "phase_number": phase.phaseNumber,
                        "status": phase.status,
                        "phase_timestamp": (
                            phase.timestamp if phase.timestamp else now
                        ),
                    }
                    for phase in phases
                ])
                self.phase_change_count += 1
                self.last_phase_ts = now_dt
        except AttributeError:
            pass

        # --- Occupancy Change Events ---
        try:
            occupancies = msg.occupancyChange.occupancies
            if len(occupancies) > 0:
                self.writer.submit(self.intersection_id, "occupancy_changes", [
                    {
                        "intersection_id": self.intersection_id,
                        "recv_timestamp": now,
                        "phase_number": occupancy.phaseLabel,
                        "status": occupancy.status,
                        "phase_timestamp": (
                            occupancy.timestamp if occupancy.timestamp else now
                        ),
                    }
                    for occupancy in occupancies
                ])
                self.occupancy_change_count += 1
        except AttributeError:
            pass

        # --- Phase Time to Change ---
        try:
            phases = msg.phaseTimeToChange.phases
            if len(phases) > 0:
                self.writer.submit(self.intersection_id, "phase_ttc", [
                    {
                        "intersection_id": self.intersection_id,
                        "recv_timestamp": now,
                        "phase_number": phase.phaseNumber,
                        "min_time_to_change": phase.minTimeToChange,
                        "max_time_to_change": phase.maxTimeToChange,
                    }
                    for phase in phases
                ])
                self.phase_ttc_count += 1
                self.last_ttc_ts = now_dt
        except AttributeError:
            pass


class IngestService:
    """Owns the writer and every collector. The single thing both entry points
    construct.

    start() returns immediately; every collector comes up at once on a
    background launcher thread, with connect concurrency bounded by a
    semaphore. stop() halts collectors, drains the queue and flushes, which is
    what keeps a restart from discarding up to a minute of data per sensor.
    """

    def __init__(self, sensors, *, connect_concurrency: int = CONNECT_CONCURRENCY):
        # sensors: iterable of (intersection_id, udid)
        self.sensors = list(sensors)
        self.writer = WriterService()
        self.collectors: list[Collector] = []

        self._connect_semaphore = threading.Semaphore(connect_concurrency)
        self._threads: list[threading.Thread] = []
        self._launcher = None
        self._supervisor = None
        self._stopped = threading.Event()
        self.writer_restarts = 0

    # ------------------------------------------------------
    def start(self) -> None:
        # Writer first: collectors submit as soon as they connect, and a batch
        # submitted before the writer is draining would sit in the queue.
        self.writer.start()

        self._supervisor = threading.Thread(
            target=self._supervise, name="ingest-supervisor", daemon=True
        )
        self._supervisor.start()

        self._launcher = threading.Thread(
            target=self._launch_all, name="ingest-launcher", daemon=True
        )
        self._launcher.start()

    def _supervise(self) -> None:
        # One writer means one point of failure: if it dies, every sensor stops
        # being written and the collectors carry on as if nothing is wrong.
        # Buffers live on the WriterService, not the thread, so a restart keeps
        # whatever was already buffered.
        while not self._stopped.wait(WRITER_WATCHDOG_SECONDS):
            if not self.writer.is_alive():
                self.writer_restarts += 1
                log.error(
                    f"Writer thread is dead -- restarting "
                    f"(restart #{self.writer_restarts})"
                )
                self.writer.start()

    def _launch_all(self) -> None:
        # No stagger. Every collector starts now; the semaphore inside
        # Collector.start bounds how many connect at once.
        for intersection_id, udid in self.sensors:
            if self._stopped.is_set():
                return
            self._spawn(intersection_id, udid)
        log.info(f"All {len(self.sensors)} collectors launched.")

    def _spawn(self, intersection_id: str, udid: str) -> None:
        collector = Collector(
            intersection_id, udid, self.writer,
            connect_semaphore=self._connect_semaphore,
        )
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

        # Collectors are done submitting, so the writer can drain what is left
        # and write everything out.
        pending = self.writer.pending_rows()
        log.info(
            f"Final flush: {pending} buffered rows, "
            f"{self.writer.queue_depth()} queued batches"
        )
        self.writer.stop(drain_timeout=drain_timeout)

        if self.writer.dropped_batches:
            log.error(
                f"Dropped {self.writer.dropped_batches} batches / "
                f"{self.writer.dropped_rows} rows over this run"
            )
        log.info("Ingest stopped.")

    def connected_count(self) -> int:
        return sum(1 for c in self.collectors if c.connected)

    def wait_ready(self, *, timeout: float = READY_TIMEOUT_S) -> bool:
        """Block until every sensor has connected, or timeout. Returns whether
        all of them made it.

        Callers that report sensor health need this: a status pass run before
        the collectors are up sees empty freshness timestamps and marks healthy
        sensors down.
        """
        deadline = time.monotonic() + timeout
        expected = len(self.sensors)
        while time.monotonic() < deadline:
            if self._stopped.is_set():
                return False
            if len(self.collectors) == expected and self.connected_count() == expected:
                log.info(f"All {expected} collectors connected.")
                return True
            time.sleep(0.5)

        log.warning(
            f"Only {self.connected_count()}/{expected} collectors connected "
            f"within {timeout:.0f}s; continuing anyway."
        )
        return False

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
            "collectors_connected": self.connected_count(),
            "pending_rows": self.writer.pending_rows(),
            "buffer_max_rows": self.writer.buffer_max_rows(),
            "buffer_row_cap": logger_rt.MAX_BUFFER_ROWS,
            "queue_depth": self.writer.queue_depth(),
            "queue_maxsize": self.writer.queue.maxsize,
            "queue_depth_percentiles": self.writer.queue_depth_percentiles(),
            "writer_alive": self.writer.is_alive(),
            "writer_restarts": self.writer_restarts,
            "flush_count": self.writer.flush_count,
            "last_flush_files": self.writer.last_flush_files,
            "last_flush_seconds": round(self.writer.last_flush_seconds, 3),
            "max_flush_seconds": round(self.writer.max_flush_seconds, 3),
            "dropped_batches": self.writer.dropped_batches,
            "dropped_rows_queue": self.writer.dropped_rows,
            "dropped_rows_buffer": self.writer.buffer_dropped_rows(),
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
