import asyncio
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path

import bct_client
import database
import emailer
import history
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from routers import history as history_router
from routers import sensor

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "logging"))
from ingest import IngestService, sensors_from_csv  # path must be set up first

REPO_ROOT = Path(__file__).resolve().parent.parent
UDID_CSV = REPO_ROOT / "udid.csv"
# Long enough for receive loops to return from get_hyperparameter and for the
# final flush to finish. The systemd unit's TimeoutStopSec must exceed it or
# the process is killed mid-flush.
INGEST_DRAIN_TIMEOUT_S = 30

UPDATE_INTERVAL = 900 # 15 minutes

# How often ingest health goes to the log. The endpoint is for looking; this is
# so a problem is visible in the journal after the fact without one.
INGEST_HEALTH_INTERVAL_S = 300

_first_update_done = asyncio.Event()


async def update_loop(service):
    # The first status pass used to run seconds after boot, while collectors
    # were still connecting. bct_client then read empty freshness timestamps and
    # wrote frame/phase/ttc false for nearly every sensor, so the dashboard
    # showed healthy sensors as down until the next 15 minute cycle -- and the
    # daily email could be sent from that state.
    await asyncio.to_thread(lambda: service.wait_ready())

    warmup_done = False
    while True:
        print("[INFO] Updating sensor statuses...")

        try:
            with database.get_connection() as conn:
                udids = [
                    row["udid"]
                    for row in conn.execute("SELECT udid FROM sensors").fetchall()
                ]
        except Exception as e:
            print(f"[ERROR] Failed to fetch sensor list: {e}")
            await asyncio.sleep(UPDATE_INTERVAL)
            continue

        total = len(udids)
        completed = 0

        async def update_one(udid: str):
            nonlocal completed
            await bct_client.check_and_update(udid)
            completed += 1
            print(f"\r[INFO] {completed}/{total} sensors updated...", end="", flush=True)

        await asyncio.gather(*[update_one(udid) for udid in udids])
        print(f"\n[INFO] All sensor statuses updated at {datetime.now().isoformat()}")

        if not warmup_done:
            warmup_done = True
            print("[INFO] Warmup cycle complete — running second pass for stable data...")
            continue

        _first_update_done.set()
        try:
            await asyncio.to_thread(history.record_and_alert)
        except Exception as e:
            print(f"[ERROR] Snapshot/alert failed: {e}")
        await asyncio.sleep(UPDATE_INTERVAL)


async def ingest_health_loop(service):
    while True:
        await asyncio.sleep(INGEST_HEALTH_INTERVAL_S)
        s = service.stats()
        depth = s["queue_depth_percentiles"]
        line = (
            f"[INGEST] connected {s['collectors_connected']}/{s['sensors_configured']}  "
            f"writer_alive={s['writer_alive']} restarts={s['writer_restarts']}  "
            f"queue p50={depth['p50']} p99={depth['p99']} max={depth['max']}"
            f"/{s['queue_maxsize']}  "
            f"buffered={s['pending_rows']} (max {s['buffer_max_rows']}"
            f"/{s['buffer_row_cap']})  "
            f"flush last={s['last_flush_seconds']}s max={s['max_flush_seconds']}s"
        )
        dropped = s["dropped_rows_queue"] + s["dropped_rows_buffer"]
        if dropped or not s["writer_alive"]:
            print(f"[ERROR] {line}  DROPPED rows queue={s['dropped_rows_queue']} "
                  f"buffer={s['dropped_rows_buffer']}")
        else:
            print(f"[INFO] {line}")


REPORT_HOUR   = 23
REPORT_MINUTE = 59


async def _send_report():
    try:
        with database.get_connection() as conn:
            rows = conn.execute("""
                SELECT udid, major, minor, status, frame_status, phase_status,
                       ttc_status, last_online, last_checked
                FROM sensors
                ORDER BY major, minor
            """).fetchall()
    except Exception as e:
        print(f"[ERROR] Failed to fetch sensor data for daily email: {e}")
        return

    try:
        await asyncio.to_thread(lambda: emailer.send_daily_report(rows))
        print(f"[INFO] Daily email sent at {datetime.now().isoformat()}")
    except Exception as e:
        print(f"[ERROR] Failed to send daily email: {e}")


async def daily_email():
    await _first_update_done.wait()
    await _send_report()  # send after first update cycle completes (testing)

    while True:
        now = datetime.now()
        target = now.replace(hour=REPORT_HOUR, minute=REPORT_MINUTE, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)

        wait = (target - now).total_seconds()
        print(f"[INFO] Daily email scheduled for {target.strftime('%Y-%m-%d %H:%M:%S')} ({wait:.0f}s from now)")
        await asyncio.sleep(wait)

        await _send_report()


@asynccontextmanager
async def lifespan(app: FastAPI):
    database.init_db()

    # start() returns immediately; sensors come up on a launcher thread.
    app.state.ingest = IngestService(sensors_from_csv(UDID_CSV))
    app.state.ingest.start()

    asyncio.create_task(update_loop(app.state.ingest))
    asyncio.create_task(daily_email())
    asyncio.create_task(ingest_health_loop(app.state.ingest))

    yield

    # Previously absent, so every restart discarded up to a minute of buffered
    # data from every sensor.
    await asyncio.to_thread(
        lambda: app.state.ingest.stop(drain_timeout=INGEST_DRAIN_TIMEOUT_S)
    )


app = FastAPI(lifespan=lifespan)
app.include_router(sensor.router, prefix="/api")
app.include_router(history_router.router, prefix="/api")


@app.get("/health")
async def health():
    return {"status": "200 OK"}


@app.get("/api/ingest")
async def ingest_status():
    """Ingest health: queue depth, flush cost, and anything dropped.

    Without this there is no way to tell a healthy ingest from one that is
    silently shedding rows -- the parquet files still appear, just with less in
    them.
    """
    stats = app.state.ingest.stats()
    stats["healthy"] = (
        stats["writer_alive"]
        and stats["dropped_rows_queue"] == 0
        and stats["dropped_rows_buffer"] == 0
        and stats["collectors_connected"] == stats["sensors_configured"]
    )
    return stats


_FRONTEND_DIST = REPO_ROOT / "frontend" / "dist"
if _FRONTEND_DIST.is_dir():
    app.mount("/", StaticFiles(directory=_FRONTEND_DIST, html=True), name="frontend")
