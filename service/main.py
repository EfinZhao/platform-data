import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import bct_client
import database
import emailer
import history
from fastapi import FastAPI
from routers import history as history_router
from routers import sensor

UPDATE_INTERVAL = 900 # 15 minutes

_first_update_done = asyncio.Event()


async def update_loop():
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
    asyncio.create_task(update_loop())
    asyncio.create_task(daily_email())
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(sensor.router)
app.include_router(history_router.router)


@app.get("/health")
async def health():
    return {"status": "200 OK"}
