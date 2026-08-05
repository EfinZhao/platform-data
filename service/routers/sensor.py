import asyncio

from fastapi import APIRouter, HTTPException

import bct_client
import database

router = APIRouter()


def _row_to_dict(row) -> dict:
    return {
        "name":         f"{row['major']}_{row['minor']}",
        "status":       row["status"],
        "frame_status": row["frame_status"],
        "phase_status": row["phase_status"],
        "ttc_status":   row["ttc_status"],
        "last_online":  row["last_online"],
        "last_checked": row["last_checked"],
    }


@router.get("/status/all")
async def all_sensor_statuses():
    try:
        with database.get_connection() as conn:
            rows = conn.execute("""
                SELECT udid, major, minor, status, frame_status, phase_status, ttc_status, last_online, last_checked
                FROM sensors
            """).fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {row["udid"]: _row_to_dict(row) for row in rows}


@router.get("/status/{udid}")
async def sensor_status(udid: str):
    try:
        with database.get_connection() as conn:
            row = conn.execute("""
                SELECT major, minor, status, frame_status, phase_status, ttc_status, last_online, last_checked
                FROM sensors
                WHERE udid = ?
            """, (udid,)).fetchone()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if row is None:
        raise HTTPException(status_code=404, detail=f"Sensor {udid} not found")

    return _row_to_dict(row)


@router.post("/update/all")
async def update_all_sensor_statuses():
    try:
        with database.get_connection() as conn:
            udids = [
                row["udid"]
                for row in conn.execute("SELECT udid FROM sensors").fetchall()
            ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    await asyncio.gather(*[bct_client.check_and_update(udid) for udid in udids])

    try:
        with database.get_connection() as conn:
            rows = conn.execute("""
                SELECT udid, major, minor, status, frame_status, phase_status, ttc_status, last_online, last_checked
                FROM sensors
            """).fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {row["udid"]: _row_to_dict(row) for row in rows}


@router.post("/update/{udid}")
async def update_sensor_status(udid: str):
    try:
        await bct_client.check_and_update(udid)

        with database.get_connection() as conn:
            row = conn.execute("""
                SELECT major, minor, status, frame_status, phase_status, ttc_status, last_online, last_checked
                FROM sensors
                WHERE udid = ?
            """, (udid,)).fetchone()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if row is None:
        raise HTTPException(status_code=404, detail=f"Sensor {udid} not found")

    return _row_to_dict(row)
