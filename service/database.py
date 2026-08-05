import sqlite3
from datetime import datetime

DB_PATH = "bct.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_connection() as conn:
        conn.execute("PRAGMA journal_mode=WAL")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS sensors (
                udid         TEXT PRIMARY KEY,
                major        TEXT NOT NULL,
                minor        TEXT NOT NULL,
                authority    TEXT NOT NULL,
                status       BOOLEAN,
                frame_status BOOLEAN,
                phase_status BOOLEAN,
                ttc_status   BOOLEAN,
                last_online  TEXT,
                last_checked TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                taken_at TEXT NOT NULL,
                data     TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)


def update_sensor_status(
    udid: str,
    status: bool,
    frame_status: bool = False,
    phase_status: bool = False,
    ttc_status: bool = False,
):
    now = datetime.now().isoformat()

    if status:
        last_online_time = now
    else:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT last_online FROM sensors WHERE udid = ?", (udid,)
            ).fetchone()
        last_online_time = row["last_online"] if row else None

    with get_connection() as conn:
        conn.execute(
            """
            UPDATE sensors
            SET status = ?, frame_status = ?, phase_status = ?, ttc_status = ?,
                last_online = ?, last_checked = ?
            WHERE udid = ?
            """,
            (status, frame_status, phase_status, ttc_status, last_online_time, now, udid),
        )


def get_setting(key: str) -> str | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = ?", (key,)
        ).fetchone()
    return row["value"] if row else None


def set_setting(key: str, value: str) -> None:
    with get_connection() as conn:
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?)"
            " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )


def insert_and_trim_snapshot(taken_at: str, data_json: str, keep_n: int) -> None:
    """Insert a new snapshot and trim to the most recent keep_n rows in one transaction."""
    with get_connection() as conn:
        conn.execute(
            "INSERT INTO snapshots (taken_at, data) VALUES (?, ?)",
            (taken_at, data_json),
        )
        conn.execute(
            "DELETE FROM snapshots WHERE id NOT IN"
            " (SELECT id FROM snapshots ORDER BY id DESC LIMIT ?)",
            (keep_n,),
        )


def get_recent_snapshots(n: int) -> list:
    """Return up to n most-recent snapshot rows, newest first."""
    with get_connection() as conn:
        return conn.execute(
            "SELECT id, taken_at, data FROM snapshots ORDER BY id DESC LIMIT ?",
            (n,),
        ).fetchall()


def trim_snapshots(keep_n: int) -> None:
    """Trim snapshots to the most recent keep_n rows (called immediately on retention change)."""
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM snapshots WHERE id NOT IN"
            " (SELECT id FROM snapshots ORDER BY id DESC LIMIT ?)",
            (keep_n,),
        )
