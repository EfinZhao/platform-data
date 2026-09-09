#!/usr/bin/env python3

import logging
import signal
import sys
from pathlib import Path

from ingest import IngestService, sensors_from_csv

REPO_ROOT = Path(__file__).resolve().parent.parent
UDID_CSV = REPO_ROOT / "udid.csv"

STATUS_INTERVAL_S = 30.0

log = logging.getLogger("logger")


def main():
    sensors = sensors_from_csv(UDID_CSV)
    log.info(f"Loaded {len(sensors)} sensors from {UDID_CSV}")

    service = IngestService(sensors)

    def shutdown(signum, _frame):
        log.info(f"Signal {signum} received, shutting down...")
        service.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    service.start()

    try:
        while not service.wait(STATUS_INTERVAL_S):
            stats = service.stats()
            log.info(
                f"connected {stats['collectors_connected']}/{stats['sensors_configured']}  "
                f"msgs={stats['messages']}  buffered={stats['pending_rows']}  "
                f"queue={stats['queue_depth']}  "
                f"dropped={stats['dropped_rows_queue'] + stats['dropped_rows_buffer']}"
            )
    except KeyboardInterrupt:
        service.stop()


if __name__ == "__main__":
    main()
