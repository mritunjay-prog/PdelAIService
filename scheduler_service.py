"""
scheduler_service.py
====================
Polls `stream_schedule` for upcoming bookings, resolves the camera_id
from `camera_info`, then schedules two shell commands per booking:

  START  →  stream_start  - 20 seconds
            cd /home/administrator/Edge_computing-Padel
            sudo CAM_ID=<X> BOOKING_ID=<Y> -E docker compose -p padel_<X>_<Y> up -d

  STOP   →  stream_end    + 30 seconds
            cd /home/administrator/Edge_computing-Padel
            sudo CAM_ID=<X> BOOKING_ID=<Y> bash -c 'expect -c "..."
            && sudo docker rm padel_tracker_<X>_<Y>'
"""

import logging
import subprocess
import time
from datetime import datetime, timedelta

import mysql.connector
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger

from db_service import load_config, get_connection

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)-8s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scheduler")

# ─────────────────────────────────────────────
# Constants (override in config.properties if needed)
# ─────────────────────────────────────────────
EDGE_DIR          = "/home/administrator/Edge_computing-Padel"
START_OFFSET_SEC  = -20   # fire 20 s before stream_start
STOP_OFFSET_SEC   = 30    # fire 30 s after  stream_end
POLL_INTERVAL_SEC = 60    # how often to re-query the DB


# ─────────────────────────────────────────────
# Shell commands
# ─────────────────────────────────────────────

def _run(cmd: str, label: str) -> None:
    """Run a shell command and log the result."""
    log.info(f"[{label}] Executing:\n  {cmd}")
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True
        )
        if result.returncode == 0:
            log.info(f"[{label}] ✅ Success\n  {result.stdout.strip()}")
        else:
            log.error(
                f"[{label}] ❌ Failed (exit {result.returncode})\n"
                f"  stderr: {result.stderr.strip()}"
            )
    except Exception as exc:
        log.exception(f"[{label}] ❌ Exception: {exc}")


def run_start_cmd(cam_id: int, booking_id: int) -> None:
    """
    START command:
        cd <EDGE_DIR>
        sudo CAM_ID=<cam_id> BOOKING_ID=<booking_id> -E \
            docker compose -p padel_<cam_id>_<booking_id> up -d
    """
    cmd = (
        f"cd {EDGE_DIR} && "
        f"sudo CAM_ID={cam_id} BOOKING_ID={booking_id} -E "
        f"docker compose -p padel_{cam_id}_{booking_id} up -d"
    )
    _run(cmd, f"START cam={cam_id} booking={booking_id}")


def run_stop_cmd(cam_id: int, booking_id: int) -> None:
    """
    STOP command:
        cd <EDGE_DIR>
        sudo CAM_ID=<cam_id> BOOKING_ID=<booking_id> bash -c '
          expect -c "
            spawn sudo docker attach padel_tracker_<cam_id>_<booking_id>
            sleep 2
            send \"q\\r\"
            set timeout 180
            expect \"Success! Data sent successfully.\"
            sleep 45
          " && sudo docker rm padel_tracker_<cam_id>_<booking_id>
        '
    """
    project      = f"padel_tracker_{cam_id}_{booking_id}"
    expect_inner = (
        f'spawn sudo docker attach {project}\\n'
        f'sleep 2\\n'
        f'send \\"q\\\\r\\"\\n'
        f'set timeout 180\\n'
        f'expect \\"Success! Data sent successfully.\\"\\n'
        f'sleep 45'
    )
    cmd = (
        f"cd {EDGE_DIR} && "
        f"sudo CAM_ID={cam_id} BOOKING_ID={booking_id} bash -c '"
        f'expect -c "{expect_inner}" && '
        f"sudo docker rm {project}'"
    )
    _run(cmd, f"STOP  cam={cam_id} booking={booking_id}")


# ─────────────────────────────────────────────
# DB queries
# ─────────────────────────────────────────────

def fetch_upcoming_bookings(conn) -> list[dict]:
    """
    Return all rows from stream_schedule whose stream_end is still
    in the future (so we can still schedule either the start or the stop).
    """
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT booking_id, court_id, stream_start, stream_end
        FROM   stream_schedule
        WHERE  stream_end > NOW()
        ORDER  BY stream_start ASC
        """
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def fetch_camera_id(conn, court_id: int) -> int | None:
    """Return camera_id from camera_info for the given court_id."""
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT camera_id FROM camera_info WHERE court_id = %s LIMIT 1",
        (court_id,),
    )
    row = cur.fetchone()
    cur.close()
    return int(row["camera_id"]) if row else None


# ─────────────────────────────────────────────
# Scheduler logic
# ─────────────────────────────────────────────

# Track what we've already scheduled so we don't double-schedule
_scheduled_starts: set[int] = set()   # booking_ids with start job queued
_scheduled_stops:  set[int] = set()   # booking_ids with stop  job queued


def poll_and_schedule(scheduler: BackgroundScheduler, config: dict) -> None:
    """Called every POLL_INTERVAL_SEC — checks DB and schedules new jobs."""
    log.info("🔍 Polling stream_schedule for upcoming bookings …")

    try:
        conn = get_connection(config)
    except Exception as exc:
        log.error(f"DB connection failed during poll: {exc}")
        return

    try:
        bookings = fetch_upcoming_bookings(conn)
        log.info(f"   Found {len(bookings)} upcoming booking(s).")

        now = datetime.now()

        for b in bookings:
            booking_id  = int(b["booking_id"])
            court_id    = int(b["court_id"])
            stream_start: datetime = b["stream_start"]
            stream_end:   datetime = b["stream_end"]

            # ── resolve camera_id ──────────────────────────────────────
            cam_id = fetch_camera_id(conn, court_id)
            if cam_id is None:
                log.warning(
                    f"   ⚠️  booking_id={booking_id}: no camera_info row "
                    f"for court_id={court_id} — skipping."
                )
                continue

            log.info(
                f"   📅 booking_id={booking_id} | court_id={court_id} | "
                f"cam_id={cam_id} | start={stream_start} | end={stream_end}"
            )

            # ── schedule START ─────────────────────────────────────────
            start_fire = stream_start + timedelta(seconds=START_OFFSET_SEC)

            if booking_id not in _scheduled_starts:
                if start_fire > now:
                    scheduler.add_job(
                        func=run_start_cmd,
                        trigger=DateTrigger(run_date=start_fire),
                        args=[cam_id, booking_id],
                        id=f"start_{booking_id}",
                        replace_existing=True,
                        misfire_grace_time=60,
                    )
                    log.info(
                        f"   ✅  START scheduled at {start_fire}  "
                        f"(stream_start - 20s)"
                    )
                else:
                    log.info(
                        f"   ⏩  booking_id={booking_id}: START time "
                        f"{start_fire} already passed — skipping start."
                    )
                _scheduled_starts.add(booking_id)

            # ── schedule STOP ──────────────────────────────────────────
            stop_fire = stream_end + timedelta(seconds=STOP_OFFSET_SEC)

            if booking_id not in _scheduled_stops:
                if stop_fire > now:
                    scheduler.add_job(
                        func=run_stop_cmd,
                        trigger=DateTrigger(run_date=stop_fire),
                        args=[cam_id, booking_id],
                        id=f"stop_{booking_id}",
                        replace_existing=True,
                        misfire_grace_time=120,
                    )
                    log.info(
                        f"   ✅  STOP  scheduled at {stop_fire}  "
                        f"(stream_end + 30s)"
                    )
                _scheduled_stops.add(booking_id)

    finally:
        conn.close()


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print("  PdelAIService — Stream Scheduler")
    print("=" * 60)

    config = load_config("config.properties")

    # Read optional overrides from config
    poll_interval = int(
        config.get("scheduler.poll.interval.seconds", POLL_INTERVAL_SEC)
    )
    log.info(f"Poll interval: every {poll_interval}s")

    # Start APScheduler
    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.start()

    log.info("Scheduler started. Running first poll immediately …")

    try:
        while True:
            poll_and_schedule(scheduler, config)
            log.info(f"⏳ Sleeping {poll_interval}s until next poll …\n")
            time.sleep(poll_interval)

    except KeyboardInterrupt:
        log.info("🛑 Shutting down scheduler …")
        scheduler.shutdown(wait=False)
        log.info("Goodbye.")


if __name__ == "__main__":
    main()
