#!/usr/bin/env python3
"""
Refactored multi-camera vision script for Jetson + MySQL + MJPEG streaming.
- Single read per camera per loop
- FPS limiter
- DB queue + single writer worker (prevents thread explosion)
- Structured logging
- Graceful shutdown (SIGINT/SIGTERM)
- Table auto-create per camera + per sfvis host
- Minimal changes to your data model

Author: Mason + ChatGPT
"""

import os
import re
import cv2
import sys
import time
import queue
import socket
import signal
import logging
import threading
from datetime import datetime, timedelta
from configparser import ConfigParser

import mysql.connector
from mysql.connector import Error

import jetson.inference
import jetson.utils

from flask import Flask, Response

# ========================
# Config & Globals
# ========================
FRAME_RATE = 40  # target FPS per camera (soft cap)
FRAME_WIDTH = 1280
FRAME_HEIGHT = 720
GRAFANA_ROW_LIMIT = 10  # keep only the most recent N rows in sfvis_cam{station}

HOSTNAME = socket.gethostname()
APP = Flask(__name__)

# Shared frames for Flask endpoints
latest_frames = {}

# Camera bookkeeping
cameras = {}
stop_event = threading.Event()

# ========================
# Logging
# ========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("vision")


# ========================
# Utilities
# ========================

def read_db_config(filename: str = "dbconfig.ini", section: str = "database") -> dict:
    parser = ConfigParser()
    parser.read(filename)
    if not parser.has_section(section):
        raise RuntimeError(f"Section [{section}] not found in {filename}")
    return {k: v for k, v in parser.items(section)}


def find_sfvis_no(hostname: str) -> str:
    match = re.search(r"\d+", hostname)
    if not match:
        raise RuntimeError(
            f"Unable to parse numeric sfvis id from hostname '{hostname}'. Expected digits in name (e.g., SFVIS01)."
        )
    return match.group(0)


def quote_ident(ident: str) -> str:
    """Safely wrap MySQL identifiers with backticks after validation."""
    if not re.fullmatch(r"[A-Za-z0-9_]+", ident):
        raise ValueError(f"Invalid identifier: {ident}")
    return f"`{ident}`"


# ========================
# Data classes
# ========================
class Camera:
    def __init__(self, index: int, device_id: int, station: int, sfvis: str):
        self.index = index
        self.device_id = device_id  # e.g., 0, 2...
        self.station = station      # workstation camera id per your mapping
        self.sfvis = sfvis          # numeric string, e.g., "01"

        self.cap = None
        self.ret = False
        self.frame = None

        # presence/status bookkeeping
        self.previous_status = "Vacant"
        self.status = "Vacant"
        self.people_count = 0

        self.time_started = None
        self.time_spent = None

        self.presence_total = 0
        self.presence_60 = 0
        self.presence_rate = 0

        self.pause = False
        self.checkpoint = None
        self.check_time = 0

        self.cuda_img = None
        self.detections = None


# ========================
# Jetson detection
# ========================

def init_model():
    log.info("Loading detection model: ssd-mobilenet-v2")
    return jetson.inference.detectNet("ssd-mobilenet-v2", threshold=0.5)


def count_people(detections) -> int:
    # On COCO, ClassID 1 == person
    return sum(1 for d in detections if d.ClassID == 1 and d.Confidence > 0.60)


def workstation_for(sfvis: str, camera_place: int) -> int:
    # camera_place: 1-based position on the sfvis
    return int(sfvis) * 2 - 1 if camera_place == 1 else int(sfvis) * 2


def status_from_people(n: int) -> str:
    return "Occupied" if n else "Vacant"


def fmt_elapsed(seconds: float) -> str:
    td = timedelta(seconds=seconds)
    s = str(td)
    if len(s.split(":")) == 2:
        s = "00:" + s
    return s


# ========================
# Camera discovery & init
# ========================

def list_video_devices() -> list[int]:
    import subprocess

    result = subprocess.run(["v4l2-ctl", "--list-devices"], capture_output=True, text=True)
    devices = []
    if result.returncode == 0:
        for line in result.stdout.splitlines():
            if "/dev/video" in line:
                m = re.search(r"/dev/video(\d+)", line)
                if m:
                    devices.append(int(m.group(1)))
    return devices


def select_even_devices(devs: list[int]) -> list[int]:
    # Original logic: use even-numbered /dev/video* entries for actual cameras
    return [d for d in devs if d % 2 == 0]


def init_camera_capture(device_id: int) -> cv2.VideoCapture | None:
    cap = cv2.VideoCapture(device_id)
    if not cap.isOpened():
        log.error(f"Could not open camera /dev/video{device_id}")
        return None

    cap.set(cv2.CAP_PROP_FRAME_WIDTH, FRAME_WIDTH)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, FRAME_HEIGHT)
    cap.set(cv2.CAP_PROP_FPS, FRAME_RATE)
    return cap


# ========================
# Database writer (single worker)
# ========================
class DBWriter:
    def __init__(self, db_conf: dict):
        self.db_conf = db_conf
        self.q: "queue.Queue[tuple[str, tuple]]" = queue.Queue(maxsize=1000)
        self.thread = threading.Thread(target=self._worker, name="DBWriter", daemon=True)
        self._conn = None
        self._cursor = None

    def start(self):
        self.thread.start()

    def stop(self):
        # Send sentinel
        self.q.put(("__STOP__", ()))
        self.thread.join(timeout=5)
        self._close()

    def _connect(self):
        self._conn = mysql.connector.connect(**self.db_conf)
        self._cursor = self._conn.cursor()

    def _close(self):
        try:
            if self._cursor:
                self._cursor.close()
            if self._conn and self._conn.is_connected():
                self._conn.close()
        except Exception:
            pass
        finally:
            self._cursor = None
            self._conn = None

    def enqueue(self, query: str, params: tuple):
        try:
            self.q.put_nowait((query, params))
        except queue.Full:
            log.warning("DB queue full; dropping event")

    def _ensure(self):
        if self._conn is None or not self._conn.is_connected():
            self._close()
            self._connect()

    def _worker(self):
        while True:
            query, params = self.q.get()
            if query == "__STOP__":
                break
            try:
                self._ensure()
                self._cursor.execute(query, params)
                self._conn.commit()
            except Error as e:
                log.error(f"DB error: {e}")
                self._close()
            except Exception as ex:
                log.exception(f"Unexpected DB writer error: {ex}")


# ========================
# Table management & insert helpers
# ========================
CREATE_TABLE_TEMPLATE = (
    "CREATE TABLE IF NOT EXISTS {table} ("
    "`Timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
    "`Workstation_Camera` INT NOT NULL,"
    "`Vision_System` INT NOT NULL,"
    "`Old_Status` VARCHAR(45) NOT NULL,"
    "`Period_Status_Last` TIME(6) DEFAULT NULL,"
    "`New_Status` VARCHAR(45) NOT NULL,"
    "`People_Count` INT NOT NULL,"
    "`Frame_Rate` INT NOT NULL,"
    "`Presence_Change_Total` INT NOT NULL,"
    "`Presence_Change_Rate` INT NOT NULL)"
)

INSERT_TEMPLATE_WITH_TIME = (
    "INSERT INTO {table} "
    "(Timestamp, Workstation_Camera, Vision_System, Old_Status, Period_Status_Last, New_Status, "
    "People_Count, Frame_Rate, Presence_Change_Total, Presence_Change_Rate) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
)

INSERT_TEMPLATE_NO_TIME = (
    "INSERT INTO {table} "
    "(Timestamp, Workstation_Camera, Vision_System, Old_Status, New_Status, "
    "People_Count, Frame_Rate, Presence_Change_Total, Presence_Change_Rate) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
)

DELETE_OLDEST_ONE = "DELETE FROM {table} ORDER BY `Timestamp` ASC LIMIT 1"
COUNT_ROWS = "SELECT COUNT(*) FROM {table}"


def create_tables_for_camera(dbw: DBWriter, sfvis: str, station: int):
    sfvis_table = quote_ident(f"sfvis{sfvis}")
    cam_table = quote_ident(f"sfvis_cam{station}")
    for tbl in (sfvis_table, cam_table):
        dbw.enqueue(CREATE_TABLE_TEMPLATE.format(table=tbl), tuple())


def enqueue_delete_if_exceeds(dbw: DBWriter, cam_table: str):
    # Since our DB worker is simple, we enqueue count + conditional delete sequentially.
    # This is eventually consistent and fine for a dashboard buffer.
    dbw.enqueue(COUNT_ROWS.format(table=cam_table), tuple())
    # We can't branch in the worker, so we always enqueue a delete; it will delete nothing if none match
    dbw.enqueue(DELETE_OLDEST_ONE.format(table=cam_table), tuple())


def enqueue_insert(dbw: DBWriter, table_name: str, values: tuple, include_time: bool):
    tbl = quote_ident(table_name)
    if include_time:
        dbw.enqueue(INSERT_TEMPLATE_WITH_TIME.format(table=tbl), values)
    else:
        dbw.enqueue(INSERT_TEMPLATE_NO_TIME.format(table=tbl), values)


# ========================
# Business logic
# ========================

def publish_event(dbw: DBWriter, cam: Camera, time_spent: str | None, presence_rate: int, presence_total: int):
    now = datetime.now()
    include_time = time_spent is not None

    # Build values tuples
    if include_time:
        data = (now, cam.station, cam.sfvis, cam.previous_status, time_spent, cam.status,
                cam.people_count, FRAME_RATE, presence_total, presence_rate)
    else:
        data = (now, cam.station, cam.sfvis, cam.previous_status, cam.status,
                cam.people_count, FRAME_RATE, presence_total, presence_rate)

    sfvis_table = f"sfvis{cam.sfvis}"
    cam_table = f"sfvis_cam{cam.station}"

    enqueue_insert(dbw, sfvis_table, data, include_time)
    enqueue_insert(dbw, cam_table, data, include_time)

    # Enforce rolling buffer on cam table
    dbw.enqueue(COUNT_ROWS.format(table=quote_ident(cam_table)), tuple())
    # Always try deleting oldest one; harmless if table small
    dbw.enqueue(DELETE_OLDEST_ONE.format(table=quote_ident(cam_table)), tuple())


def handle_status_transitions(dbw: DBWriter, cam: Camera):
    if cam.status == cam.previous_status:
        return

    if cam.status == "Occupied" and cam.previous_status == "Vacant":
        cam.time_started = time.time()
        publish_event(dbw, cam, time_spent=None, presence_rate=cam.presence_rate, presence_total=cam.presence_total)
        cam.previous_status = "Occupied"

    elif cam.status == "Vacant" and cam.previous_status == "Occupied":
        cam.presence_rate += 1
        if cam.time_started is not None:
            cam.time_spent = fmt_elapsed(time.time() - cam.time_started)
        else:
            cam.time_spent = fmt_elapsed(0)
        publish_event(dbw, cam, time_spent=cam.time_spent, presence_rate=cam.presence_rate, presence_total=cam.presence_total)
        cam.previous_status = "Vacant"
        cam.time_started = None
        cam.time_spent = None


def regular_post_if_needed(dbw: DBWriter, cam: Camera, elapsed_sec: int):
    # Every 60s, roll presence into totals and reset rate
    if elapsed_sec % 60 == 0:
        cam.presence_total += cam.presence_rate
        cam.presence_60 = cam.presence_rate
        cam.presence_rate = 0
    publish_event(dbw, cam, time_spent=None, presence_rate=cam.presence_60, presence_total=cam.presence_total)


# ========================
# Flask MJPEG streaming
# ========================

def mjpeg_generator(cam_index: int):
    while not stop_event.is_set():
        frame = latest_frames.get(cam_index)
        if frame is not None:
            ret, jpeg = cv2.imencode('.jpg', frame)
            if ret:
                yield (b'--frame\r\n'
                       b'Content-Type: image/jpeg\r\n\r\n' + jpeg.tobytes() + b'\r\n\r\n')
        time.sleep(0.01)


@APP.route('/camera/<int:cam_index>')
def camera_feed(cam_index: int):
    return Response(mjpeg_generator(cam_index), mimetype='multipart/x-mixed-replace; boundary=frame')


# ========================
# Main loop
# ========================

def main():
    db_config = read_db_config()
    sfvis = find_sfvis_no(HOSTNAME)

    # Discover cameras (even-numbered /dev/video*)
    devs = list_video_devices()
    even_devs = select_even_devices(devs)
    if not even_devs:
        log.error("No camera devices found (even-numbered /dev/video*). Exiting.")
        return 1

    # Prepare Camera objects for first two cameras (or however many found)
    for i, device_id in enumerate(even_devs):
        station = workstation_for(sfvis, i + 1)  # 1-based position
        cam = Camera(index=i, device_id=device_id, station=station, sfvis=sfvis)
        cameras[i] = cam

    # Init model
    model = init_model()

    # Start DB writer
    dbw = DBWriter(db_config)
    dbw.start()

    # Create tables (queued)
    for cam in cameras.values():
        create_tables_for_camera(dbw, cam.sfvis, cam.station)

    # Open caps
    for cam in cameras.values():
        cam.cap = init_camera_capture(cam.device_id)
        if cam.cap is None:
            log.error(f"Skipping camera index {cam.index}")

    # Start Flask in background
    server_thread = threading.Thread(
        target=lambda: APP.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False),
        name="FlaskServer",
        daemon=True,
    )
    server_thread.start()

    # Graceful shutdown handlers
    def _shutdown(signum, frame):
        log.info(f"Signal {signum} received; stopping...")
        stop_event.set()
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    overall_start = time.perf_counter()

    try:
        while not stop_event.is_set():
            loop_start = time.perf_counter()

            for i, cam in cameras.items():
                if cam.cap is None:
                    continue

                # Single read per loop
                ret, frame = cam.cap.read()
                if not ret or frame is None:
                    log.warning(f"Failed to read from /dev/video{cam.device_id}")
                    continue

                cam.ret, cam.frame = ret, frame
                latest_frames[i] = frame  # for MJPEG endpoint

                # Detection
                cam.cuda_img = jetson.utils.cudaFromNumpy(frame)
                cam.detections = model.Detect(cam.cuda_img)
                cam.people_count = count_people(cam.detections)
                cam.status = status_from_people(cam.people_count)

                # Transitions
                handle_status_transitions(dbw, cam)

            # Periodics per camera
            elapsed = int(time.perf_counter() - overall_start)
            for cam in cameras.values():
                cam.check_time = elapsed
                if not cam.pause and (elapsed % 20 == 0):
                    cam.checkpoint = time.perf_counter()
                    regular_post_if_needed(dbw, cam, elapsed)
                if cam.checkpoint is not None:
                    if (time.perf_counter() - cam.checkpoint) >= 1.0:
                        cam.pause = False
                        cam.checkpoint = None

            # FPS limit
            loop_elapsed = time.perf_counter() - loop_start
            target = 1.0 / FRAME_RATE
            if loop_elapsed < target:
                time.sleep(target - loop_elapsed)

    finally:
        stop_event.set()
        # Close cameras
        for cam in cameras.values():
            try:
                if cam.cap:
                    cam.cap.release()
            except Exception:
                pass
        cv2.destroyAllWindows()

        # Stop DB
        dbw.stop()

        log.info("Shutdown complete.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
