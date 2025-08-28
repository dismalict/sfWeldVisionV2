import os
import cv2
import socket
import threading
import time
import subprocess
from datetime import datetime, timedelta
from configparser import ConfigParser
from flask import Flask, Response
import jetson_inference
import jetson_utils
import mysql.connector
from mysql.connector import Error

# ---------------- CONFIG ---------------- #
frame_rate = 40
camera_group = {}  # camera index -> Camera object
hostname = socket.gethostname()
app = Flask(__name__)

def read_db_config(filename='dbconfig.ini', section='database'):
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        for key, value in parser.items(section):
            db[key] = value.replace('%', '%%')  # escape % in passwords
    else:
        raise Exception(f"Section '{section}' not found in {filename}")
    return db

db_config = read_db_config()

# ---------------- CAMERA CLASS ---------------- #
class Camera:
    def __init__(self, station, sfvis, cap, model):
        self.station = station
        self.sfvis = sfvis
        self.cap = cap
        self.frame = None
        self.cuda_img = None
        self.detections = None
        self.model = model
        self.people_count = 0
        self.status = "Vacant"
        self.previous_status = "Vacant"
        self.time_started = None
        self.time_spent = None
        self.presence_total = 0
        self.presence_60 = 0
        self.presence_rate = 0
        self.pause = False
        self.checkpoint = None

# ---------------- UTILITIES ---------------- #
def findSFVISno(hostname):
    import re
    m = re.search(r'\d+', hostname)
    return m.group() if m else None

def get_camera_devices():
    result = subprocess.run(['v4l2-ctl', '--list-devices'], capture_output=True, text=True)
    devices = []
    if result.returncode == 0:
        for line in result.stdout.splitlines():
            if '/dev/video' in line:
                devices.append(int(''.join(filter(str.isdigit, line))))
    return devices

def initialize_camera(camera_id):
    cap = cv2.VideoCapture(camera_id)
    if not cap.isOpened():
        print(f"Error: Could not open camera {camera_id}")
        return None
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1080)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)
    cap.set(cv2.CAP_PROP_FPS, frame_rate)
    return cap

def initialize_model():
    return jetson_inference.detectNet("ssd-mobilenet-v2", threshold=0.5)

def get_people_count(detections):
    return sum(1 for d in detections if d.ClassID == 1 and d.Confidence > 0.6)

def get_workstation(sfvis, camera_place):
    return int(sfvis)*2 - 1 if camera_place == 1 else int(sfvis)*2

def get_workstation_status(people_count):
    return "Occupied" if people_count else "Vacant"

def get_formatted_time(elapsed_seconds):
    t = timedelta(seconds=elapsed_seconds)
    s = str(t)
    if len(s.split(':')) == 2:
        s = "00:" + s
    return s

def get_working_time(start):
    return get_formatted_time(time.time() - start)

# ---------------- DATABASE ---------------- #
def create_table(sfvis, station, db_config):
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        # Cam table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS sfvis_cam{station} (
                Timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                Workstation_Camera INT NOT NULL,
                Vision_System INT NOT NULL,
                Old_Status VARCHAR(45) NOT NULL,
                Period_Status_Last TIME(6) DEFAULT NULL,
                New_Status VARCHAR(45) NOT NULL,
                People_Count INT NOT NULL,
                Frame_Rate INT NOT NULL,
                Presence_Change_Total INT NOT NULL,
                Presence_Change_Rate INT NOT NULL
            )
        """)
        # SFVIS table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS sfvis{sfvis} (
                Timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                Workstation_Camera INT NOT NULL,
                Vision_System INT NOT NULL,
                Old_Status VARCHAR(45) NOT NULL,
                Period_Status_Last TIME(6) DEFAULT NULL,
                New_Status VARCHAR(45) NOT NULL,
                People_Count INT NOT NULL,
                Frame_Rate INT NOT NULL,
                Presence_Change_Total INT NOT NULL,
                Presence_Change_Rate INT NOT NULL
            )
        """)
        print(f"Tables for SFVIS {sfvis}, Cam {station} ready.")
    except mysql.connector.Error as e:
        print(f"MySQL Error: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def delete_old_records(cursor, connection, station, limit=10):
    cursor.execute(f"SELECT COUNT(*) FROM sfvis_cam{station};")
    row_count = cursor.fetchone()[0]
    if row_count > limit:
        cursor.execute(f"""
            DELETE FROM sfvis_cam{station}
            WHERE Timestamp = (
                SELECT Timestamp
                FROM (SELECT Timestamp FROM sfvis_cam{station} ORDER BY Timestamp ASC LIMIT 1) AS sub
            );
        """)
        connection.commit()

def publish_to_mysql(cam: Camera, db_config):
    def _publish():
        connection = None
        cursor = None
        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            timestamp = datetime.now()
            data = (
                timestamp, cam.station, cam.sfvis, cam.previous_status,
                cam.time_spent if cam.time_spent else None,
                cam.status, cam.people_count, frame_rate,
                cam.presence_total, cam.presence_rate
            )
            for table in [f"sfvis{cam.sfvis}", f"sfvis_cam{cam.station}"]:
                cursor.execute(f"""
                    INSERT INTO {table} 
                    (Timestamp, Workstation_Camera, Vision_System, Old_Status, Period_Status_Last, New_Status,
                     People_Count, Frame_Rate, Presence_Change_Total, Presence_Change_Rate)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, data)
            connection.commit()
            delete_old_records(cursor, connection, cam.station)
        except Exception as e:
            print(f"MySQL publish error: {e}")
        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()
    threading.Thread(target=_publish).start()

# ---------------- FLASK STREAMING ---------------- #
def generate_camera_feed(cam_id):
    cam = camera_group[cam_id]
    while True:
        if cam.frame is not None:
            cam.cuda_img = jetson_utils.cudaFromNumpy(cam.frame)
            cam.detections = cam.model.Detect(cam.cuda_img)
            jetson_utils.cudaDeviceSynchronize()
            frame = jetson_utils.cudaToNumpy(cam.cuda_img)
            ret, jpeg = cv2.imencode('.jpg', frame)
            if ret:
                yield (b'--frame\r\nContent-Type: image/jpeg\r\n\r\n' + jpeg.tobytes() + b'\r\n\r\n')

def create_camera_routes():
    for cam_id in camera_group.keys():
        route = f"/camera{cam_id+1}"
        app.route(route)(lambda cam_id=cam_id: Response(generate_camera_feed(cam_id),
                                                        mimetype='multipart/x-mixed-replace; boundary=frame'))
        print(f"Camera {cam_id+1} feed at {route}")

def start_flask():
    threading.Thread(target=lambda: app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)).start()

# ---------------- STATUS CHECK ---------------- #
def check_status(cam: Camera):
    if cam.status != cam.previous_status:
        if cam.status == "Occupied" and cam.previous_status == "Vacant":
            cam.time_started = time.time()
        elif cam.status == "Vacant" and cam.previous_status == "Occupied":
            cam.time_spent = get_working_time(cam.time_started)
        publish_to_mysql(cam, db_config)
        cam.previous_status = cam.status

# ---------------- MAIN ---------------- #
def main():
    sfvis = findSFVISno(hostname)
    model = initialize_model()

    device_ids = get_camera_devices()
    print(f"{len(device_ids)} cameras detected: {device_ids}")

    for i, dev in enumerate(device_ids):
        cap = initialize_camera(dev)
        if cap is None:
            continue
        camera_group[i] = Camera(get_workstation(sfvis, i+1), sfvis, cap, model)
        create_table(sfvis, camera_group[i].station, db_config)

    create_camera_routes()
    start_flask()

    # Continuous capture loop
    while True:
        for cam in camera_group.values():
            ret, frame = cam.cap.read()
            if ret:
                cam.frame = frame
                cam.people_count = get_people_count(cam.model.Detect(jetson_utils.cudaFromNumpy(frame)))
                cam.status = get_workstation_status(cam.people_count)
                check_status(cam)
        time.sleep(1/frame_rate)

if __name__ == "__main__":
    main()
