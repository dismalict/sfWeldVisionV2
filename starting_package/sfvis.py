import cv2
import socket
import jetson_inference
import jetson_utils
import mysql.connector
from mysql.connector import Error
from configparser import ConfigParser
import time
import subprocess
from datetime import datetime, timedelta
import threading
from flask import Flask, Response
from typing import List, Dict

frame_rate = 40
app = Flask(__name__)
frame1 = None
frame2 = None
camera_group: Dict[int, 'Camera'] = {}
local: Dict[int, int] = {}
hostname = socket.gethostname()

def read_db_config(filename='dbconfig.ini', section='database') -> Dict[str, str]:
    parser = ConfigParser(interpolation=None)  # Disable interpolation to allow % in password
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db[item[0]] = item[1]
    else:
        raise Exception(f'Section {section} not found in {filename}')
    return db

db_config = read_db_config()

class Camera:
    def __init__(self, station, sfvis, previous_status, time_spent, status, people_count,
                 frame_rate, presence_total, presence_60, presence_rate,
                 ret, frame, cap, time_started, first_time, pause, checkpoint, cuda_img, detections):
        self.station = station
        self.sfvis = sfvis
        self.previous_status = previous_status
        self.time_spent = time_spent
        self.status = status
        self.people_count = people_count
        self.frame_rate = frame_rate
        self.presence_total = presence_total
        self.presence_60 = presence_60
        self.presence_rate = presence_rate
        self.ret = ret
        self.frame = frame
        self.cap = cap
        self.time_started = time_started
        self.first_time = first_time
        self.pause = pause
        self.checkpoint = checkpoint
        self.cuda_img = cuda_img
        self.detections = detections
        self.jpeg = None

def findSFVISno(hostname: str) -> str:
    import re
    number_of_sfvis = re.search(r'\d+', hostname)
    return number_of_sfvis.group() if number_of_sfvis else None

def devices(camera_device: str) -> int:
    import re
    match = re.search(r'\d+', camera_device)
    return int(match.group()) if match else None

def get_camera_devices() -> List[str]:
    result = subprocess.run(['v4l2-ctl', '--list-devices'], capture_output=True, text=True)
    devices_list: List[str] = []
    if result.returncode == 0:
        lines = result.stdout.split('\n')
        for line in lines:
            if '/dev/video' in line:
                devices_list.append(line.strip())
    return devices_list

def place_cameras() -> int:
    camera_devices = get_camera_devices()
    camera_amount = int(len(camera_devices) / 2)
    print(f"{camera_amount} cameras connected to: {len(camera_devices)} devices")
    counter = 0
    for i, dev in enumerate(camera_devices):
        dev_id = devices(dev)
        if dev_id % 2 == 0:
            local[counter] = dev_id
            counter += 1
    return camera_amount

def initialize_camera(camera_id: int):
    cap = cv2.VideoCapture(camera_id)
    if not cap.isOpened():
        print(f"Error: Could not open camera {camera_id}.")
        return None
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1080)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)
    cap.set(cv2.CAP_PROP_FPS, frame_rate)
    return cap

def initialize_model():
    return jetson_inference.detectNet("ssd-mobilenet-v2", threshold=0.5)

def get_people_count(detections) -> int:
    return sum(1 for det in detections if det.ClassID == 1 and det.Confidence > 0.60)

def generate_frame(frame):
    while True:
        if frame is not None:
            ret, jpeg = cv2.imencode('.jpg', frame)
            if ret:
                yield (b'--frame\r\n'
                       b'Content-Type: image/jpeg\r\n\r\n' + jpeg.tobytes() + b'\r\n\r\n')

@app.route('/camera1')
def camera1_feed():
    return Response(generate_frame(frame1),
                    mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/camera2')
def camera2_feed():
    return Response(generate_frame(frame2),
                    mimetype='multipart/x-mixed-replace; boundary=frame')

def get_workstation(sfvis: str, camera_place: int) -> int:
    if camera_place == 1:
        return int(sfvis) * 2 - 1
    elif camera_place == 2:
        return int(sfvis) * 2
    return 0

def get_workstation_status(people_count: int) -> str:
    return "Occupied" if people_count != 0 else "Vacant"

def get_formatted_time(elapsed_seconds: float) -> str:
    elapsed_time = timedelta(seconds=elapsed_seconds)
    formatted_time = str(elapsed_time)
    if len(formatted_time.split(":")) == 2:
        formatted_time = "00:" + formatted_time
    return formatted_time

def get_working_time(start: float) -> str:
    elapsed_t = time.time() - start
    return get_formatted_time(elapsed_t)

def create_table(sfvis: str, station: int):
    connection = None
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        create_table_cam_query = f"""
        CREATE TABLE IF NOT EXISTS `sfvis_cam{station}` (
            `Timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            `Workstation_Camera` INT NOT NULL,
            `Vision_System` INT NOT NULL,
            `Old_Status` VARCHAR(45) NOT NULL,
            `Period_Status_Last` TIME(6) DEFAULT NULL,
            `New_Status` VARCHAR(45) NOT NULL,
            `People_Count` INT NOT NULL,
            `Frame_Rate` INT NOT NULL,
            `Presence_Change_Total` INT NOT NULL,
            `Presence_Change_Rate` INT NOT NULL
        )
        """
        cursor.execute(create_table_cam_query)
        print(f"Table `sfvis_cam{station}` is ready.")

        create_table_sfvis_query = f"""
        CREATE TABLE IF NOT EXISTS `sfvis{sfvis}` (
            `Timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            `Workstation_Camera` INT NOT NULL,
            `Vision_System` INT NOT NULL,
            `Old_Status` VARCHAR(45) NOT NULL,
            `Period_Status_Last` TIME(6) DEFAULT NULL,
            `New_Status` VARCHAR(45) NOT NULL,
            `People_Count` INT NOT NULL,
            `Frame_Rate` INT NOT NULL,
            `Presence_Change_Total` INT NOT NULL,
            `Presence_Change_Rate` INT NOT NULL
        )
        """
        cursor.execute(create_table_sfvis_query)
        print(f"Table `sfvis{sfvis}` is ready.")

    except Error as e:
        print(f"MySQL Error: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

def delete_oldest_record(cursor, connection, station: int):
    count_query = f"SELECT COUNT(*) FROM sfvis_cam{station}"
    cursor.execute(count_query)
    row_count = cursor.fetchone()[0]
    try:
        if row_count > 10:
            delete_query = f"""
            DELETE FROM sfvis_cam{station}
            WHERE Timestamp = (
                SELECT Timestamp FROM (
                    SELECT Timestamp FROM sfvis_cam{station} ORDER BY Timestamp ASC LIMIT 1
                ) AS subquery
            )
            """
            cursor.execute(delete_query)
            connection.commit()
            print(f"Oldest record deleted from sfvis_cam{station}.")
    except Error as e:
        print(f"Error deleting record from sfvis_cam{station}: {e}")
        connection.rollback()

def publish_to_mysql(people_count: int, station: int, time_spent: str, status: str,
                     previous_status: str, sfvis: str, presence_rate: int, presence_total: int):
    def publish():
        connection = None
        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            timestamp = datetime.now()

            # Determine table fields for time_spent
            if time_spent:
                time_field = "Period_Status_Last, "
                time_placeholder = "%s, "
                data = (timestamp, station, sfvis, previous_status, time_spent, status,
                        people_count, frame_rate, presence_total, presence_rate)
            else:
                time_field = ""
                time_placeholder = ""
                data = (timestamp, station, sfvis, previous_status, status,
                        people_count, frame_rate, presence_total, presence_rate)

            base_query = (
                f"INSERT INTO {{table}} "
                f"(Timestamp, Workstation_Camera, Vision_System, Old_Status, {time_field}New_Status, "
                f"People_Count, Frame_Rate, Presence_Change_Total, Presence_Change_Rate) "
                f"VALUES (%s, %s, %s, %s, {time_placeholder}%s, %s, %s, %s, %s)"
            )

            query_sfvis = base_query.format(table=f"sfvis{sfvis}", time_field=time_field, time_placeholder=time_placeholder)
            query_cam = base_query.format(table=f"sfvis_cam{station}", time_field=time_field, time_placeholder=time_placeholder)

            cursor.execute(query_sfvis, data)
            cursor.execute(query_cam, data)
            connection.commit()
            print(f"Published to MySQL: {people_count} people at Cam{station}")

            delete_oldest_record(cursor, connection, station)

        except Error as e:
            print(f"Database error: {e}")
        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()
    threading.Thread(target=publish).start()

def check_status(camera: Camera):
    if camera.status != camera.previous_status:
        if camera.status == "Occupied" and camera.previous_status == "Vacant":
            camera.time_started = time.time()
            publish_to_mysql(camera.people_count, camera.station, camera.time_spent,
                             camera.status, camera.previous_status, camera.sfvis,
                             camera.presence_rate, camera.presence_total)
            time.sleep(0.5)
            camera.previous_status = "Occupied"
        elif camera.status == "Vacant" and camera.previous_status == "Occupied":
            camera.presence_rate += 1
            camera.time_spent = get_working_time(camera.time_started)
            publish_to_mysql(camera.people_count, camera.station, camera.time_spent,
                             camera.status, camera.previous_status, camera.sfvis,
                             camera.presence_rate, camera.presence_total)
            time.sleep(0.5)
            camera.previous_status = "Vacant"
            camera.time_started = None
            camera.time_spent = None

def regular_post(camera: Camera, check_time: int):
    if check_time % 60 == 0:
        camera.presence_total += camera.presence_rate
        camera.presence_60 = camera.presence_rate
        camera.presence_rate = 0
    publish_to_mysql(camera.people_count, camera.station, None, camera.status,
                     camera.previous_status, camera.sfvis,
                     camera.presence_60, camera.presence_total)
    camera.pause = True

def main():
    sfvis = findSFVISno(hostname)
    model = initialize_model()
    global frame1, frame2

    camera_amount = place_cameras()
    for i in range(camera_amount):
        print(f"Camera {i+1} is positioned in: /dev/video{local[i]}")

    try:
        for i in range(camera_amount):
            cap = initialize_camera(local[i])
            if cap is None:
                print(f"Skipping camera {i+1} due to initialization error.")
                continue
            camera_group[i] = Camera(
                get_workstation(sfvis, i+1), sfvis, "Vacant", None, "Vacant", 0,
                frame_rate, 0, 0, 0, None, None, cap, None, True, False, None, None, None
            )
            create_table(sfvis, camera_group[i].station)
    except Error as e:
        print(f"Error during camera setup: {e}")

    threading.Thread(target=lambda: app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)).start()

    overall_time = time.time()

    while True:
        for i in range(camera_amount):
            cam = camera_group[i]
            ret, frame = cam.cap.read()
            if not ret:
                print(f"Error: Failed to read from camera {i+1}.")
                continue
            if i == 0:
                frame1 = frame
            elif i == 1:
                frame2 = frame
            cam.ret, cam.frame = ret, frame
            cam.cuda_img = jetson_utils.cudaFromNumpy(cam.frame)
            cam.detections = model.Detect(cam.cuda_img)
            cam.people_count = get_people_count(cam.detections)
            cam.status = get_workstation_status(cam.people_count)
            check_status(cam)
            cam.check_time = int(time.time() - overall_time)
            if not cam.pause:
                if cam.check_time % 20 == 0:
                    cam.checkpoint = time.time()
                    regular_post(cam, cam.check_time)
            if cam.checkpoint is not None:
                testing = time.time() - cam.checkpoint
                if testing >= 1.0:
                    cam.pause = False
                    cam.checkpoint = None

        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

    for i in range(camera_amount):
        camera_group[i].cap.release()
    cv2.destroyAllWindows()

if __name__ == "__main__":
    main()
