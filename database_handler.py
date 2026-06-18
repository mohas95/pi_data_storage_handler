import sqlite3
import json
from datetime import datetime, timezone
from pathlib import Path
import os, time
from influxdb_client_3 import InfluxDBClient3, Point
import threading
from dataclasses import dataclass


### Experimental Information
EXPERIMENTS_META_TABLE_CONTENT = """
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    experiment_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    crop TEXT NOT NULL,
    start_time TEXT NOT NULL,
    notes TEXT
"""

### Event Capture Tables

CAPTURE_EVENTS_TABLE_CONTENT = """
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    capture_id TEXT NOT NULL UNIQUE,
    experiment_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    logged_timestamp TEXT NOT NULL,
    notes TEXT,

    FOREIGN KEY(experiment_id) REFERENCES experiments(experiment_id)

"""

IMAGE_CAPTURE_TABLE_CONTENT ="""
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    capture_id TEXT NOT NULL,
    image_id TEXT NOT NULL,
    camera_id TEXT NOT NULL,
    file_name TEXT NOT NULL,
    image_type TEXT,
    width INTEGER,
    height INTEGER,
    size_bytes INTEGER,

    FOREIGN KEY(capture_id) REFERENCES capture_events(capture_id)

"""


POSE_CAPTURE_TABLE_CONTENT = """
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    capture_id TEXT NOT NULL,
    x_mm REAL  NOT NULL,
    y_mm REAL  NOT NULL,
    z_mm REAL  NOT NULL,
    raw_joints_json TEXT NOT NULL,
    pose_is_stale INTEGER DEFAULT 0,

    FOREIGN KEY(capture_id) REFERENCES capture_events(capture_id)
"""


SENSOR_CAPTURE_TABLE_CONTENT = """
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    capture_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    sensor_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,

    FOREIGN KEY(capture_id) REFERENCES capture_events(capture_id)
"""


### Continuous monitoring Tables

POSE_TABLE_CONTENT = """
    pose_id INTEGER PRIMARY KEY AUTOINCREMENT,
    experiment_id TEXT NOT NULL,
    x_mm REAL NOT NULL,
    y_mm REAL NOT NULL,
    z_mm REAL NOT NULL,
    raw_joints_json TEXT NOT NULL,
    pose_is_stale INTEGER DEFAULT 0,
    note TEXT,
    timestamp TEXT NOT NULL,
    FOREIGN KEY(experiment_id) REFERENCES experiments(experiment_id)
"""

SENSOR_TABLE_CONTENT = """
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    experiment_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    sensor_type TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    FOREIGN KEY(experiment_id) REFERENCES experiments(experiment_id)
"""

## PRESET TABLES
EXPERIMENTS_TABLE={"experiments" : EXPERIMENTS_META_TABLE_CONTENT}

CAPTURE_EVENTS_TABLE={"experiments" : EXPERIMENTS_META_TABLE_CONTENT,
                      "capture_events" : CAPTURE_EVENTS_TABLE_CONTENT}

POSE_EVENTS_TABLE={"experiments" : EXPERIMENTS_META_TABLE_CONTENT,
                   "capture_events" : CAPTURE_EVENTS_TABLE_CONTENT,
                   "pose_events" : POSE_CAPTURE_TABLE_CONTENT }

SENSOR_EVENTS_TABLE={"experiments" : EXPERIMENTS_META_TABLE_CONTENT,
                     "capture_events" : CAPTURE_EVENTS_TABLE_CONTENT,
                     "sensor_events" : SENSOR_CAPTURE_TABLE_CONTENT }

IMAGE_EVENTS_TABLE={"experiments" : EXPERIMENTS_META_TABLE_CONTENT,
                     "capture_events" : CAPTURE_EVENTS_TABLE_CONTENT,
                     "image_events" : IMAGE_CAPTURE_TABLE_CONTENT }

SENSORS_TABLE={"experiments" : EXPERIMENTS_META_TABLE_CONTENT,
               "sensor_continuous" : SENSOR_TABLE_CONTENT }

POSE_TABLE={"experiments" : EXPERIMENTS_META_TABLE_CONTENT,
            "pose_continuous" : POSE_TABLE_CONTENT}

DEFAULT_TABLES = {"experiments" : EXPERIMENTS_META_TABLE_CONTENT,
                  "capture_events" : CAPTURE_EVENTS_TABLE_CONTENT,
                  "pose_events" : POSE_CAPTURE_TABLE_CONTENT,
                  "sensor_events" : SENSOR_CAPTURE_TABLE_CONTENT,
                  "image_events" : IMAGE_CAPTURE_TABLE_CONTENT,
                  "sensor_continuous" : SENSOR_TABLE_CONTENT,
                  "pose_continuous" : POSE_TABLE_CONTENT,
                  }


@dataclass
class DataRoutine:
    thread: threading.Thread
    stop_event: threading.Event


class SQLiteDataHandler:
    def __init__(self, db_path, dict_of_tables = None, influxdb_client = None):
        
        self.db_path = Path(db_path)
        self.tables = dict_of_tables
        self.influxdb_client = influxdb_client
        
        self.active_data_routines = {}


        if(dict_of_tables):
            for table_name, table_content in self.tables.items():
                self.init_table(table_name, table_content)

    def connect_db(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row  # lets you access columns by name
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def init_table(self, table_name, table_content):

        table_content = self._add_auto_columns(table_content)

        with self.connect_db() as conn:
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {table_content}
                )
            """)

    def insert(self, table_name, data):
        logged_timestamp = datetime.now(timezone.utc).isoformat()
        data.setdefault("logged_timestamp", logged_timestamp)

        ok, message = self.data_compatible(table_name, data)

        if not ok:
            raise ValueError(message)

        data_keys = ", ".join(data.keys())
        place_holders = ", ".join("?" for _ in data)
        data_values = tuple(data.values())

        with self.connect_db() as conn:
            conn.execute(
                f"""
                INSERT INTO {table_name}
                ({data_keys})
                VALUES ({place_holders})
                """,
                data_values,
            )

    def _add_auto_columns(self, table_content):
        auto_columns = []

        if "logged_timestamp" not in table_content:
            auto_columns.append("logged_timestamp TEXT NOT NULL")

        if "uploaded" not in table_content:
            auto_columns.append("uploaded INTEGER NOT NULL DEFAULT 0")

        if not auto_columns:
            return table_content

        if "FOREIGN KEY" in table_content:
            before_fk, after_fk = table_content.split("FOREIGN KEY", 1)

            before_fk = before_fk.rstrip().rstrip(",")
            after_fk = "FOREIGN KEY" + after_fk.lstrip()

            return f"""
                {before_fk},
                {", ".join(auto_columns)},
                {after_fk}
            """

        return table_content.rstrip().rstrip(",") + ", " + ", ".join(auto_columns)

    def data_compatible(self, table_name, data):
        with self.connect_db() as conn:
            rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()

            if not rows:
                raise ValueError(f"Table does not exist: {table_name}")
            
            columns ={
                row["name"]:{
                    "type": row["type"],
                    "notnull": bool(row["notnull"]),
                    "default": row["dflt_value"],
                    "pk": bool(row["pk"]),
                }

                for row in rows
            }

            data_keys = set(data.keys())
            column_keys = set(columns.keys())

            extra_keys = data_keys-column_keys
            if extra_keys:
                return False, f"Extra columns not in table: {extra_keys}"
            
            missing_required = []

            for  col_name, info in columns.items():
                is_primary_key = info["pk"]
                has_default = info["default"] is not None

                if(info["notnull"] and not has_default and not is_primary_key and col_name not in data):
                    missing_required.append(col_name)

            if missing_required:
                return False, f"Missing required columns: {missing_required}"
            
            return True, "compatible"
    
    def get_unsynced_data(self, table_name,limit=50):
        with self.connect_db() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM {table_name}
                WHERE uploaded = 0
                ORDER BY logged_timestamp ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        return {"table": table_name,
                "unsynced_data":[dict(row) for row in rows]}

    def mark_uploaded(self, table_name, id):
        with self.connect_db() as conn:
            conn.execute(
                f"""
                UPDATE {table_name}
                SET uploaded = 1
                WHERE id = ?
                """,
                (id,),
            )

    def init_influxdb_client(self, influxdb_client):
        self.influxdb_client = influxdb_client
        
    def write_to_influxdb(self, bucket, measurement_group, payload, tags, timestamp=None):
        if not self.influxdb_client:
            print("No influxdb client set, please initialize")
            return False
        
        try:
            point = Point(measurement_group)

            for field_name, value in payload.items():
                point.field(field_name, value)

            for tag_name, tag in tags.items():
                point.tag(tag_name, tag)
            
            if timestamp:
                point.time(timestamp)
            
            self.influxdb_client.write(database=bucket, record=point)

            return True
        except Exception as e:
            print(f"failed to upload to influxdb client: {e}")
            return False

    def start(self, data_routine, run_rate_s = 1, routine_name = "data_routine"):
        self.kill(routine_name)

        stop_event = threading.Event()

        def _data_routine_loop():
            while not stop_event.is_set():
                try:
                    data_routine()
                    
                except Exception as e:
                    print(f"[{routine_name}]Data routine failed: {e}")
                
                stop_event.wait(run_rate_s)

        thread = threading.Thread(target=_data_routine_loop, daemon=True, name=routine_name)
        try:
            thread.start()
            self.active_data_routines[routine_name] = DataRoutine(thread=thread, stop_event=stop_event)

        except Exception as e:
            print(f"[{routine_name}]Data routine failed to start: {e}")




    def kill(self, routine_name):

        routine = self.active_data_routines.get(routine_name)

        if routine is None:
            return

        routine.stop_event.set()

        try:
            if threading.current_thread() is not routine.thread:
                routine.thread.join(timeout=5)
        except Exception as e:
            print(f"[{routine_name}]Data routine failed: {e}")
        self.active_data_routines.pop(routine_name, None)

    def kill_all(self):
        for routine_name in list(self.active_data_routines.keys()):
            self.kill(routine_name)
        


if __name__ == "__main__":


    db_path = "data/test1.db"
    data = {"device_id":"atmino",
            "sensor_type":"env",
            "timestamp":"atmino",
            "payload_json":"atmino",
            }

    sensor_handler = SQLiteDataHandler(db_path, DEFAULT_TABLES)

    sensor_handler.insert("sensor_readings", data)

    unsynced_sensor_data = sensor_handler.get_unsynced_data("sensor_readings")

    # print(unsynced_sensor_data)
    for data_point in unsynced_sensor_data.get("unsynced_data"):
        id = data_point.get("id")

        if id %2 == 1:
            sensor_handler.mark_uploaded(unsynced_sensor_data.get("table"), id)

    # with sqlite3.connect(db_path) as conn:
    #     conn.row_factory = sqlite3.Row

    #     rows = conn.execute(
    #         "SELECT * FROM sensor_readings"
    #     ).fetchall()

    #     for row in rows:
    #         print(dict(row))