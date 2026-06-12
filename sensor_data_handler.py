import sqlite3
import json
from datetime import datetime, timezone
from pathlib import Path

SENSOR_TABLE_CONTENT = """
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL,
    sensor_type TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    uploaded INTEGER NOT NULL DEFAULT 0
"""


DEFAULT_TABLES = {"sensor_readings":SENSOR_TABLE_CONTENT}


class SQLiteDataHandler:
    def __init__(self, db_path, dict_of_tables = None):
        
        self.db_path = Path(db_path)
        self.tables = dict_of_tables

        if(dict_of_tables):
            for table_name, table_content in self.tables.items():
                self.init_table(table_name, table_content)

    def connect_db(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # lets you access columns by name
        return conn

    def init_table(self, table_name, table_content):

        if "logged_timestamp" not in table_content:
            table_content += ", logged_timestamp TEXT NOT NULL"

        if "uploaded" not in table_content:
            table_content += ", uploaded INTEGER NOT NULL DEFAULT 0"

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
    
    # TODO: 
    def get_unsynced_readings(self,limit=50):
        pass

    def mark_uploaded(self):
        pass


if __name__ == "__main__":

    db_path = "data/test1.db"
    data = {"device_id":"atmino",
            "sensor_type":"env",
            "timestamp":"atmino",
            "payload_json":"atmino",
            }


    sensor_handler = SQLiteDataHandler(db_path, DEFAULT_TABLES)

    sensor_handler.insert("sensor_readings", data)


    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row

        rows = conn.execute(
            "SELECT * FROM sensor_readings"
        ).fetchall()

        for row in rows:
            print(dict(row))