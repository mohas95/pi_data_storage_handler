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
        self.tables = dict_of_tables or DEFAULT_TABLES

        for table_name, table_content in self.tables.items():
            self.init_table(table_name, table_content)

    def connect_db(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # lets you access columns by name
        return conn

    def init_table(self, table_name, table_content):
        with self.connect_db() as conn:
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {table_content}
                )
            """)