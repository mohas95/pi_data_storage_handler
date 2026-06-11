import sqlite3
import json
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path("data/sensors.db")


def connect_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # lets you access columns by name
    return conn


def init_db():
    with connect_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                sensor_type TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                uploaded INTEGER NOT NULL DEFAULT 0
            )
        """)


def insert_reading(device_id, sensor_type, payload):
    timestamp = datetime.now(timezone.utc).isoformat()

    with connect_db() as conn:
        conn.execute(
            """
            INSERT INTO sensor_readings
            (device_id, sensor_type, timestamp, payload_json)
            VALUES (?, ?, ?, ?)
            """,
            (
                device_id,
                sensor_type,
                timestamp,
                json.dumps(payload),
            ),
        )


def get_unsynced_readings(limit=50):
    with connect_db() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM sensor_readings
            WHERE uploaded = 0
            ORDER BY timestamp ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return [dict(row) for row in rows]


def mark_uploaded(reading_id):
    with connect_db() as conn:
        conn.execute(
            """
            UPDATE sensor_readings
            SET uploaded = 1
            WHERE id = ?
            """,
            (reading_id,),
        )


if __name__ == "__main__":
    init_db()

    insert_reading(
        device_id="pico_01",
        sensor_type="environment",
        payload={
            "temperature_C": 23.4,
            "humidity_%": 61.2,
            "co2_ppm": 812,
            "o2_%": 20.8,
        },
    )

    unsynced = get_unsynced_readings()

    for row in unsynced:
        print(row)

        # pretend cloud upload succeeded
        mark_uploaded(row["id"])