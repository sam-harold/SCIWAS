import csv
import json
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

CSV_FILE = "weather_history.csv"
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_stream")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[s.strip() for s in BOOTSTRAP_SERVERS if s.strip()],
    group_id="sciwas-archiver",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

headers = [
    "timestamp_unix",
    "timestamp_iso",
    "city",
    "temp_c",
    "humidity_pct",
    "condition",
]

file_exists = os.path.exists(CSV_FILE)

print(f"[Archiver] ACTIVE. Saving data to {CSV_FILE} ...")

with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=headers)

    if not file_exists:
        writer.writeheader()
        f.flush()

    for msg in consumer:
        data = msg.value

        row = {
            "timestamp_unix": data.get("timestamp_unix", data.get("timestamp")),
            "timestamp_iso": data.get("timestamp_iso", ""),
            "city": data.get("city", ""),
            "temp_c": data.get("temp_c", data.get("temp")),
            "humidity_pct": data.get("humidity_pct", data.get("humidity")),
            "condition": data.get("condition", ""),
        }

        writer.writerow(row)
        f.flush()
        print(f"[Archiver] Saved {row['city']}")