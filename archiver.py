import json
import csv
import os
from kafka import KafkaConsumer

CSV_FILE = "weather_history.csv"
BOOTSTRAP_SERVERS = ["localhost:9092"]

consumer = KafkaConsumer(
    "weather_stream",
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

# Create CSV with headers if missing
if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Timestamp", "City", "Temperature", "Condition", "Humidity"])

print("ARCHIVER ACTIVE. Saving data to CSV...")

for message in consumer:
    data = message.value
    with open(CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                data["timestamp"],
                data["city"],
                data["temp"],
                data["condition"],
                data["humidity"],
            ]
        )
    print(f"[Saved] {data['city']}")
