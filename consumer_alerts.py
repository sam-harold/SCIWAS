import json
import os
import time
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

load_dotenv()

# --- CONFIG ---
RAW_TOPIC = os.getenv("KAFKA_TOPIC", "weather_stream")
ALERT_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "weather_alerts")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")

# --- KAFKA CONSUMER (raw weather) ---
consumer = KafkaConsumer(
    RAW_TOPIC,
    bootstrap_servers=[s.strip() for s in BOOTSTRAP_SERVERS if s.strip()],
    group_id="sciwas-alert-engine",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

# --- KAFKA PRODUCER (alerts) ---
producer = KafkaProducer(
    bootstrap_servers=[s.strip() for s in BOOTSTRAP_SERVERS if s.strip()],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

# --- ALERT RULES ---
WET_CONDITIONS = {"Rain", "Thunderstorm", "Snow", "Drizzle"}
HEAT_THRESHOLD_C = 35.0

print("[Alerts] Alert Engine ACTIVE")
print(f"[Alerts] Subscribed to: {RAW_TOPIC}")
print(f"[Alerts] Publishing alerts to: {ALERT_TOPIC}")

for msg in consumer:
    data = msg.value

    city = data.get("city", "Unknown")
    temp_c = float(data.get("temp_c", data.get("temp", 0.0)))
    condition = data.get("condition", "")
    timestamp = data.get("timestamp_iso", "")

    alert = None

    if condition in WET_CONDITIONS:
        alert = {
            "type": "WEATHER_WARNING",
            "city": city,
            "message": f"{condition} detected in {city}",
            "severity": "HIGH",
            "timestamp": timestamp,
        }

    elif temp_c > HEAT_THRESHOLD_C:
        alert = {
            "type": "HEAT_ALERT",
            "city": city,
            "message": f"High temperature {temp_c:.1f}°C in {city}",
            "severity": "HIGH",
            "timestamp": timestamp,
        }

    if alert:
        producer.send(
            ALERT_TOPIC,
            key=city.encode("utf-8"),
            value=alert,
        )
        producer.flush(timeout=5)

        print(f"[ALERT PUBLISHED] {alert['type']} → {alert['city']}")
    else:
        print(f"[OK] {city} normal ({temp_c:.1f}°C, {condition})")

    time.sleep(0.1)