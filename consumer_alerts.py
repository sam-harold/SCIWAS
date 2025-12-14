import json
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_stream")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[s.strip() for s in BOOTSTRAP_SERVERS if s.strip()],
    group_id="sciwas-alert-engine",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

WET_CONDITIONS = {"Rain", "Thunderstorm", "Snow", "Drizzle"}
HEAT_THRESHOLD_C = 35.0

print("[Alerts] ALERT SYSTEM ACTIVE. Filtering for dangerous weather...")

for msg in consumer:
    data = msg.value
    city = data.get("city", "Unknown")
    condition = data.get("condition", "")
    temp_c = float(data.get("temp_c", data.get("temp", 0.0)))

    is_critical = False
    alert_msg = ""

    if condition in WET_CONDITIONS:
        is_critical = True
        alert_msg = f"WET WEATHER: {condition} in {city}"
    elif temp_c > HEAT_THRESHOLD_C:
        is_critical = True
        alert_msg = f"HEATWAVE: {temp_c:.1f}°C in {city}"

    if is_critical:
        print(f"\n[!!! CRITICAL ALERT !!!] {alert_msg}")
    else:
        print(f"[OK] {city} is normal ({temp_c:.1f}°C, {condition})")