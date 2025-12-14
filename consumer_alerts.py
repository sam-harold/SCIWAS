import json
from kafka import KafkaConsumer

KAFKA_TOPIC = "weather_stream"
BOOTSTRAP_SERVERS = ["localhost:9092"]

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print("ALERT SYSTEM ACTIVE. Filtering for dangerous weather...")

for message in consumer:
    data = message.value

    # IMPROVEMENT: Filtering Logic
    is_critical = False
    alert_msg = ""

    if data["condition"] in ["Rain", "Thunderstorm", "Snow", "Drizzle"]:
        is_critical = True
        alert_msg = f"WET WEATHER: {data['condition']} in {data['city']}"

    elif data["temp"] > 35.0:
        is_critical = True
        alert_msg = f"HEATWAVE: {data['temp']}°C in {data['city']}"

    if is_critical:
        print(f"\n[!!! CRITICAL ALERT !!!] {alert_msg}")
    else:
        print(f"[OK] {data['city']} is normal.")
