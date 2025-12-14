import json
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_stream")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "5"))

CITIES = ["Kuala Lumpur", "London", "New York", "Tokyo", "Singapore"]

if not API_KEY:
    raise ValueError(
        "OPENWEATHER_API_KEY not found. Put it in .env (copy from .env.example)."
    )

producer = KafkaProducer(
    bootstrap_servers=[s.strip() for s in BOOTSTRAP_SERVERS if s.strip()],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

session = requests.Session()

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def fetch_city(city: str) -> dict | None:
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": city, "appid": API_KEY, "units": "metric"}

    backoff = 1.0
    for attempt in range(1, 4):
        try:
            resp = session.get(url, params=params, timeout=(3, 10))
            if resp.status_code == 200:
                api = resp.json()
                return {
                    "city": api.get("name", city),
                    "temp_c": float(api["main"]["temp"]),
                    "humidity_pct": int(api["main"]["humidity"]),
                    "condition": str(api["weather"][0]["main"]),
                    "timestamp_unix": time.time(),
                    "timestamp_iso": now_iso(),
                }
            else:
                print(
                    f"[Producer] API {resp.status_code} for {city}: {resp.text[:120]}"
                )
        except Exception as e:
            print(f"[Producer] Attempt {attempt} failed for {city}: {e}")

        time.sleep(backoff)
        backoff *= 2

    return None

def main() -> None:
    print(f"[Producer] Started. Topic={KAFKA_TOPIC} Brokers={BOOTSTRAP_SERVERS}")
    print(f"[Producer] Monitoring cities: {CITIES}")

    while True:
        for city in CITIES:
            data = fetch_city(city)
            if not data:
                continue

            producer.send(KAFKA_TOPIC, key=data["city"].encode("utf-8"), value=data)
            producer.flush(timeout=5)

            print(
                f"[Producer] Sent: {data['city']} {data['temp_c']}°C {data['condition']}"
            )

        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()