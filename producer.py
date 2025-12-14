import os
import time
import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- CONFIGURATION ---
API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITIES = ["Kuala Lumpur", "London", "New York", "Tokyo", "Singapore"]
KAFKA_TOPIC = "weather_stream"
BOOTSTRAP_SERVERS = ["localhost:9092"]

# Validate API key is loaded
if not API_KEY:
    raise ValueError("OPENWEATHER_API_KEY not found in environment variables")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)


def fetch_weather():
    print(f"Producer Started. Monitoring: {CITIES}")
    while True:
        for city in CITIES:
            try:
                # INTEGRATION: Fetching from External API
                url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
                response = requests.get(url)
                if response.status_code == 200:
                    api_data = response.json()
                    # IMPROVEMENT: Data Cleaning/Transformation
                    cleaned_data = {
                        "city": api_data["name"],
                        "temp": api_data["main"]["temp"],
                        "condition": api_data["weather"][0]["main"],
                        "humidity": api_data["main"]["humidity"],
                        "timestamp": time.time(),
                    }
                    producer.send(KAFKA_TOPIC, value=cleaned_data)
                    print(
                        f" -> Sent: {cleaned_data['city']} ({cleaned_data['temp']}°C)"
                    )
                else:
                    print(f"API Error {response.status_code} for {city}")
            except Exception as e:
                print(f"Connection Error: {e}")
        print("Waiting 5 seconds...")
        time.sleep(5)


if __name__ == "__main__":
    fetch_weather()
