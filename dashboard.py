import json
import os
import time
from datetime import datetime

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

# --- Configuration ---
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_stream")
KAFKA_ALERT_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "weather_alerts")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")

st.set_page_config(page_title="City Weather Monitor", layout="wide")
st.title("Real-Time Smart City Weather Dashboard")

st.sidebar.header("Controls")
auto_refresh = st.sidebar.checkbox("Auto-refresh", value=True)
refresh_seconds = st.sidebar.slider("Refresh interval (seconds)", 2, 15, 5)
max_messages_per_refresh = st.sidebar.slider(
    "Max Kafka messages per refresh", 1, 200, 50
)

if "city_data" not in st.session_state:
    st.session_state.city_data = {}
if "alerts" not in st.session_state:
    st.session_state.alerts = []

# NOTE: Local alert logic (WET_CONDITIONS, HEAT_THRESHOLD) removed.
# We now consume alerts directly from KAFKA_ALERT_TOPIC.


@st.cache_resource
def get_consumer():
    # Subscribe to BOTH topics: Raw Data and Alerts
    return KafkaConsumer(
        KAFKA_TOPIC,
        KAFKA_ALERT_TOPIC,
        bootstrap_servers=[s.strip() for s in BOOTSTRAP_SERVERS if s.strip()],
        group_id="sciwas-dashboard",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=250,  # non-blocking-ish
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )


def ingest_messages():
    consumer = get_consumer()
    count = 0

    for msg in consumer:
        data = msg.value
        topic = msg.topic

        # --- CASE 1: Raw Weather Data ---
        if topic == KAFKA_TOPIC:
            city = data.get("city", "Unknown")
            temp_c = float(data.get("temp_c", data.get("temp", 0.0)))
            condition = data.get("condition", "")
            humidity = data.get("humidity_pct", data.get("humidity", None))

            if city not in st.session_state.city_data:
                st.session_state.city_data[city] = {
                    "temp_c": None,
                    "condition": "",
                    "humidity": None,
                    "history": [],
                    "last_update": None,
                }

            cd = st.session_state.city_data[city]
            cd["temp_c"] = temp_c
            cd["condition"] = condition
            cd["humidity"] = humidity
            cd["last_update"] = datetime.now().strftime("%H:%M:%S")

            cd["history"].append(temp_c)
            if len(cd["history"]) > 30:
                cd["history"].pop(0)

        # --- CASE 2: Alert Data ---
        elif topic == KAFKA_ALERT_TOPIC:
            # Expected format from consumer_alerts.py:
            # {"message": "...", "city": "...", "type": "...", "severity": "..."}
            alert_msg = data.get("message")

            if alert_msg and alert_msg not in st.session_state.alerts:
                st.session_state.alerts.insert(0, alert_msg)

        count += 1
        if count >= max_messages_per_refresh:
            break

    # Keep only the latest 5 alerts
    st.session_state.alerts = st.session_state.alerts[:5]


try:
    ingest_messages()
except Exception as e:
    st.error(f"Kafka error: {e}")

# Alerts section
if st.session_state.alerts:
    st.subheader("Active Alerts (Streamed from Kafka)")
    for a in st.session_state.alerts:
        st.warning(a)

st.divider()

if not st.session_state.city_data:
    st.info(
        "Waiting for data... Ensure 'producer.py' is running for data and 'consumer_alerts.py' is running for alerts."
    )
else:
    cities = sorted(st.session_state.city_data.keys())
    cols = st.columns(min(5, len(cities)))

    for i, city in enumerate(cities):
        col = cols[i % len(cols)]
        cd = st.session_state.city_data[city]

        with col:
            st.subheader(city)
            if cd["temp_c"] is None:
                st.write("No data yet.")
                continue

            st.metric(
                label=cd["condition"],
                value=f"{cd['temp_c']:.1f} °C",
                delta=(
                    f"{cd['humidity']}% humidity"
                    if cd["humidity"] is not None
                    else None
                ),
            )
            if cd["last_update"]:
                st.caption(f"Updated: {cd['last_update']}")

            if len(cd["history"]) > 1:
                st.line_chart(pd.Series(cd["history"]), height=140)
            else:
                st.caption("Building history...")

st.divider()
c1, c2 = st.columns([1, 1])

with c1:
    if st.button("Refresh now", use_container_width=True):
        st.rerun()

with c2:
    if st.button("Clear alerts", use_container_width=True):
        st.session_state.alerts = []
        st.rerun()

if auto_refresh:
    time.sleep(refresh_seconds)
    st.rerun()