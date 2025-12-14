import streamlit as st
import json
from kafka import KafkaConsumer
import time
from datetime import datetime

# Page Config
st.set_page_config(page_title="City Weather Monitor", layout="wide")
st.title("Real-Time Smart City Weather Dashboard")

# --- SESSION STATE INITIALIZATION ---
if "city_data" not in st.session_state:
    st.session_state.city_data = {
        "Kuala Lumpur": {
            "temp": None,
            "condition": "",
            "humidity": None,
            "history": [],
            "last_update": None,
        },
        "London": {
            "temp": None,
            "condition": "",
            "humidity": None,
            "history": [],
            "last_update": None,
        },
        "New York": {
            "temp": None,
            "condition": "",
            "humidity": None,
            "history": [],
            "last_update": None,
        },
        "Tokyo": {
            "temp": None,
            "condition": "",
            "humidity": None,
            "history": [],
            "last_update": None,
        },
        "Singapore": {
            "temp": None,
            "condition": "",
            "humidity": None,
            "history": [],
            "last_update": None,
        },
    }

if "alerts" not in st.session_state:
    st.session_state.alerts = []

if "last_fetch_time" not in st.session_state:
    st.session_state.last_fetch_time = 0


# --- KAFKA CONSUMER (CREATE FRESH EACH TIME) ---
@st.cache_resource
def get_consumer():
    return KafkaConsumer(
        "weather_stream",
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=100,  # Very short timeout for non-blocking
        enable_auto_commit=True,
    )


consumer = get_consumer()

# --- FETCH NEW DATA (ONLY EVERY FEW SECONDS) ---
current_time = time.time()
if current_time - st.session_state.last_fetch_time > 2:  # Fetch every 1 second
    st.session_state.last_fetch_time = current_time

    try:
        message_count = 0
        for message in consumer:
            data = message.value
            city_name = data["city"]

            if city_name in st.session_state.city_data:
                # Update city data
                st.session_state.city_data[city_name]["temp"] = data["temp"]
                st.session_state.city_data[city_name]["condition"] = data["condition"]
                st.session_state.city_data[city_name]["humidity"] = data["humidity"]
                st.session_state.city_data[city_name]["last_update"] = (
                    datetime.now().strftime("%H:%M:%S")
                )

                # Update history (keep last 15)
                st.session_state.city_data[city_name]["history"].append(data["temp"])
                if len(st.session_state.city_data[city_name]["history"]) > 15:
                    st.session_state.city_data[city_name]["history"].pop(0)

                # Check for alerts
                if data["condition"] in ["Rain", "Thunderstorm", "Snow", "Drizzle"]:
                    alert_msg = f"WET WEATHER: {data['condition']} in {city_name}"
                    if alert_msg not in st.session_state.alerts:
                        st.session_state.alerts.insert(0, alert_msg)
                elif data["temp"] > 35.0:
                    alert_msg = f"HEATWAVE: {data['temp']}°C in {city_name}"
                    if alert_msg not in st.session_state.alerts:
                        st.session_state.alerts.insert(0, alert_msg)

                message_count += 1

                # Limit messages processed per cycle
                if message_count >= 10:
                    break

    except Exception as e:
        st.sidebar.error(f"Error: {e}")

# Keep only last 5 alerts
st.session_state.alerts = st.session_state.alerts[:5]

# --- ALERT SECTION ---
if st.session_state.alerts:
    st.subheader("Active Alerts")
    for alert in st.session_state.alerts:
        st.warning(alert)
    st.markdown("---")

# --- CITY DISPLAY ---
cols = st.columns(5)

for idx, (city_name, col) in enumerate(zip(st.session_state.city_data.keys(), cols)):
    city_data = st.session_state.city_data[city_name]

    with col:
        st.subheader(city_name)

        if city_data["temp"] is not None:
            st.metric(
                label=city_data["condition"],
                value=f"{city_data['temp']:.1f} °C",
                delta=f"{city_data['humidity']}% humidity",
            )

            if city_data["last_update"]:
                st.caption(f"Updated: {city_data['last_update']}")

            # Show graph if history exists
            if len(city_data["history"]) > 1:
                st.line_chart(city_data["history"], height=120)
            else:
                st.info("Building temperature history...")

            # Color-code based on temperature
            if city_data["temp"] > 35:
                st.markdown("**EXTREME HEAT**")
            elif city_data["temp"] > 30:
                st.markdown("**Hot**")
            elif city_data["temp"] < 10:
                st.markdown("**Cold**")
            else:
                st.markdown("**Normal**")
        else:
            st.info("Waiting for data...")

# --- FOOTER & AUTO-REFRESH ---
st.markdown("---")
col1, col2, col3 = st.columns([2, 1, 1])

with col1:
    st.caption("Dashboard auto-refreshes every 5 seconds")

with col2:
    if st.button("Refresh Now", use_container_width=True):
        st.rerun()

with col3:
    if st.button("Clear Alerts", use_container_width=True):
        st.session_state.alerts = []
        st.rerun()

# Auto-refresh
time.sleep(2)
st.rerun()
