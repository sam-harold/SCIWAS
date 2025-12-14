# Smart City Weather Alert System (SCIWAS)

A real-time distributed system that monitors weather conditions across major global cities. It fetches live data from the **OpenWeatherMap API**, streams it through **Apache Kafka**, and processes it simultaneously for anomaly detection (alerts), data archiving, and live visualization.

## System Architecture

This project implements a **Event-Driven Architecture** with the following components:

1.  **Producer (Source):** Fetches live weather data (Temperature, Humidity, Condition) from an External API.
2.  **Message Broker:** A local **Kafka Cluster** (running via Docker) that handles high-throughput data streaming.
3.  **Consumer A (Alert Engine):** Analyzes the stream in real-time to detect anomalies (Storms, Heatwaves) and logs them.
4.  **Consumer B (Dashboard):** A **Streamlit** web application that visualizes live temperature trends and displays an alert history log.
5.  **Consumer C (Archiver):** A microservice that blindly backs up all data to a CSV file for auditing.

-----

## Prerequisites

Before running the project, ensure you have the following installed:

  * **Docker Desktop** (For running the Kafka Server)
  * **Python 3.8+**

-----

## Installation & Setup

### 1\. Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/SCIWAS.git
cd SCIWAS
```

### 2\. Set up Python Environment

It is recommended to use a virtual environment.

```bash
# Mac/Linux
python3 -m venv .venv
source .venv/bin/activate

# Windows
python -m venv .venv
.venv\Scripts\activate
```

### 3\. Install Dependencies

```bash
pip install -r requirements.txt
```

*(Note: If you are using Python 3.12, ensure `kafka-python-ng` is installed instead of the older `kafka-python`)*.

### 4\. Configure Environment Variables

1.  Create a `.env` file in the root directory:
    ```bash
    cp .env.example .env
    ```
2.  Open `.env` and add your **OpenWeatherMap API Key** (Get one for free at [openweathermap.org](https://openweathermap.org/api)):
    ```ini
    API_KEY=your_actual_api_key_here
    ```

-----

## How to Run (The Multi-Terminal Setup)

Since this is a distributed system, you need to run the components in parallel. Open **4 separate terminal windows**:

### Terminal 1: Infrastructure (Kafka)

Start the Docker containers.

```bash
docker-compose up
```

*Wait about 30-60 seconds for the message `[KafkaServer id=1] started`.*

### Terminal 2: The Dashboard (Frontend)

Launch the visual interface.

```bash
streamlit run dashboard.py
```

*This will automatically open your web browser to `http://localhost:8501`.*

### Terminal 3: The Alert Engine (Backend Logic)

Start the processor that filters for storms and heatwaves.

```bash
python consumer_alerts.py
```

### Terminal 4: The Producer (Data Source)

Start fetching data from the API.

```bash
python producer.py
```

-----

## Features

  * **Live Multi-City Monitoring:** Tracks Kuala Lumpur, London, New York, Tokyo, and Singapore simultaneously.
  * **Real-Time Alerts:** Instantly flags critical weather conditions (Rain, Thunderstorm, Snow, \>35°C).
  * **Visual Dashboard:** Interactive graphs showing temperature trends for each city.
  * **Alert History Log:** Displays the last 5 critical alerts with timestamps in the dashboard.
  * **Data Persistence:** Automatically saves all incoming data to `weather_history.csv`.

-----

## Troubleshooting

**Error: `NoBrokersAvailable`**

  * **Cause:** Kafka hasn't finished booting up yet.
  * **Fix:** Wait 60 seconds after running `docker-compose up` before starting the Python scripts.

**Error: `API Error 401`**

  * **Cause:** Invalid API Key.
  * **Fix:** Check your `.env` file. If you just created the key, wait 10-15 minutes for it to activate.