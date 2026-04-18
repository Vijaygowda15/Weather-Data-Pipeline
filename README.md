# Weather Data Pipeline

A real-time weather data pipeline that automatically collects weather data for five Indian cities every hour, processes it using PySpark, stores it in PostgreSQL, and visualises it in Power BI. The pipeline runs on a schedule using Apache Airflow, with all services managed through Docker.

---

## Table of Contents

- [Problem Statement](#problem-statement)
- [Project Architecture](#project-architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [How to Run](#how-to-run)
- [Pipeline Stages](#pipeline-stages)
- [Database Tables](#database-tables)
- [Airflow DAG](#airflow-dag)
- [Power BI Setup](#power-bi-setup)
- [Future Work](#future-work)

---

## Problem Statement

Weather data is one of the most widely used datasets in business decisions — from logistics and agriculture to insurance and retail. However, raw weather API responses are nested, inconsistent, and not ready for analysis straight out of the box.

The goal of this project is to build a fully automated, end-to-end data pipeline that:

- Pulls live weather data from the OpenWeatherMap API every hour
- Cleans and enriches the raw data using PySpark
- Loads the results into a structured PostgreSQL database
- Makes the data available for real-time dashboards in Power BI
- Runs on a schedule without any manual intervention, using Apache Airflow

The entire system runs inside Docker containers, making it easy to set up on any machine.

---

## Project Architecture

```
OpenWeatherMap API
        ↓
   extract.py     → Fetch weather for 5 cities (Bangalore, Mumbai, Delhi, Chennai, Hyderabad)
        ↓
   store.py       → Save raw JSON + convert to Parquet (optional S3 upload)
        ↓
   transform.py   → PySpark: clean, enrich, derive new columns, aggregate
        ↓
   load.py        → Write to PostgreSQL (weather_readings + weather_summary)
        ↓
   Power BI       → Connect to PostgreSQL for live dashboards
        ↓
   Airflow        → Schedule and monitor the whole pipeline (every hour)
```

All services — Airflow webserver, Airflow scheduler, and PostgreSQL — run inside Docker containers managed by Docker Compose.

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.11 | Core language |
| Apache Airflow 2.8 | Pipeline scheduling and monitoring |
| Apache PySpark 3.5 | Data transformation |
| PostgreSQL 15 | Data storage |
| Docker + Docker Compose | Container management |
| SQLAlchemy | Python-to-PostgreSQL connection |
| Pandas + PyArrow | Data processing and Parquet format |
| OpenWeatherMap API | Live weather data source |
| Power BI | Dashboard and visualisation |
| AWS S3 (optional) | Cloud storage for raw files |

---

## Project Structure

```
weather-data-pipeline/
│
├── docker-compose.yml       All Docker services defined here
├── init_db.sql              Creates weather_db on first startup
│
├── pipeline.py              Run manually (without Airflow)
├── extract.py               Stage 1 — API data collection
├── store.py                 Stage 2 — Save raw + Parquet
├── transform.py             Stage 3 — PySpark cleaning
├── load.py                  Stage 4 — PostgreSQL loader
├── config.py                Central configuration
│
├── dags/
│   └── weather_pipeline_dag.py   Airflow DAG (hourly schedule)
│
├── data/
│   ├── raw/                 Raw JSON files (timestamped)
│   └── processed/           Parquet files (timestamped)
│
├── logs/                    Airflow task logs
├── plugins/                 Airflow plugins folder
│
├── .env.example             Template for environment variables
├── .gitignore               Excludes secrets and data files
├── requirements.txt         Python dependencies
└── README.md
```

---

## How to Run

### Prerequisites

- Docker Desktop installed and running
- A free API key from openweathermap.org

### Step 1 — Clone the repository

```
git clone https://github.com/Vijaygowda15/weather-data-pipeline
cd weather-data-pipeline
```

### Step 2 — Set up environment variables

Copy the example file and fill in your values:

```
cp .env.example .env
```

Open `.env` and add your API key and PostgreSQL password. Do not commit this file.

### Step 3 — Start all services with Docker

```
docker compose up -d
```

This command starts three containers:
- PostgreSQL (port 5432)
- Airflow webserver (port 8080)
- Airflow scheduler

Wait about 60 seconds for everything to initialise.

### Step 4 — Open the Airflow UI

Go to http://localhost:8080 in your browser.

Login with username `admin` and password `admin`.

You will see the `weather_pipeline` DAG listed. It runs automatically every hour. To trigger it manually, click the play button on the right side of the DAG row.

### Step 5 — Run manually without Airflow (optional)

If you want to run the pipeline directly without Docker:

```
pip install -r requirements.txt
python pipeline.py
```

Make sure Java 17 is installed and JAVA_HOME is set before running this.

---

## Pipeline Stages

### Stage 1 — Extract

Connects to the OpenWeatherMap API and fetches current weather for Bangalore, Mumbai, Delhi, Chennai, and Hyderabad. Each city's response is tagged with the extraction timestamp. If one city fails, the pipeline logs the error and continues with the rest.

### Stage 2 — Store

Saves the raw API response as a timestamped JSON file on disk. This acts as an immutable archive — if anything goes wrong downstream, you can always re-process from this file. The data is also flattened from its nested JSON structure and saved as a Parquet file, which is the format PySpark reads most efficiently.

### Stage 3 — Transform

PySpark reads the Parquet file and applies the following steps:

- Casts the extracted_at field to a proper timestamp type
- Rounds temperature, feels-like, and wind speed to 2 decimal places
- Drops rows with missing city, temperature, or humidity values
- Drops duplicate rows based on city and extraction time
- Calculates a heat index from temperature and humidity
- Adds an is_raining flag based on weather condition
- Categorises wind speed as calm, moderate, or strong
- Creates a daily aggregation table with average, min, and max values per city

Data quality checks run after transformation. If the result is empty, has null temperatures, or has humidity values outside the 0 to 100 range, the pipeline stops and logs an error.

### Stage 4 — Load

Converts the two Spark DataFrames to Pandas and writes them to PostgreSQL.

For the readings table, new rows are appended. The unique constraint on city and extracted_at prevents duplicates if the pipeline re-runs.

For the summary table, today's rows are deleted before re-inserting. This makes the pipeline safe to run multiple times per day while preserving all historical data.

---

## Database Tables

### weather_readings

Stores one row for each city for each pipeline run.

| Column | Type | Description |
|---|---|---|
| city | VARCHAR | City name |
| country | VARCHAR | Country code |
| temp_c | NUMERIC | Temperature in Celsius |
| feels_like | NUMERIC | Feels-like temperature |
| humidity | INTEGER | Humidity percentage |
| pressure | INTEGER | Atmospheric pressure |
| wind_speed | NUMERIC | Wind speed in m/s |
| wind_category | VARCHAR | calm / moderate / strong |
| weather_main | VARCHAR | Clear, Rain, Clouds, etc. |
| is_raining | BOOLEAN | True if rain or thunderstorm |
| heat_index | NUMERIC | Derived comfort index |
| ingested_date | DATE | Date of the reading |
| extracted_at | TIMESTAMP | Exact API call time |

### weather_summary

Stores one row per city per day, aggregated from all readings that day.

| Column | Type | Description |
|---|---|---|
| city | VARCHAR | City name |
| ingested_date | DATE | Date |
| avg_temp_c | NUMERIC | Average temperature |
| max_temp_c | NUMERIC | Highest temperature of the day |
| min_temp_c | NUMERIC | Lowest temperature of the day |
| avg_humidity | NUMERIC | Average humidity |
| avg_wind_speed | NUMERIC | Average wind speed |
| rainy_readings | INTEGER | Number of hourly readings showing rain |
| total_readings | INTEGER | Total readings recorded that day |

---

## Airflow DAG

The DAG is named `weather_pipeline` and is located at `dags/weather_pipeline_dag.py`.

It runs every hour (cron schedule: `0 * * * *`) and contains four tasks in sequence:

```
start → extract → store → transform → load → end
```

Each task is independent. If the extract task fails, the store task does not run. Airflow retries failed tasks up to 2 times with a 3-minute gap between attempts.

You can monitor each run in the Airflow UI at http://localhost:8080. Click on the DAG name to see the graph view, task logs, and run history.

To stop all Docker services:

```
docker compose down
```

To stop and also delete the stored data:

```
docker compose down -v
```

## Future Work

**Add more cities** — extend the CITIES list in config.py to cover more locations across India or globally.

**Enable S3 storage** — set USE_S3=true in the .env file. Raw files will automatically mirror to your S3 bucket alongside local storage.

**Add email alerts** — configure Airflow's SMTP settings so you receive an email when a pipeline run fails.

**Add data quality reporting** — use Great Expectations to generate an HTML data quality report after each run, saved to the reports folder.

**Deploy to cloud** — move Docker Compose to an AWS EC2 instance or Azure VM so the pipeline runs 24/7 without keeping your laptop on.

**Use Redshift instead of PostgreSQL** — swap the PG_URL in the .env file for a Redshift connection string. SQLAlchemy handles both databases identically.

**Add a second API** — ingest air quality data from the OpenAQ API alongside weather data, join both in the transform stage, and create a combined dashboard.

---

## About

**Vijay N** — M.Sc. Data Science and Analytics, Jain University Bangalore

LinkedIn — linkedin.com/in/vijaygowda15

GitHub — github.com/Vijaygowda15

Email — vijaynvijay2002@gmail.com

---


*This project was built as part of a data engineering portfolio.*
*No real API keys or passwords are stored in this repository.*
