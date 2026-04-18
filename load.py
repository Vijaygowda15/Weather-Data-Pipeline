import pandas as pd
from sqlalchemy import create_engine, text
from config import PG_URL

engine = create_engine(PG_URL)

DDL_WEATHER_READINGS = """
CREATE TABLE IF NOT EXISTS weather_readings (
    id              SERIAL PRIMARY KEY,
    city            VARCHAR(100),
    country         VARCHAR(10),
    temp_c          NUMERIC(6,2),
    feels_like      NUMERIC(6,2),
    humidity        INTEGER,
    pressure        INTEGER,
    wind_speed      NUMERIC(6,2),
    wind_deg        INTEGER,
    weather_main    VARCHAR(50),
    weather_desc    VARCHAR(100),
    visibility      INTEGER,
    heat_index      NUMERIC(6,2),
    is_raining      BOOLEAN,
    wind_category   VARCHAR(20),
    ingested_date   DATE,
    extracted_at    TIMESTAMP,
    UNIQUE(city, extracted_at)
);
"""

DDL_WEATHER_SUMMARY = """
CREATE TABLE IF NOT EXISTS weather_summary (
    id              SERIAL PRIMARY KEY,
    city            VARCHAR(100),
    country         VARCHAR(10),
    ingested_date   DATE,
    avg_temp_c      NUMERIC(6,2),
    max_temp_c      NUMERIC(6,2),
    min_temp_c      NUMERIC(6,2),
    avg_humidity    NUMERIC(6,2),
    avg_wind_speed  NUMERIC(6,2),
    rainy_readings  INTEGER,
    total_readings  INTEGER,
    UNIQUE(city, ingested_date)
);
"""

def create_tables():
    with engine.connect() as conn:
        conn.execute(text(DDL_WEATHER_READINGS))
        conn.execute(text(DDL_WEATHER_SUMMARY))
        conn.commit()
    print("  Tables ready.")

from sqlalchemy import text

def load_to_postgres(df, table):
    pdf = df.toPandas()

    if table == "weather_summary":
        # ✅ Use transaction (IMPORTANT)
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM weather_summary"))

    pdf.to_sql(table, engine, if_exists="append", index=False)