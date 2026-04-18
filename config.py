import os
from dotenv import load_dotenv

load_dotenv()

# API
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
CITIES = ["Bangalore", "Mumbai", "Delhi", "Chennai", "Hyderabad"]

# Storage
USE_S3 = os.getenv("USE_S3", "false").lower() == "true"
S3_BUCKET = os.getenv("S3_BUCKET", "my-weather-bucket")
S3_PREFIX = "raw/weather/"
LOCAL_RAW_PATH = "data/raw/"
LOCAL_PROCESSED_PATH = "data/processed/"

# PostgreSQL
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "weather_db")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
PG_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"