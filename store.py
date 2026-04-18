import boto3
import pandas as pd
import os
from datetime import datetime
from config import USE_S3, S3_BUCKET, S3_PREFIX, LOCAL_RAW_PATH, LOCAL_PROCESSED_PATH

s3 = boto3.client("s3") if USE_S3 else None

def upload_to_s3(filepath: str, key: str):
    """Upload a local file to S3."""
    s3.upload_file(filepath, S3_BUCKET, key)
    print(f"  Uploaded to s3://{S3_BUCKET}/{key}")

def store_raw(data: list[dict], local_filepath: str) -> str:
    """Store raw data — S3 if configured, otherwise local."""
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    if USE_S3:
        s3_key = f"{S3_PREFIX}weather_{timestamp}.json"
        upload_to_s3(local_filepath, s3_key)
        return f"s3://{S3_BUCKET}/{s3_key}"
    else:
        print(f"  Stored locally at {local_filepath}")
        return local_filepath

def flatten_and_save_parquet(data: list[dict]) -> str:
    """Flatten nested JSON and save as Parquet for PySpark ingestion."""
    rows = []
    for d in data:
        rows.append({
            "city":        d["name"],
            "country":     d["sys"]["country"],
            "temp_c":      d["main"]["temp"],
            "feels_like":  d["main"]["feels_like"],
            "humidity":    d["main"]["humidity"],
            "pressure":    d["main"]["pressure"],
            "wind_speed":  d["wind"]["speed"],
            "wind_deg":    d["wind"].get("deg", None),
            "weather_main": d["weather"][0]["main"],
            "weather_desc": d["weather"][0]["description"],
            "visibility":  d.get("visibility", None),
            "extracted_at": d["_extracted_at"],
        })

    df = pd.DataFrame(rows)
    os.makedirs(LOCAL_PROCESSED_PATH, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    parquet_path = f"{LOCAL_PROCESSED_PATH}weather_{timestamp}.parquet"
    df.to_parquet(parquet_path, index=False)
    print(f"  Saved Parquet → {parquet_path}")
    return parquet_path