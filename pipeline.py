import time
from extract import extract_all_cities, save_raw_json
from store import store_raw, flatten_and_save_parquet
from transform import transform
from load import create_tables, load_to_postgres

def run_pipeline():
    print("\n=== WEATHER PIPELINE STARTED ===")
    t0 = time.time()

    # Stage 1: Extract
    print("\n[1/4] Extracting from API...")
    raw_data = extract_all_cities()
    raw_filepath = save_raw_json(raw_data)

    # Stage 2: Store
    print("\n[2/4] Storing raw data...")
    store_raw(raw_data, raw_filepath)
    parquet_path = flatten_and_save_parquet(raw_data)

    # Stage 3: Transform
    print("\n[3/4] Transforming with PySpark...")
    df_clean, df_summary = transform(parquet_path)

    # Stage 4: Load
    print("\n[4/4] Loading into PostgreSQL...")
    create_tables()
    load_to_postgres(df_clean,   "weather_readings")
    load_to_postgres(df_summary, "weather_summary")

    elapsed = round(time.time() - t0, 2)
    print(f"\n=== PIPELINE COMPLETE in {elapsed}s ===")

if __name__ == "__main__":
    run_pipeline()