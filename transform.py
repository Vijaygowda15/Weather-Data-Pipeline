import os
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from config import LOCAL_PROCESSED_PATH

def get_spark() -> SparkSession:
    return SparkSession.builder \
        .appName("WeatherPipeline") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

def transform(parquet_path: str):
    spark = get_spark()

    df = spark.read.parquet(parquet_path)

    df_clean = df \
        .withColumn("extracted_at", F.col("extracted_at").cast(TimestampType())) \
        .withColumn("temp_c",       F.round(F.col("temp_c"), 2)) \
        .withColumn("feels_like",   F.round(F.col("feels_like"), 2)) \
        .withColumn("wind_speed",   F.round(F.col("wind_speed"), 2)) \
        .dropna(subset=["city", "temp_c", "humidity"]) \
        .dropDuplicates(["city", "extracted_at"])

    # --- Derived columns ---
    df_clean = df_clean \
        .withColumn("heat_index",
            F.round(
                F.col("temp_c") - 0.55 * (1 - F.col("humidity") / 100) * (F.col("temp_c") - 14.5),
                2
            )
        ) \
        .withColumn("is_raining",
            F.col("weather_main").isin("Rain", "Drizzle", "Thunderstorm")
        ) \
        .withColumn("wind_category",
            F.when(F.col("wind_speed") < 5,  "calm")
             .when(F.col("wind_speed") < 15, "moderate")
             .otherwise("strong")
        ) \
        .withColumn("ingested_date", F.to_date(F.col("extracted_at")))

    # --- City-level aggregation ---
    df_summary = df_clean.groupBy("city", "country", "ingested_date").agg(
        F.round(F.avg("temp_c"),     2).alias("avg_temp_c"),
        F.round(F.max("temp_c"),     2).alias("max_temp_c"),
        F.round(F.min("temp_c"),     2).alias("min_temp_c"),
        F.round(F.avg("humidity"),   2).alias("avg_humidity"),
        F.round(F.avg("wind_speed"), 2).alias("avg_wind_speed"),
        F.sum(F.col("is_raining").cast("int")).alias("rainy_readings"),
        F.count("*").alias("total_readings"),
    )

    df_clean.show(5, truncate=False)
    df_summary.show(truncate=False)

    return df_clean, df_summary