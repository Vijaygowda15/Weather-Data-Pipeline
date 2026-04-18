import requests
import json
import os
from datetime import datetime
from config import WEATHER_API_KEY, CITIES, LOCAL_RAW_PATH

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

def fetch_weather(city: str) -> dict:
    """Fetch current weather for a city from OpenWeatherMap API."""
    params = {
        "q": city,
        "appid": WEATHER_API_KEY,
        "units": "metric"
    }
    response = requests.get(BASE_URL, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

def extract_all_cities() -> list[dict]:
    """Extract weather data for all configured cities."""
    results = []
    for city in CITIES:
        try:
            data = fetch_weather(city)
            data["_extracted_at"] = datetime.utcnow().isoformat()
            results.append(data)
            print(f"  Extracted: {city}")
        except requests.RequestException as e:
            print(f"  Failed: {city} — {e}")
    return results

def save_raw_json(data: list[dict]) -> str:
    """Save raw JSON locally as a timestamped file."""
    os.makedirs(LOCAL_RAW_PATH, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filepath = f"{LOCAL_RAW_PATH}weather_{timestamp}.json"
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  Saved raw JSON → {filepath}")
    return filepath