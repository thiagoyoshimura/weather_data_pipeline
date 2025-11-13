import os
import json
import pandas as pd

RAW_DIR = "../data/raw"
CLEAN_DIR = "../data/clean"

def extract_weather_data(city, file_path):
    """Extract useful metrics from one weather JSON file."""
    with open(file_path, "r") as f:
        data = json.load(f)

    # Sometimes WeatherAPI may skip a field — be safe with .get()
    day_info = data.get("forecast", {}).get("forecastday", [])[0].get("day", {})

    return {
        "city": city,
        "date": data.get("forecast", {}).get("forecastday", [])[0].get("date"),
        "avg_temp_c": day_info.get("avgtemp_c"),
        "max_temp_c": day_info.get("maxtemp_c"),
        "min_temp_c": day_info.get("mintemp_c"),
        "avg_humidity": day_info.get("avghumidity"),
        "total_precip_mm": day_info.get("totalprecip_mm"),
        "condition": day_info.get("condition", {}).get("text"),
    }

def main():
    rows = []
    for city in os.listdir(RAW_DIR):
        city_path = os.path.join(RAW_DIR, city)
        if os.path.isdir(city_path):
            for filename in os.listdir(city_path):
                if filename.endswith(".json"):
                    file_path = os.path.join(city_path, filename)
                    try:
                        record = extract_weather_data(city, file_path)
                        rows.append(record)
                    except Exception as e:
                        print(f"⚠️ Error processing {file_path}: {e}")

    df = pd.DataFrame(rows)
    os.makedirs(CLEAN_DIR, exist_ok=True)
    output_path = os.path.join(CLEAN_DIR, "weather_clean.csv")
    df.to_csv(output_path, index=False)
    print(f"✅ Saved clean dataset to {output_path}")
    print(f"Total rows: {len(df)}")

if __name__ == "__main__":
    main()