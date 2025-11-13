import requests
import os
import json
from datetime import datetime, timedelta

# ---- CONFIG ----
API_KEY = "a5f3dc8af2ba4507988221534251211"  # <- Replace with your WeatherAPI key
BASE_URL = "https://api.weatherapi.com/v1/history.json"
CITIES = ["montreal", "toronto", "vancouver"]

# How many past days to collect (including today)
DAYS_BACK = 5

# Create a base data folder
DATA_DIR = "../data/raw"

def fetch_weather(city, date):
    """Fetch historical weather for a city and date."""
    params = {
        "key": API_KEY,
        "q": city,
        "dt": date
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Error fetching {city} for {date}: {response.status_code}")
        return None

def save_data(city, date, data):
    """Save JSON data into city/date-based folders."""
    folder_path = os.path.join(DATA_DIR, city)
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, f"{date}.json")

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"✅ Saved: {file_path}")

def main():
    today = datetime.today()

    for i in range(DAYS_BACK):
        target_date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        for city in CITIES:
            print(f"Fetching {city} for {target_date}...")
            data = fetch_weather(city, target_date)
            if data:
                save_data(city, target_date, data)

if __name__ == "__main__":
    main()
