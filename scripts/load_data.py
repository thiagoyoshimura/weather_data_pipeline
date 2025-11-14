import psycopg2
import json
import os

DB_CONFIG = {
    "dbname": "weather_data",
    "user": os.getenv("USER"),  # adjust if needed
    "host": "localhost",
    "port": 5432
}

def load_to_postgres(city, date, data):
    """Insert city and daily weather data into Postgres"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Insert city (ignore if exists)
    cur.execute("""
        INSERT INTO cities (city_name)
        VALUES (%s)
        ON CONFLICT (city_name) DO NOTHING
        RETURNING id;
    """, (city,))
    
    result = cur.fetchone()
    if result:
        city_id = result[0]
    else:
        cur.execute("SELECT id FROM cities WHERE city_name = %s;", (city,))
        city_id = cur.fetchone()[0]

    # Insert daily weather
    cur.execute("""
        INSERT INTO daily_weather (
            city_id, date, temp_c, temp_f, condition, wind_kph, humidity, cloud
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (
        city_id,
        date,
        data.get("avg_temp_c"),
        data.get("avg_temp_c") * 9/5 + 32 if data.get("avg_temp_c") is not None else None,  # convert to Fahrenheit
        data.get("condition"),
        None,  # wind_kph not in your current CSV/JSON; can add later
        data.get("avg_humidity"),
        data.get("total_precip_mm")
    ))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    json_file = os.path.join(os.path.dirname(__file__), "../data/clean/weather_clean.json")

    if not os.path.exists(json_file):
        print(f"No JSON file found at {json_file}. Exiting.")
        exit(1)

    with open(json_file) as f:
        data_list = json.load(f)

    for entry in data_list:
        load_to_postgres(entry["city"], entry["date"], entry)

    print(f"Loaded {len(data_list)} rows into the database.")
