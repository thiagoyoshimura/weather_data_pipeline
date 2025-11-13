import subprocess
import datetime
import os

# Get the parent directory of the current script (scripts/)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

print("Starting daily weather data pipeline...")

# Step 1: Ingest new data
print("\n Running ingestion script...")
subprocess.run(["python3", os.path.join(BASE_DIR, "scripts", "ingest_weather_api.py")], check=True)

# Step 2: Transform data
print("\n Running transformation script...")
subprocess.run(["python3", os.path.join(BASE_DIR, "scripts", "transform_data.py")], check=True)

print(f"\n Pipeline completed successfully on {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
