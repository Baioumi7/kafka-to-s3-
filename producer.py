import time
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from confluent_kafka import Producer
import json
from jsonschema import validate, ValidationError
from datetime import datetime, timedelta

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Kafka producer configuration
producer_conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_conf)

# Define topics for each city
topics = {
    "Cairo": "WEATHER_CAIRO",
    "Alexandria": "WEATHER_ALEXANDRIA",
    "New York City": "WEATHER_NYC"
}

# Define the schema for the Kafka messages
weather_schema = {
    "type": "object",
    "properties": {
        "date": {"type": "string", "format": "date-time"},
        "city": {"type": "string"},
        "temperature_2m": {"type": "number"},
        "relative_humidity_2m": {"type": "number"},
        "rain": {"type": "number"},
        "snowfall": {"type": "number"},
        "weather_code": {"type": "number"},
        "surface_pressure": {"type": "number"},
        "cloud_cover": {"type": "number"},
        "cloud_cover_low": {"type": "number"},
        "cloud_cover_high": {"type": "number"},
        "wind_direction_10m": {"type": "number"},
        "wind_direction_100m": {"type": "number"},
        "soil_temperature_28_to_100cm": {"type": "number"}
    },
    "required": ["date", "city", "temperature_2m", "relative_humidity_2m"]
}

# Function to handle Kafka delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Function to validate and publish data to Kafka
def publish_to_kafka(data, topic, producer, schema):
    try:
        # Validate data against the schema
        validate(instance=data, schema=schema)
        
        # Produce the message to Kafka
        producer.produce(topic, value=json.dumps(data), callback=delivery_report)
        producer.flush()
    except ValidationError as ve:
        print(f"Data validation error: {ve.message}")
    except Exception as e:
        print(f"Error producing message to Kafka: {e}")

# Function to fetch and process data for a specific location
def fetch_and_process_data(latitude, longitude, city):
    # Set fixed start and end dates
    start_date = "2022-01-01"
    end_date = "2024-01-01"

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ["temperature_2m", "relative_humidity_2m", "rain", "snowfall", "weather_code", "surface_pressure", "cloud_cover", "cloud_cover_low", "cloud_cover_high", "wind_direction_10m", "wind_direction_100m", "soil_temperature_28_to_100cm"],
        "timezone": "UTC"
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process the response
    response = responses[0]
    print(f"Processing data for {city}: Coordinates {response.Latitude()}°N {response.Longitude()}°E")

    hourly = response.Hourly()
    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ).strftime('%Y-%m-%d %H:%M:%S').tolist(),
        "temperature_2m": hourly.Variables(0).ValuesAsNumpy().tolist(),
        "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy().tolist(),
        "rain": hourly.Variables(2).ValuesAsNumpy().tolist(),
        "snowfall": hourly.Variables(3).ValuesAsNumpy().tolist(),
        "weather_code": hourly.Variables(4).ValuesAsNumpy().tolist(),
        "surface_pressure": hourly.Variables(5).ValuesAsNumpy().tolist(),
        "cloud_cover": hourly.Variables(6).ValuesAsNumpy().tolist(),
        "cloud_cover_low": hourly.Variables(7).ValuesAsNumpy().tolist(),
        "cloud_cover_high": hourly.Variables(8).ValuesAsNumpy().tolist(),
        "wind_direction_10m": hourly.Variables(9).ValuesAsNumpy().tolist(),
        "wind_direction_100m": hourly.Variables(10).ValuesAsNumpy().tolist(),
        "soil_temperature_28_to_100cm": hourly.Variables(11).ValuesAsNumpy().tolist()
    }

    # Convert hourly data to a DataFrame
    hourly_dataframe = pd.DataFrame(data=hourly_data)
    hourly_dataframe['city'] = city
    
    return hourly_dataframe

# Main execution loop to process data for all locations
cities = [
    {"name": "Cairo", "latitude": 30.0626, "longitude": 31.2497},
    {"name": "Alexandria", "latitude": 31.2018, "longitude": 29.9158},
    {"name": "New York City", "latitude": 40.7143, "longitude": -74.006}
]

# Fetch all data for all cities
city_data = {}
for city in cities:
    city_data[city["name"]] = fetch_and_process_data(city["latitude"], city["longitude"], city["name"])
    print(f"Finished fetching data for {city['name']}")

try:
    # Initialize indices for each city
    indices = {city: 0 for city in city_data.keys()}
    
    while True:
        for city, data in city_data.items():
            if indices[city] < len(data):
                row = data.iloc[indices[city]]
                topic = topics[city]
                publish_to_kafka(row.to_dict(), topic, producer, weather_schema)
                indices[city] += 1
            else:
                print(f"Finished streaming all data for {city}")
        
        # Wait for 2 second before the next iteration
        time.sleep(2)

except KeyboardInterrupt:
    print("Streaming interrupted. Shutting down...")

print("Data streaming completed.")