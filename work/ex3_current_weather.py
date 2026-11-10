import json
import time
import requests
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = ["kafka:9092", "localhost:9094"]
TOPIC = "weather_stream"

CITIES = [
    {"city": "Paris", "country": "France", "latitude": 48.8566, "longitude": 2.3522},
    {"city": "Seville", "country": "Spain", "latitude": 37.3891, "longitude": -5.9845},
    {"city": "Dubai", "country": "United Arab Emirates", "latitude": 25.2048, "longitude": 55.2708},
    {"city": "Reykjavik", "country": "Iceland", "latitude": 64.1466, "longitude": -21.9426},
    {"city": "Wellington", "country": "New Zealand", "latitude": -41.2865, "longitude": 174.7762},
]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

while True:
    for c in CITIES:
        url = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={c['latitude']}&longitude={c['longitude']}"
            f"&current=temperature_2m,wind_speed_10m,weather_code&wind_speed_unit=ms"
        )
        data = requests.get(url, timeout=10).json()
        data["city"] = c["city"]
        data["country"] = c["country"]
        producer.send(TOPIC, data)
        print(f"Envoyé : {c['city']} ({c['country']})")
    producer.flush()
    time.sleep(30)

