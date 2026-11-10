import json
from kafka import KafkaConsumer

TOPIC = "weather_stream"
BOOTSTRAP_SERVERS = ["kafka:9092", "localhost:9094"]

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

for message in consumer:
    print(message.value)
