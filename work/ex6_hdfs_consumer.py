import json
from hdfs import InsecureClient
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = ["kafka:9092", "localhost:9094"]
TOPIC = "weather_transformed"

hdfs_client = InsecureClient("http://namenode:9870", user="root")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

for msg in consumer:
    data = msg.value
    hdfs_path = f"/hdfs-data/{data['country']}/{data['city']}/alerts.json"
    line = json.dumps(data) + "\n"
    if hdfs_client.status(hdfs_path, strict=False):
        hdfs_client.write(hdfs_path, data=line, append=True, encoding="utf-8")
    else:
        hdfs_client.write(hdfs_path, data=line, replication=1, encoding="utf-8")
    print(f"Sauvegardé dans {hdfs_path}")
