import json
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP_SERVERS = ["kafka:9092", "localhost:9094"]
TOPIC = "weather_stream"

admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
try:
    admin.create_topics([NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)])
except TopicAlreadyExistsError:
    pass
admin.close()

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
producer.send(TOPIC, {"msg": "Hello Kafka"})
producer.flush()
producer.close()
print("Message envoyé : {'msg': 'Hello Kafka'}")
