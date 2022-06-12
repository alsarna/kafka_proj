from pykafka import KafkaClient
import threading

KAFKA_HOST = "150.254.78.69:29092"

client = KafkaClient(hosts = KAFKA_HOST)
topic = client.topics["s424343"]

print(client.topics.values())