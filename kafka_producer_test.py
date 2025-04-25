import os
from kafka import KafkaProducer

# Get Kafka host from environment variable or use localhost as default
kafka_host = os.environ.get('KAFKA_HOST', 'localhost')
bootstrap_servers = f'{kafka_host}:9092'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

topic = 'test-topic'
message = b'Hello, Kafka!'

producer.send(topic, message)
producer.flush()

print(f"Sent: {message.decode()} to topic '{topic}'")
print(f"Connected to Kafka at {bootstrap_servers}")
