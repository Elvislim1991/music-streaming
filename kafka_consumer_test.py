import os
from kafka import KafkaConsumer

# Get Kafka host from environment variable or use localhost as default
kafka_host = os.environ.get('KAFKA_HOST', 'localhost')
bootstrap_servers = f'{kafka_host}:9092'

consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    group_id='test-group',
    consumer_timeout_ms=5000  # so it exits after no messages
)

print(f"Waiting for messages from Kafka at {bootstrap_servers}...")
for message in consumer:
    print(f"Received: {message.value.decode()}")
