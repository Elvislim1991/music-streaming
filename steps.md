
# Deploying to Raspberry Pi

## Prerequisites
- Raspberry Pi with Docker and Docker Compose installed
- The Raspberry Pi should be connected to your local WiFi network

## Setup

1. Clone this repository to your Raspberry Pi

2. Find your Raspberry Pi's IP address:
```
hostname -I
```

3. Set the KAFKA_HOST environment variable to your Raspberry Pi's IP address:
```
export KAFKA_HOST=<your-raspberry-pi-ip>
```

## Starting the Services

1. Start Kafka first:
```
docker-compose -f kafka-docker-compose.yml up -d
```

2. Start Spark:
```
docker-compose -f spark-docker-compose.yml up -d
```

3. Check that all services are running:
```
docker ps
```

## Testing the Setup

### Test Kafka

1. Run the producer test script:
```
python kafka_producer_test.py
```

2. Run the consumer test script:
```
python kafka_consumer_test.py
```

### Test Spark Structured Streaming

1. Copy the Spark streaming script to the Spark master container:
```
docker cp ./spark_kafka_stream.py spark-master:/opt/bitnami/spark/spark_kafka_stream.py 
```

2. Execute the Spark streaming job:
```
docker exec -it spark-master bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 spark_kafka_stream.py
```

3. In another terminal, produce messages to Kafka:
```
docker exec -it broker bash
kafka-console-producer --broker-list broker:9092 --topic test-topic
```

## Accessing from Other Devices

To access Kafka from other devices on your network:

1. Set the KAFKA_HOST environment variable on the client device:
```
export KAFKA_HOST=<your-raspberry-pi-ip>
```

2. Run the Kafka producer or consumer scripts on the client device.
```
