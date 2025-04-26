import os
import socket
from kafka import KafkaConsumer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable

# Get Kafka host from environment variable or use the hostname if not set
# On Raspberry Pi, 'localhost' might not work correctly with Docker
kafka_host = os.environ.get('KAFKA_HOST')
if not kafka_host:
    # Try to get the hostname or IP address that might work better with Docker
    kafka_host = socket.gethostname()
    print(f"KAFKA_HOST not set, using hostname: {kafka_host}")

    # Fallback to localhost if needed
    if kafka_host == 'localhost' or not kafka_host:
        kafka_host = 'localhost'
        print("Using localhost as fallback")

bootstrap_servers = f'{kafka_host}:9092'
print(f"Attempting to connect to Kafka at {bootstrap_servers}")

# Function to check if the Kafka port is accessible
def check_kafka_port(host, port=9092, timeout=10):
    try:
        # Create a socket object
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)

        # Attempt to connect
        result = s.connect_ex((host, port))
        s.close()

        # If result is 0, the port is open
        return result == 0
    except socket.error:
        return False

# Check if Kafka port is accessible
port_accessible = check_kafka_port(kafka_host)
if not port_accessible:
    print(f"Warning: Kafka port 9092 is not accessible on {kafka_host}")
    print("This may indicate that:")
    print("1. Kafka broker is not running")
    print("2. There's a firewall blocking the connection")
    print("3. The IP address is incorrect")
    print("\nTroubleshooting steps:")
    print("- Verify Docker containers are running: docker-compose -f kafka-docker-compose.yml ps")
    print("- Check if the port is open: telnet", kafka_host, "9092")
    print("- Verify the IP address is correct: hostname -I")
    print("- Make sure KAFKA_HOST is set correctly when starting Docker: KAFKA_HOST=<your-ip> docker-compose -f kafka-docker-compose.yml up -d")
    print("\nAttempting to connect anyway...\n")

try:
    print(f"Creating consumer with bootstrap servers: {bootstrap_servers}")
    # Add connection timeout to avoid long waits
    consumer = KafkaConsumer(
        'test-topic',
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        group_id='test-group',
        consumer_timeout_ms=10000,  # so it exits after no messages
        api_version=(2, 5, 0),  # Specify API version to ensure compatibility
        request_timeout_ms=15000,  # 15 seconds timeout for requests
        connections_max_idle_ms=30000,  # 30 seconds idle time
        retry_backoff_ms=500,  # 0.5 seconds between retries
        session_timeout_ms=10000  # 10 seconds session timeout
    )

    # Test the connection by checking topics
    print("Testing Kafka connection...")
    try:
        topics = consumer.topics()
        print(f"Successfully connected to Kafka. Available topics: {topics}")
        if 'test-topic' not in topics:
            print("Warning: 'test-topic' does not exist. You may need to create it first.")
            print("You can create it by running the producer script first.")
    except Exception as e:
        print(f"Warning: Could not list topics: {e}")
        print("Attempting to consume messages anyway...")

    print(f"Waiting for messages from Kafka at {bootstrap_servers}...")
    print("(Press Ctrl+C to exit)")

    message_count = 0
    for message in consumer:
        message_count += 1
        print(f"Received: {message.value.decode()}")
        print(f"Partition: {message.partition}, Offset: {message.offset}")
        print(f"Timestamp: {message.timestamp}")
        print("-" * 50)

    if message_count == 0:
        print("No messages received. This could be because:")
        print("1. No messages have been sent to the 'test-topic' topic")
        print("2. All messages have been consumed already")
        print("3. The consumer group offset has been committed")
        print("\nTry running the producer script to send messages:")
        print("python kafka_producer_test.py")

except KafkaTimeoutError as e:
    print(f"Timeout connecting to Kafka: {e}")
    print("\nThis usually indicates one of the following issues:")
    print("1. The Kafka broker is not running")
    print("2. The Kafka broker is not accessible at the specified IP address")
    print("3. The Kafka broker is running but not properly configured")

    print("\nTroubleshooting steps:")
    print("1. Make sure your Docker containers are running:")
    print("   docker-compose -f kafka-docker-compose.yml ps")
    print("2. Ensure the KAFKA_HOST environment variable is set correctly when starting Docker:")
    print(f"   KAFKA_HOST={kafka_host} docker-compose -f kafka-docker-compose.yml up -d")
    print("3. Check if the Kafka port is accessible:")
    print(f"   telnet {kafka_host} 9092")
    print("4. Verify your network configuration:")
    print("   docker network inspect streaming-network")
    raise

except NoBrokersAvailable as e:
    print(f"No Kafka brokers available: {e}")
    print("\nThis usually means the Kafka broker is not running or not accessible.")
    print("Make sure your Docker containers are running:")
    print("docker-compose -f kafka-docker-compose.yml ps")
    print("docker-compose -f kafka-docker-compose.yml logs broker")
    raise

except KafkaError as e:
    print(f"Error connecting to Kafka: {e}")
    print("\nTry setting the KAFKA_HOST environment variable to the IP address of your Kafka broker")
    print("For example: export KAFKA_HOST=192.168.1.100")

    print("\nAdditional troubleshooting:")
    print("1. Check if Docker is running the Kafka container:")
    print("   docker ps | grep broker")
    print("2. Check Kafka broker logs:")
    print("   docker-compose -f kafka-docker-compose.yml logs broker")
    print("3. Restart the Kafka broker:")
    print("   docker-compose -f kafka-docker-compose.yml restart broker")
    raise
