import os
import socket
import time
import argparse
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable, KafkaError

# Parse command line arguments
parser = argparse.ArgumentParser(description='Create Kafka topics for music streaming analytics')
parser.add_argument('--internal', action='store_true', help='Use internal Kafka bootstrap server (broker:29092)')
parser.add_argument('--external', action='store_true', help='Use external Kafka bootstrap server (KAFKA_HOST:9092)')
args = parser.parse_args()

# Determine which bootstrap server to use
use_internal = args.internal or os.environ.get('USE_INTERNAL_KAFKA', 'false').lower() in ('true', '1', 'yes')
use_external = args.external or os.environ.get('USE_EXTERNAL_KAFKA', 'false').lower() in ('true', '1', 'yes')

# If both are specified, internal takes precedence
if use_internal:
    bootstrap_servers = 'broker:29092'
    print("Using internal Kafka bootstrap server: broker:29092")
    # Add localhost:29092 as a fallback
    fallback_servers = ['localhost:29092']
elif use_external:
    # Get Kafka host from environment variable or use the hostname if not set
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
    print(f"Using external Kafka bootstrap server: {bootstrap_servers}")
    # Add fallback servers
    fallback_servers = ['localhost:9092', 'broker:29092', 'localhost:29092']
else:
    # Default to internal for better compatibility with consumer commands
    bootstrap_servers = 'broker:29092'
    print("Using default internal Kafka bootstrap server: broker:29092")
    # Add localhost:29092 as a fallback
    fallback_servers = ['localhost:29092']

# Print all servers we'll try
print(f"Will try the following servers in order: {bootstrap_servers}, then {', '.join(fallback_servers)}")

print(f"Attempting to connect to Kafka at {bootstrap_servers}")

# Define topics and their configurations
topics_config = [
    # Dimension tables - fewer partitions as they're smaller and less frequently updated
    {
        'name': 'users',
        'partitions': 3,
        'replication_factor': 1,
        'config': {
            'retention.ms': str(7 * 24 * 60 * 60 * 1000),  # 7 days retention
            'cleanup.policy': 'compact'  # Keep the latest value for each key
        }
    },
    {
        'name': 'artists',
        'partitions': 3,
        'replication_factor': 1,
        'config': {
            'retention.ms': str(7 * 24 * 60 * 60 * 1000),
            'cleanup.policy': 'compact'
        }
    },
    {
        'name': 'albums',
        'partitions': 3,
        'replication_factor': 1,
        'config': {
            'retention.ms': str(7 * 24 * 60 * 60 * 1000),
            'cleanup.policy': 'compact'
        }
    },
    {
        'name': 'songs',
        'partitions': 5,
        'replication_factor': 1,
        'config': {
            'retention.ms': str(7 * 24 * 60 * 60 * 1000),
            'cleanup.policy': 'compact'
        }
    },
    {
        'name': 'devices',
        'partitions': 2,
        'replication_factor': 1,
        'config': {
            'retention.ms': str(7 * 24 * 60 * 60 * 1000),
            'cleanup.policy': 'compact'
        }
    },
    {
        'name': 'locations',
        'partitions': 3,
        'replication_factor': 1,
        'config': {
            'retention.ms': str(7 * 24 * 60 * 60 * 1000),
            'cleanup.policy': 'compact'
        }
    },
    # Stream events - more partitions for higher throughput
    {
        'name': 'stream-events',
        'partitions': 10,
        'replication_factor': 1,
        'config': {
            'retention.ms': str(3 * 24 * 60 * 60 * 1000),  # 3 days retention
            'cleanup.policy': 'delete'  # Delete old records
        }
    }
]

def try_create_topics(server):
    """Try to create topics using the specified bootstrap server"""
    print(f"\nAttempting to connect to Kafka at {server}")
    try:
        # Create admin client with timeouts
        admin_client = KafkaAdminClient(
            bootstrap_servers=server,
            client_id='music-streaming-admin',
            api_version=(2, 5, 0),
            request_timeout_ms=10000,  # 10 seconds timeout for requests
            connections_max_idle_ms=30000,  # 30 seconds max idle time
            retry_backoff_ms=500  # 0.5 seconds backoff between retries
            # socket_timeout_ms parameter removed as it's not supported in this version of kafka-python
        )

        # Get existing topics
        try:
            existing_topics = admin_client.list_topics()
            print(f"Successfully connected to {server}!")
            print(f"Existing topics: {existing_topics}")
        except Exception as e:
            print(f"Warning: Could not list existing topics: {e}")
            existing_topics = []

        # Create new topics
        new_topics = []
        for topic_config in topics_config:
            if topic_config['name'] not in existing_topics:
                new_topics.append(
                    NewTopic(
                        name=topic_config['name'],
                        num_partitions=topic_config['partitions'],
                        replication_factor=topic_config['replication_factor'],
                        topic_configs=topic_config['config']
                    )
                )
            else:
                print(f"Topic '{topic_config['name']}' already exists")

        if new_topics:
            print(f"Creating {len(new_topics)} topics...")
            admin_client.create_topics(new_topics=new_topics, validate_only=False)
            print("Topics created successfully")
        else:
            print("No new topics to create")

        # Close the admin client
        admin_client.close()
        return True

    except TopicAlreadyExistsError:
        print("Some topics already exist. Continuing...")
        return True

    except NoBrokersAvailable as e:
        print(f"No Kafka brokers available at {server}: {e}")
        return False

    except KafkaError as e:
        print(f"Error connecting to Kafka at {server}: {e}")
        return False

    except Exception as e:
        print(f"Unexpected error connecting to {server}: {e}")
        return False

def create_topics():
    """Create Kafka topics with specified configurations, trying fallback servers if needed"""
    # First try the primary bootstrap server
    if try_create_topics(bootstrap_servers):
        return True

    print(f"\nFailed to connect to primary server {bootstrap_servers}. Trying fallback servers...")

    # If that fails, try each fallback server
    for server in fallback_servers:
        print(f"\nTrying fallback server: {server}")
        if try_create_topics(server):
            return True

    # If all servers fail, print troubleshooting information
    print("\n=== TROUBLESHOOTING INFORMATION ===")
    print("All Kafka bootstrap servers failed. This usually means:")
    print("1. The Kafka broker is not running or not accessible")
    print("2. There's a network configuration issue")
    print("3. The Docker container is not properly configured")

    print("\nTry the following:")
    print("1. Check if Docker is running the Kafka container:")
    print("   docker ps | grep broker")
    print("2. Check Kafka broker logs:")
    print("   docker-compose -f kafka-docker-compose.yml logs broker")
    print("3. Test connectivity:")
    print("   telnet localhost 29092")
    print("   telnet localhost 9092")
    print("4. Restart the Kafka broker:")
    print("   docker-compose -f kafka-docker-compose.yml restart broker")
    print("5. Set the KAFKA_HOST environment variable to your machine's IP address:")
    print("   export KAFKA_HOST=$(hostname -I | awk '{print $1}')")
    print("   or")
    print("   export KAFKA_HOST=localhost")

    return False

def main():
    """Main function to create topics with retry logic"""
    max_retries = 5
    retry_interval = 5  # seconds

    for attempt in range(1, max_retries + 1):
        print(f"Attempt {attempt}/{max_retries} to create Kafka topics")

        if create_topics():
            print("All topics created or already exist")
            return 0

        if attempt < max_retries:
            print(f"Retrying in {retry_interval} seconds...")
            time.sleep(retry_interval)

    print(f"Failed to create topics after {max_retries} attempts")
    return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
