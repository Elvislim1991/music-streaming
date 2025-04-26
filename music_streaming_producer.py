import os
import socket
import time
import uuid
import json
import random
import argparse
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable

# Parse command line arguments
parser = argparse.ArgumentParser(description='Generate and send music streaming data to Kafka')
parser.add_argument('--internal', action='store_true', help='Use internal Kafka bootstrap server (broker:29092)')
parser.add_argument('--external', action='store_true', help='Use external Kafka bootstrap server (KAFKA_HOST:9092)')
args = parser.parse_args()

# Initialize Faker
fake = Faker()

# Determine which bootstrap server to use
use_internal = args.internal or os.environ.get('USE_INTERNAL_KAFKA', 'false').lower() in ('true', '1', 'yes')
use_external = args.external or os.environ.get('USE_EXTERNAL_KAFKA', 'false').lower() in ('true', '1', 'yes')

# If both are specified, internal takes precedence
if use_internal:
    bootstrap_servers = 'broker:29092'
    print("Using internal Kafka bootstrap server: broker:29092")
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
else:
    # Default to internal for better compatibility with consumer commands
    bootstrap_servers = 'broker:29092'
    print("Using default internal Kafka bootstrap server: broker:29092")

print(f"Attempting to connect to Kafka at {bootstrap_servers}")

# Extract host from bootstrap_servers
kafka_host = bootstrap_servers.split(':')[0]

# Function to check if the Kafka port is accessible
def check_kafka_port(host, port=9092, timeout=5):
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

# Generate dimension data
def generate_dimension_data():
    # Generate users
    users = []
    for i in range(1, 101):  # 100 users
        users.append({
            'user_id': i,
            'username': fake.user_name(),
            'age': random.randint(18, 65),
            'gender': random.choice(['Male', 'Female', 'Non-binary', 'Prefer not to say']),
            'subscription_type': random.choice(['Free', 'Premium', 'Family', 'Student']),
            'registration_date': fake.date_between(start_date='-3y', end_date='today').isoformat(),
            'country': fake.country()
        })

    # Generate artists
    artists = []
    for i in range(1, 51):  # 50 artists
        artists.append({
            'artist_id': i,
            'name': fake.name(),
            'country': fake.country(),
            'genre': random.choice(['Pop', 'Rock', 'Hip Hop', 'R&B', 'Country', 'Electronic', 'Jazz', 'Classical']),
            'is_band': random.choice([True, False])
        })

    # Generate albums
    albums = []
    for i in range(1, 101):  # 100 albums
        artist_id = random.randint(1, 50)
        albums.append({
            'album_id': i,
            'title': ' '.join(fake.words(nb=random.randint(1, 4))),
            'artist_id': artist_id,
            'release_date': fake.date_between(start_date='-10y', end_date='today').isoformat(),
            'total_tracks': random.randint(5, 15),
            'album_type': random.choice(['LP', 'EP', 'Single', 'Compilation'])
        })

    # Generate songs
    songs = []
    for i in range(1, 501):  # 500 songs
        album_id = random.randint(1, 100)
        album = next(album for album in albums if album['album_id'] == album_id)
        artist_id = album['artist_id']
        songs.append({
            'song_id': i,
            'title': ' '.join(fake.words(nb=random.randint(1, 6))),
            'duration_seconds': random.randint(120, 420),
            'release_date': album['release_date'],
            'genre': next(artist for artist in artists if artist['artist_id'] == artist_id)['genre'],
            'language': random.choice(['English', 'Spanish', 'French', 'German', 'Japanese', 'Korean', 'Chinese']),
            'explicit': random.choice([True, False])
        })

    # Generate devices
    devices = []
    for i in range(1, 11):  # 10 device types
        devices.append({
            'device_id': i,
            'device_type': random.choice(['Mobile', 'Desktop', 'Tablet', 'Smart Speaker', 'Smart TV', 'Car System']),
            'os': random.choice(['iOS', 'Android', 'Windows', 'macOS', 'Linux']),
            'browser': random.choice(['Chrome', 'Safari', 'Firefox', 'Edge', 'N/A']),
            'app_version': f"{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}"
        })

    # Generate locations
    locations = []
    for i in range(1, 51):  # 50 locations
        locations.append({
            'location_id': i,
            'country': fake.country(),
            'region': fake.state(),
            'city': fake.city(),
            'timezone': fake.timezone()
        })

    return {
        'users': users,
        'artists': artists,
        'albums': albums,
        'songs': songs,
        'devices': devices,
        'locations': locations
    }

# Generate a streaming event
def generate_streaming_event(dimension_data):
    user = random.choice(dimension_data['users'])
    song = random.choice(dimension_data['songs'])
    album_id = random.randint(1, 100)
    artist_id = next(album for album in dimension_data['albums'] if album['album_id'] == album_id)['artist_id']
    device = random.choice(dimension_data['devices'])
    location = random.choice(dimension_data['locations'])

    # Determine if song was skipped
    is_skipped = random.random() < 0.3  # 30% chance of skipping

    # Calculate stream duration based on skip status
    song_duration = song['duration_seconds']
    if is_skipped:
        stream_duration = random.randint(10, song_duration // 2)
        is_complete_play = False
    else:
        stream_duration = song_duration
        is_complete_play = True

    # Generate other interaction flags
    is_liked = random.random() < 0.2  # 20% chance of liking
    is_shared = random.random() < 0.1  # 10% chance of sharing
    is_added_to_playlist = random.random() < 0.15  # 15% chance of adding to playlist

    # Generate current timestamp for real-time visualization
    timestamp = datetime.now().isoformat()

    event = {
        'event_id': str(uuid.uuid4()),
        'user_id': user['user_id'],
        'song_id': song['song_id'],
        'artist_id': artist_id,
        'album_id': album_id,
        'device_id': device['device_id'],
        'location_id': location['location_id'],
        'timestamp': timestamp,
        'stream_duration_seconds': stream_duration,
        'is_complete_play': is_complete_play,
        'is_skipped': is_skipped,
        'is_liked': is_liked,
        'is_shared': is_shared,
        'is_added_to_playlist': is_added_to_playlist
    }

    return event

try:
    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 5, 0),
        request_timeout_ms=10000,
        connections_max_idle_ms=30000,
        retry_backoff_ms=500,
        max_block_ms=10000
    )

    # Generate dimension data
    print("Generating dimension data...")
    dimension_data = generate_dimension_data()

    # Create topics for dimension data
    dimension_topics = ['users', 'artists', 'albums', 'songs', 'devices', 'locations']
    stream_topic = 'stream-events'

    # Send dimension data to Kafka
    print("Sending dimension data to Kafka...")
    for topic_name in dimension_topics:
        for item in dimension_data[topic_name]:
            # Use the ID field as the key for the message
            key_field = f"{topic_name[:-1]}_id" if topic_name.endswith('s') else f"{topic_name}_id"
            if key_field in item:
                key = str(item[key_field]).encode('utf-8')
                producer.send(topic_name, key=key, value=item)
            else:
                producer.send(topic_name, value=item)

    # Flush to ensure dimension data is sent
    producer.flush()
    print("Dimension data sent successfully")

    # Generate and send streaming events continuously
    print(f"Generating and sending streaming events to topic '{stream_topic}'...")
    print("Press Ctrl+C to stop")

    event_count = 0
    try:
        while True:
            event = generate_streaming_event(dimension_data)
            # Use event_id as the key for the stream events
            key = str(event['event_id']).encode('utf-8')
            producer.send(stream_topic, key=key, value=event)
            event_count += 1

            if event_count % 10 == 0:
                producer.flush()
                print(f"Sent {event_count} events")

            # Sleep for a random time between 0.1 and 1 second
            time.sleep(random.uniform(0.1, 1))

    except KeyboardInterrupt:
        print("\nStopping event generation")
        producer.flush()
        print(f"Total events sent: {event_count}")

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
