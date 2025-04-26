#!/bin/bash

# Music Streaming Analytics Pipeline Setup Script
# This script automates the setup of the music streaming analytics pipeline
# as described in README_ANALYTICS.md

# Exit on any error
set -e

# Function to display usage information
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  -h, --help                 Display this help message"
    echo "  -k, --kafka-only           Start only Kafka"
    echo "  -s, --spark-only           Start only Spark"
    echo "  -p, --postgres-only        Start only Postgres"
    echo "  -u, --superset-only        Start only Superset"
    echo "  -d, --data-only            Generate and process data only"
    echo "  -a, --all                  Start all components (default)"
    echo "  -c, --clean                Shut down all services and remove volumes"
    echo "  --internal                 Use internal Kafka bootstrap server"
    echo "  --external                 Use external Kafka bootstrap server"
    exit 1
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check if a container is running
container_running() {
    docker ps | grep "$1" >/dev/null 2>&1
}

# Function to print section headers
print_section() {
    echo "============================================================"
    echo "$1"
    echo "============================================================"
}

# Function to check prerequisites
check_prerequisites() {
    print_section "Checking prerequisites"
    
    # Check if Docker is installed
    if ! command_exists docker; then
        echo "Error: Docker is not installed. Please install Docker and try again."
        exit 1
    fi
    
    # Check if Docker Compose is installed
    if ! command_exists docker compose; then
        echo "Error: Docker Compose is not installed. Please install Docker Compose and try again."
        exit 1
    fi
    
    # Check if Python is installed
    if ! command_exists python3; then
        echo "Error: Python 3 is not installed. Please install Python 3 and try again."
        exit 1
    fi
    
    # Check if pip is installed
    if ! command_exists pip3; then
        echo "Error: pip3 is not installed. Please install pip3 and try again."
        exit 1
    fi
    
    # Install required Python packages
    echo "Installing required Python packages..."
    pip3 install kafka-python faker pyspark psycopg2-binary matplotlib pandas
    
    echo "All prerequisites are satisfied."
}

# Function to start Kafka
start_kafka() {
    print_section "Starting Kafka Cluster"
    
    # Set KAFKA_HOST to the machine's IP address
    export KAFKA_HOST=$(hostname -I | awk '{print $1}')
    echo "Using KAFKA_HOST=$KAFKA_HOST"
    
    # Start Kafka
    docker compose -f kafka-docker-compose.yml up -d
    
    # Verify that Kafka is running
    echo "Verifying Kafka is running..."
    sleep 5
    if container_running "broker"; then
        echo "Kafka is running."
    else
        echo "Error: Kafka failed to start. Check the logs with 'docker compose -f kafka-docker-compose.yml logs'."
        exit 1
    fi
}

# Function to create Kafka topics
create_kafka_topics() {
    print_section "Creating Kafka Topics"
    
    # Create Kafka topics
    if [ "$USE_EXTERNAL" = true ]; then
        echo "Using external bootstrap server..."
        python3 create_kafka_topics.py --external
    elif [ "$USE_INTERNAL" = true ]; then
        echo "Using internal bootstrap server..."
        python3 create_kafka_topics.py --internal
    else
        echo "Using default bootstrap server..."
        python3 create_kafka_topics.py
    fi
    
    echo "Kafka topics created successfully."
}

# Function to start Spark
start_spark() {
    print_section "Starting Spark Cluster"
    
    # Start Spark
    docker compose -f spark-docker-compose.yml up -d
    
    # Verify that Spark is running
    echo "Verifying Spark is running..."
    sleep 5
    if container_running "spark"; then
        echo "Spark is running."
    else
        echo "Error: Spark failed to start. Check the logs with 'docker compose -f spark-docker-compose.yml logs'."
        exit 1
    fi
}

# Function to start Postgres
start_postgres() {
    print_section "Starting Postgres Database"
    
    # Start Postgres
    docker compose -f postgres-docker-compose.yml up -d
    
    # Verify that Postgres is running
    echo "Verifying Postgres is running..."
    sleep 5
    if container_running "postgres"; then
        echo "Postgres is running."
    else
        echo "Error: Postgres failed to start. Check the logs with 'docker compose -f postgres-docker-compose.yml logs'."
        exit 1
    fi
}

# Function to initialize Postgres schema
initialize_postgres_schema() {
    print_section "Initializing Postgres Schema"
    
    # Wait for Postgres to be ready
    echo "Waiting for Postgres to be ready..."
    sleep 5
    
    # Copy the schema file to the Postgres container
    echo "Copying schema file to Postgres container..."
    docker cp setup_postgres_schema.sql postgres:/tmp/
    
    # Execute the schema file
    echo "Executing schema file..."
    docker exec -it postgres psql -U postgres -d music_streaming -f /tmp/setup_postgres_schema.sql
    
    echo "Postgres schema initialized successfully."
}

# Function to start Superset
start_superset() {
    print_section "Starting Apache Superset"
    
    # Start Superset
    docker compose -f superset-docker-compose.yml up -d
    
    # Verify that Superset is running
    echo "Verifying Superset is running..."
    sleep 5
    if container_running "superset"; then
        echo "Superset is running."
        echo "Access Superset at http://localhost:8088 with username: admin, password: admin"
    else
        echo "Error: Superset failed to start. Check the logs with 'docker compose -f superset-docker-compose.yml logs'."
        exit 1
    fi
}

# Function to generate streaming data
generate_streaming_data() {
    print_section "Generating Streaming Data"
    
    # Generate streaming data
    if [ "$USE_EXTERNAL" = true ]; then
        echo "Using external bootstrap server..."
        python3 music_streaming_producer.py --external &
    elif [ "$USE_INTERNAL" = true ]; then
        echo "Using internal bootstrap server..."
        python3 music_streaming_producer.py --internal &
    else
        echo "Using default bootstrap server..."
        python3 music_streaming_producer.py &
    fi
    
    # Save the PID to kill it later if needed
    PRODUCER_PID=$!
    echo "Streaming data generator started with PID $PRODUCER_PID"
    echo "Press Ctrl+C to stop the data generator when you're done."
}

# Function to load dimension data into PostgreSQL
load_dimension_data() {
    print_section "Loading Dimension Data into PostgreSQL"
    
    # Copy the dimension data loader script to the Spark master container
    echo "Copying dimension data loader script to Spark master container..."
    docker cp dimension_data_loader.py spark-master:/opt/bitnami/spark/
    
    # Execute the dimension data loader script
    echo "Executing dimension data loader script..."
    docker exec -it spark-master bash -c "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 dimension_data_loader.py"
    
    echo "Dimension data loaded successfully."
}

# Function to process streaming data with Spark
process_streaming_data() {
    print_section "Processing Streaming Data with Spark"
    
    # Copy the PostgreSQL JDBC driver to the Spark master container
    echo "Downloading PostgreSQL JDBC driver..."
    if [ ! -f postgresql-42.5.0.jar ]; then
        wget https://jdbc.postgresql.org/download/postgresql-42.5.0.jar
    fi
    
    echo "Copying PostgreSQL JDBC driver to Spark master container..."
    docker cp postgresql-42.5.0.jar spark-master:/opt/bitnami/spark/jars/
    
    # Copy the Spark streaming job to the Spark master container
    echo "Copying Spark streaming job to Spark master container..."
    docker cp spark_streaming_job.py spark-master:/opt/bitnami/spark/
    
    # Execute the Spark streaming job
    echo "Executing Spark streaming job..."
    docker exec -it spark-master bash -c "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 spark_streaming_job.py" &
    
    # Save the PID to kill it later if needed
    SPARK_PID=$!
    echo "Spark streaming job started with PID $SPARK_PID"
    echo "Press Ctrl+C to stop the Spark job when you're done."
}

# Function to validate the data model
validate_data_model() {
    print_section "Validating Data Model"
    
    # Copy the validation queries to the Postgres container
    echo "Copying validation queries to Postgres container..."
    docker cp validation_queries.sql postgres:/tmp/
    
    # Execute the validation queries
    echo "Executing validation queries..."
    docker exec -it postgres psql -U postgres -d music_streaming -f /tmp/validation_queries.sql
    
    echo "Data model validated successfully."
}

# Function to shut down all services
shutdown_services() {
    print_section "Shutting Down All Services"
    
    echo "Shutting down Superset..."
    docker compose -f superset-docker-compose.yml down
    
    echo "Shutting down Postgres..."
    docker compose -f postgres-docker-compose.yml down
    
    echo "Shutting down Spark..."
    docker compose -f spark-docker-compose.yml down
    
    echo "Shutting down Kafka..."
    docker compose -f kafka-docker-compose.yml down
    
    echo "All services shut down successfully."
}

# Function to clean up volumes
clean_volumes() {
    print_section "Cleaning Up Volumes"
    
    echo "Removing all data volumes..."
    docker volume prune -f
    
    echo "Volumes cleaned up successfully."
}

# Default options
START_KAFKA=false
START_SPARK=false
START_POSTGRES=false
START_SUPERSET=false
GENERATE_DATA=false
PROCESS_DATA=false
CLEAN=false
USE_INTERNAL=false
USE_EXTERNAL=false

# Parse command-line options
if [ $# -eq 0 ]; then
    # If no options are provided, start all components
    START_KAFKA=true
    START_SPARK=true
    START_POSTGRES=true
    START_SUPERSET=true
    GENERATE_DATA=true
    PROCESS_DATA=true
else
    while [ $# -gt 0 ]; do
        case "$1" in
            -h|--help)
                usage
                ;;
            -k|--kafka-only)
                START_KAFKA=true
                ;;
            -s|--spark-only)
                START_SPARK=true
                ;;
            -p|--postgres-only)
                START_POSTGRES=true
                ;;
            -u|--superset-only)
                START_SUPERSET=true
                ;;
            -d|--data-only)
                GENERATE_DATA=true
                PROCESS_DATA=true
                ;;
            -a|--all)
                START_KAFKA=true
                START_SPARK=true
                START_POSTGRES=true
                START_SUPERSET=true
                GENERATE_DATA=true
                PROCESS_DATA=true
                ;;
            -c|--clean)
                CLEAN=true
                ;;
            --internal)
                USE_INTERNAL=true
                ;;
            --external)
                USE_EXTERNAL=true
                ;;
            *)
                echo "Unknown option: $1"
                usage
                ;;
        esac
        shift
    done
fi

# Main execution
if [ "$CLEAN" = true ]; then
    shutdown_services
    clean_volumes
    exit 0
fi

# Check prerequisites
check_prerequisites

# Start components based on options
if [ "$START_KAFKA" = true ]; then
    start_kafka
    create_kafka_topics
fi

if [ "$START_SPARK" = true ]; then
    start_spark
fi

if [ "$START_POSTGRES" = true ]; then
    start_postgres
    initialize_postgres_schema
fi

if [ "$START_SUPERSET" = true ]; then
    start_superset
fi

if [ "$GENERATE_DATA" = true ]; then
    generate_streaming_data
fi

if [ "$PROCESS_DATA" = true ]; then
    load_dimension_data
    process_streaming_data
    validate_data_model
fi

echo "Setup completed successfully!"
echo "To shut down all services, run: $0 -c"