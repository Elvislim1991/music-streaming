# Music Streaming Analytics Pipeline Setup Script

This README provides instructions for using the `setup.sh` script to automate the setup of the music streaming analytics pipeline.

## Overview

The `setup.sh` script automates all the steps described in the `README_ANALYTICS.md` file, including:

1. Starting Kafka, Spark, and Postgres clusters
2. Creating Kafka topics
3. Initializing the Postgres schema
4. Starting Apache Superset
5. Generating streaming data
6. Processing streaming data with Spark

## Prerequisites

Before using the script, make sure you have:

- Docker and Docker Compose installed
- Python 3.8+ with pip
- Internet connection (for downloading dependencies)

## Getting Started

1. Make the script executable:
   ```bash
   chmod +x setup.sh
   ```

2. Run the script with no options to start all components:
   ```bash
   ./setup.sh
   ```

## Command-Line Options

The script supports the following command-line options:

- `-h, --help`: Display help message
- `-k, --kafka-only`: Start only Kafka
- `-s, --spark-only`: Start only Spark
- `-p, --postgres-only`: Start only Postgres
- `-u, --superset-only`: Start only Superset
- `-d, --data-only`: Generate and process data only
- `-a, --all`: Start all components (default)
- `-c, --clean`: Shut down all services and remove volumes
- `--internal`: Use internal Kafka bootstrap server
- `--external`: Use external Kafka bootstrap server

## Examples

1. Start only Kafka:
   ```bash
   ./setup.sh --kafka-only
   ```

2. Start Kafka and Spark:
   ```bash
   ./setup.sh --kafka-only --spark-only
   ```

3. Generate and process data (assuming services are already running):
   ```bash
   ./setup.sh --data-only
   ```

4. Shut down all services and clean up:
   ```bash
   ./setup.sh --clean
   ```

5. Use external Kafka bootstrap server:
   ```bash
   ./setup.sh --external
   ```

## Troubleshooting

### General Issues

If you encounter issues:

1. Check the logs of the specific service:
   ```bash
   docker-compose -f <service>-docker-compose.yml logs
   ```

2. Make sure all prerequisites are installed.

3. Verify that the required ports are available on your system.

4. Check that the KAFKA_HOST environment variable is set correctly.

### Kafka Connectivity Issues

If the script gets stuck when creating Kafka topics (especially on Raspberry Pi):

1. **Check Kafka Connectivity**: Test if you can connect to Kafka using both the internal and external addresses:
   ```bash
   # Test internal connectivity
   telnet localhost 29092

   # Test external connectivity (using your IP address)
   telnet $(hostname -I | awk '{print $1}') 9092
   ```

2. **Try Using Localhost**: If external connectivity fails but internal works, try:
   ```bash
   export KAFKA_HOST=localhost
   ./setup.sh
   ```

3. **Check Docker Network**: Make sure the Docker network is properly configured:
   ```bash
   docker network inspect streaming-network
   ```

4. **Restart Kafka**: Try restarting the Kafka container:
   ```bash
   docker-compose -f kafka-docker-compose.yml restart broker
   ```

5. **Check Firewall**: Make sure your firewall isn't blocking the required ports:
   ```bash
   sudo ufw status
   ```

6. **Hostname Resolution Issues**: If you see errors like "Server lookup failure: broker:9092, No address associated with hostname", this means the hostname "broker" cannot be resolved to an IP address. The setup script now includes automatic hostname resolution checks and will:
   - Try to add "broker" to your /etc/hosts file (requires sudo)
   - Automatically fall back to using "localhost" instead of "broker" if the hostname is not resolvable

   If you still encounter hostname resolution issues, you can manually add an entry to your /etc/hosts file:
   ```bash
   echo "127.0.0.1 broker" | sudo tee -a /etc/hosts
   ```

7. **Kafka Configuration Errors**: If you see errors like "KafkaConfigurationError: Unrecognized configs", this usually means there's an incompatibility between the Kafka client library and the version of kafka-python installed:
   ```
   Error connecting to Kafka at broker:29092: KafkaConfigurationError: Unrecognized configs: {'socket_timeout_ms'}
   ```
   This has been fixed in the latest version of the script, but if you encounter similar errors with other parameters, you may need to update your kafka-python library or modify the script to remove the unsupported parameters.

The setup script now includes improved error handling and will automatically try alternative connection methods if the default one fails.

## Shutting Down

To shut down all services and clean up volumes:

```bash
./setup.sh --clean
```

This will stop all Docker containers and remove all data volumes.
