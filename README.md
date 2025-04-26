# Realtime Music Streaming Project

This project implements a real-time music streaming system using Kafka and Spark.

## Kafka Connection Issues on Raspberry Pi

When running Kafka in Docker on a Raspberry Pi, you may encounter connection issues with the default configuration. The updated scripts in this repository address these issues.

### Common Issues and Solutions

1. **KafkaTimeoutError**: If you see an error like `KafkaTimeoutError: Failed to update metadata after 60.0 secs`, it means the Kafka client cannot connect to the broker.

2. **Hostname Resolution**: Docker containers may not be accessible via `localhost` on Raspberry Pi. You need to use the actual IP address or hostname of your Raspberry Pi.

### How to Use the Updated Scripts

#### Setting the KAFKA_HOST Environment Variable

The most reliable way to connect to Kafka is to set the `KAFKA_HOST` environment variable to the IP address of your Raspberry Pi:

```bash
# Replace with your Raspberry Pi's IP address
export KAFKA_HOST=192.168.1.100
```

You can find your Raspberry Pi's IP address using:

```bash
hostname -I
```

#### Running the Producer

```bash
python kafka_producer_test.py
```

#### Running the Consumer

```bash
python kafka_consumer_test.py
```

### Script Improvements

The updated scripts include:

1. Better hostname detection when `KAFKA_HOST` is not set
2. Improved error handling with detailed error messages
3. Configurable timeouts to avoid long waits
4. API version specification for better compatibility
5. Connection retry parameters for more reliable connections

### Docker Compose Configuration

The Kafka broker is configured in `kafka-docker-compose.yml` with:

```yaml
KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://broker:29092,PLAINTEXT_HOST://${KAFKA_HOST:-localhost}:9092'
```

This means the broker advertises itself at the hostname specified by `KAFKA_HOST` (or `localhost` if not set).

## Troubleshooting

### Common Connection Issues

If you're seeing errors like `KafkaTimeoutError: Timeout after waiting for 10 secs` when trying to connect to a specific IP address (e.g., 192.168.1.88:9092), try these troubleshooting steps:

1. **Verify Docker Containers are Running**:
   ```bash
   docker-compose -f kafka-docker-compose.yml ps
   ```
   All containers should be in the "Up" state.

2. **Check Kafka Broker Logs**:
   ```bash
   docker-compose -f kafka-docker-compose.yml logs broker
   ```
   Look for any error messages or configuration issues.

3. **Verify the KAFKA_HOST Environment Variable**:
   - When running the scripts:
     ```bash
     export KAFKA_HOST=192.168.1.88
     python kafka_producer_test.py
     ```
   - When starting Docker containers:
     ```bash
     KAFKA_HOST=192.168.1.88 docker-compose -f kafka-docker-compose.yml up -d
     ```
   **Important**: The KAFKA_HOST value must be set to an IP address that is accessible from both:
   - The host machine (where you run the scripts)
   - The Docker containers (for internal communication)

4. **Test Network Connectivity**:
   ```bash
   # Check if the port is open
   telnet 192.168.1.88 9092

   # Check if you can ping the host
   ping 192.168.1.88
   ```

5. **Check Docker Network Configuration**:
   ```bash
   docker network ls
   docker network inspect streaming-network
   ```
   Verify that the broker container is properly connected to the network.

6. **Ensure No Firewall is Blocking the Connection**:
   ```bash
   sudo ufw status
   ```
   If active, make sure port 9092 is allowed.

7. **Restart the Kafka Broker**:
   ```bash
   docker-compose -f kafka-docker-compose.yml restart broker
   ```
   Sometimes a simple restart can resolve connection issues.

### Advanced Troubleshooting

If the above steps don't resolve the issue:

1. **Check Host Network Interface**:
   ```bash
   ip addr show
   ```
   Verify that the IP address you're using (e.g., 192.168.1.88) is actually assigned to your Raspberry Pi.

2. **Test with Different Network Settings**:
   Try using the Docker host network mode:
   ```yaml
   # In kafka-docker-compose.yml
   services:
     broker:
       # Add this line
       network_mode: "host"
   ```

3. **Check for Port Conflicts**:
   ```bash
   sudo netstat -tulpn | grep 9092
   ```
   Ensure no other service is using port 9092.

4. **Verify Docker DNS Resolution**:
   ```bash
   docker exec broker ping 192.168.1.88
   ```
   Check if the broker container can reach the host IP.

5. **Try with a Different Kafka Version**:
   The current configuration uses Confluent Kafka 7.9.0. You might try with a different version if compatibility issues are suspected.
