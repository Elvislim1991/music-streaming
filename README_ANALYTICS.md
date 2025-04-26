# Music Streaming Analytics Pipeline

This document provides instructions for running the complete music streaming analytics pipeline.

## Overview

The pipeline consists of the following components:

1. **Kafka Cluster**: For ingesting streaming events
2. **Spark Cluster**: For processing streaming data
3. **Postgres Database**: For storing processed data
4. **Apache Superset**: For visualizing metrics

## Prerequisites

- Docker and Docker Compose installed
- Python 3.8+ with pip
- Required Python packages: `kafka-python`, `faker`, `pyspark`

You can install the required Python packages with:

```bash
pip install kafka-python faker pyspark
```

## Step 1: Start Kafka Cluster

```bash
# Set KAFKA_HOST to your machine's IP address
export KAFKA_HOST=$(hostname -I | awk '{print $1}')
echo "Using KAFKA_HOST=$KAFKA_HOST"

# Start Kafka
docker-compose -f kafka-docker-compose.yml up -d
```

Verify that Kafka is running:

```bash
docker ps | grep broker
```

## Step 2: Create Kafka Topics

Run the script to create Kafka topics with appropriate partitioning:

```bash
python create_kafka_topics.py
```

## Step 3: Start Spark Cluster

```bash
docker-compose -f spark-docker-compose.yml up -d
```

Verify that Spark is running:

```bash
docker ps | grep spark
```

## Step 4: Start Postgres Database

```bash
docker-compose -f postgres-docker-compose.yml up -d
```

Verify that Postgres is running:

```bash
docker ps | grep postgres
```

## Step 5: Initialize Postgres Schema

```bash
# Copy the schema file to the Postgres container
docker cp setup_postgres_schema.sql postgres:/tmp/

# Execute the schema file
docker exec -it postgres psql -U postgres -d music_streaming -f /tmp/setup_postgres_schema.sql
```

## Step 6: Start Apache Superset

```bash
docker-compose -f superset-docker-compose.yml up -d
```

Verify that Superset is running:

```bash
docker ps | grep superset
```

Access Superset with the following credentials:
- Username: admin
- Password: admin

For local access: http://localhost:8088
For remote access: http://<raspberry-pi-ip-address>:8088 (e.g., http://192.168.1.88:8088)

## Step 7: Generate Streaming Data

Run the Faker-based Kafka producer to generate streaming events:

```bash
python music_streaming_producer.py
```

This will continuously generate music streaming events and send them to Kafka.

## Step 8: Process Streaming Data with Spark

Copy the Spark streaming job to the Spark master container:

```bash
docker cp spark_streaming_job.py spark-master:/opt/bitnami/spark/
```

Execute the Spark streaming job:

```bash
docker exec -it spark-master bash -c "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 spark_streaming_job.py"
```

This will start processing the streaming data and writing metrics directly to PostgreSQL in real-time.

### Note on Watermarks

The Spark streaming job uses a watermark of 10 minutes on the timestamp column. This means:

- The system will wait for late data up to 10 minutes after the event time
- Data arriving more than 10 minutes late (based on the event timestamp) may be dropped
- Aggregation results for a time window will be finalized after the watermark threshold has passed

Watermarks are required when using append output mode with streaming aggregations that use window functions. They help manage state and handle late-arriving data in a streaming context.

### Note on Approximate Distinct Counts

For metrics that count unique users or songs (like unique_users and unique_songs), the streaming job uses `approx_count_distinct()` instead of `COUNT(DISTINCT)`. This is because Spark Structured Streaming doesn't support exact distinct counts in streaming queries.

The approximate count distinct function:
- Provides an estimate of the number of distinct items
- Has a small margin of error (typically 2-5%)
- Uses significantly less memory than exact counts
- Is the recommended approach for distinct counting in streaming applications

## Step 9: Copy the PostgreSQL JDBC Driver

Copy the PostgreSQL JDBC driver to the Spark master container:

```bash
# Download the PostgreSQL JDBC driver if you don't have it
wget https://jdbc.postgresql.org/download/postgresql-42.5.0.jar

# Copy it to the Spark master container
docker cp postgresql-42.5.0.jar spark-master:/opt/bitnami/spark/jars/
```

The Spark streaming job is configured to write data directly to PostgreSQL in real-time, so there's no need for a separate data loading step.

## Step 10: Validate the Data Model

Connect to Postgres and run the validation queries:

```bash
docker cp validation_queries.sql postgres:/tmp/
docker exec -it postgres psql -U postgres -d music_streaming -f /tmp/validation_queries.sql
```

## Step 11: Visualize Metrics in Superset

1. Log in to Superset at http://localhost:8088
2. Add a new database connection:
   - Go to Data -> Databases -> + Database
   - Set the SQLAlchemy URI to: `postgresql://postgres:postgres@postgres:5432/music_streaming`
   - Test the connection and save
3. Create dataset connections:
   - Go to Data -> Datasets -> + Dataset
   - Select the database you just created
   - Create datasets for each table (stream_events, user_engagement_metrics, etc.)
4. Create dashboards:
   - Go to Dashboards -> + Dashboard
   - Add charts based on the datasets you created

### Example Visualizations

Here are some example visualizations you might want to create:

1. **User Engagement Dashboard**:
   - Streams per user by subscription type (Bar chart)
   - Skip rate by age group (Bar chart)
   - Streaming activity by hour of day (Line chart)
   - Like rate by subscription type (Bar chart)

2. **Content Performance Dashboard**:
   - Top 10 most streamed songs (Bar chart)
   - Skip rate by genre (Bar chart)
   - Like rate by genre (Bar chart)
   - Trending songs (Bar chart)

3. **Geographic Dashboard**:
   - Streaming activity by country (World map)
   - Unique users by country (World map)
   - Streaming patterns by region (Bar chart)

4. **Device Usage Dashboard**:
   - Streaming activity by device type (Pie chart)
   - Completion rate by device type (Bar chart)
   - Average listen time by device type and OS (Heat map)

## Troubleshooting

### Kafka Connection Issues

If you encounter Kafka connection issues, make sure:
- The KAFKA_HOST environment variable is set correctly
- Kafka containers are running
- Network connectivity between containers is working

### Spark Job Failures

If Spark jobs fail, check:
- Spark container logs: `docker logs spark-master`
- Make sure the Kafka topics exist and have data
- Verify that the Spark job has the correct Kafka bootstrap servers

### Postgres Connection Issues

If you can't connect to Postgres, check:
- Postgres container is running
- Database and user credentials are correct
- Network connectivity between containers

### Superset Issues

If Superset doesn't work correctly:
- Check Superset logs: `docker logs superset`
- Verify the database connection settings
- Make sure the Postgres database has data

If you cannot connect to Superset from another machine (e.g., "Connection refused" when trying to telnet to port 8088):
1. **Verify Superset Container is Running**:
   ```bash
   docker ps | grep superset
   ```
   Make sure it's in the "Up" state.

2. **Check Superset Logs for Binding Issues**:
   ```bash
   docker logs superset
   ```
   Look for messages about which address it's binding to.

3. **Ensure No Firewall is Blocking the Connection**:
   ```bash
   sudo ufw status
   ```
   If active, make sure port 8088 is allowed:
   ```bash
   sudo ufw allow 8088/tcp
   ```

4. **Test Network Connectivity**:
   ```bash
   # From another machine
   telnet <raspberry-pi-ip> 8088
   ping <raspberry-pi-ip>
   ```

5. **Restart the Superset Container**:
   ```bash
   docker-compose -f superset-docker-compose.yml restart superset
   ```

## Shutting Down

To shut down all services:

```bash
docker-compose -f superset-docker-compose.yml down
docker-compose -f postgres-docker-compose.yml down
docker-compose -f spark-docker-compose.yml down
docker-compose -f kafka-docker-compose.yml down
```

To remove all data volumes:

```bash
docker volume prune
```
