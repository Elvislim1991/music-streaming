from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, count, avg, sum, expr, when, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType
import json
import traceback
import time

# Define schema for stream events
stream_events_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("song_id", IntegerType(), True),
    StructField("artist_id", IntegerType(), True),
    StructField("album_id", IntegerType(), True),
    StructField("device_id", IntegerType(), True),
    StructField("location_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("stream_duration_seconds", IntegerType(), True),
    StructField("is_complete_play", BooleanType(), True),
    StructField("is_skipped", BooleanType(), True),
    StructField("is_liked", BooleanType(), True),
    StructField("is_shared", BooleanType(), True),
    StructField("is_added_to_playlist", BooleanType(), True)
])

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MusicStreamingAnalytics") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.5.0.jar") \
    .getOrCreate()

# Set log level to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Postgres connection properties
postgres_properties = {
    "url": "jdbc:postgresql://postgres:5432/music_streaming",
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Read streaming data from Kafka
stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "stream-events") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON data from Kafka
parsed_stream_df = stream_df \
    .selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json("json_data", stream_events_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp("timestamp"))

# Add watermark to handle late data - this is required for aggregations with append mode
# The watermark specifies the threshold of how late the data is expected to be
# Using a smaller watermark (2 minutes) for more real-time visualization
parsed_stream_df = parsed_stream_df.withWatermark("timestamp", "2 minutes")

# Create a temporary view for SQL queries
parsed_stream_df.createOrReplaceTempView("stream_events")

# Define various metrics to compute

# 1. User engagement metrics - 1 minute windows for real-time visualization
user_engagement_df = spark.sql("""
    SELECT 
        window(timestamp, '1 minute') as time_window,
        user_id,
        COUNT(*) as stream_count,
        SUM(stream_duration_seconds) as total_listen_time,
        AVG(stream_duration_seconds) as avg_listen_time,
        SUM(CASE WHEN is_skipped THEN 1 ELSE 0 END) / COUNT(*) as skip_rate,
        SUM(CASE WHEN is_liked THEN 1 ELSE 0 END) / COUNT(*) as like_rate,
        SUM(CASE WHEN is_shared THEN 1 ELSE 0 END) / COUNT(*) as share_rate,
        SUM(CASE WHEN is_added_to_playlist THEN 1 ELSE 0 END) / COUNT(*) as playlist_add_rate
    FROM stream_events
    GROUP BY window(timestamp, '1 minute'), user_id
""")

# 2. Content performance metrics - 1 minute windows for real-time visualization
content_performance_df = spark.sql("""
    SELECT 
        window(timestamp, '1 minute') as time_window,
        song_id,
        artist_id,
        COUNT(*) as stream_count,
        SUM(CASE WHEN is_complete_play THEN 1 ELSE 0 END) / COUNT(*) as completion_rate,
        SUM(CASE WHEN is_skipped THEN 1 ELSE 0 END) / COUNT(*) as skip_rate,
        SUM(CASE WHEN is_liked THEN 1 ELSE 0 END) / COUNT(*) as like_rate
    FROM stream_events
    GROUP BY window(timestamp, '1 minute'), song_id, artist_id
""")

# 3. Device metrics - 1 minute windows for real-time visualization
device_metrics_df = spark.sql("""
    SELECT 
        window(timestamp, '1 minute') as time_window,
        device_id,
        COUNT(*) as stream_count,
        AVG(stream_duration_seconds) as avg_listen_time,
        SUM(CASE WHEN is_complete_play THEN 1 ELSE 0 END) / COUNT(*) as completion_rate
    FROM stream_events
    GROUP BY window(timestamp, '1 minute'), device_id
""")

# 4. Location metrics - 1 minute windows for real-time visualization
location_metrics_df = spark.sql("""
    SELECT 
        window(timestamp, '1 minute') as time_window,
        location_id,
        COUNT(*) as stream_count,
        approx_count_distinct(user_id) as unique_users,
        approx_count_distinct(song_id) as unique_songs
    FROM stream_events
    GROUP BY window(timestamp, '1 minute'), location_id
""")

# 5. Hourly metrics - 1 hour windows
hourly_metrics_df = spark.sql("""
    SELECT 
        window(timestamp, '1 hour') as time_window,
        COUNT(*) as stream_count,
        approx_count_distinct(user_id) as unique_users,
        approx_count_distinct(song_id) as unique_songs,
        approx_count_distinct(artist_id) as unique_artists,
        SUM(stream_duration_seconds) as total_listen_time,
        AVG(stream_duration_seconds) as avg_listen_time,
        SUM(CASE WHEN is_skipped THEN 1 ELSE 0 END) / COUNT(*) as skip_rate,
        SUM(CASE WHEN is_liked THEN 1 ELSE 0 END) / COUNT(*) as like_rate
    FROM stream_events
    GROUP BY window(timestamp, '1 hour')
""")

# Function to handle time_window column for JDBC and real-time visualization
def transform_time_window(df):
    # Extract both start and end times from the window for better real-time visualization
    # This allows the dashboard to show the exact time range for each data point
    # First, extract the window start and end
    df_with_window = df.withColumnRenamed("time_window", "window_struct") \
                       .withColumn("time_window", col("window_struct.start")) \
                       .withColumn("window_end", col("window_struct.end")) \
                       .drop("window_struct")

    # Drop the window_end column before writing to PostgreSQL
    # This is necessary because the PostgreSQL tables don't have a window_end column
    return df_with_window.drop("window_end")

# Function to handle batch processing with error handling and dead letter queue
def process_batch_with_dlq(df, epoch_id, table_name, properties):
    """
    Process a batch of data with error handling and dead letter queue.

    Args:
        df: DataFrame to process
        epoch_id: Batch ID
        table_name: Target table name in PostgreSQL
        properties: JDBC connection properties
    """
    try:
        # Try to write the batch to PostgreSQL
        df.write.jdbc(
            url=postgres_properties["url"],
            table=table_name,
            mode="append",
            properties=properties
        )
        print(f"Successfully wrote batch {epoch_id} to {table_name}")
    except Exception as e:
        error_message = str(e)
        stack_trace = traceback.format_exc()
        print(f"Error writing batch {epoch_id} to {table_name}: {error_message}")
        print(f"Stack trace: {stack_trace}")

        # Check if it's a foreign key constraint violation
        if "violates foreign key constraint" in error_message:
            handle_foreign_key_violation(df, epoch_id, table_name, properties, error_message)
        else:
            # For other errors, send all records to the dead letter queue
            send_to_dead_letter_queue(df, epoch_id, table_name, error_message)

def handle_foreign_key_violation(df, epoch_id, table_name, properties, error_message):
    """
    Handle foreign key constraint violations by filtering out invalid records.

    Args:
        df: DataFrame to process
        epoch_id: Batch ID
        table_name: Target table name in PostgreSQL
        properties: JDBC connection properties
        error_message: The error message from the exception
    """
    print(f"Handling foreign key constraint violation for batch {epoch_id}")
    print(f"Error message: {error_message}")

    # Extract the constraint name and column from the error message
    # Example: "violates foreign key constraint "stream_events_user_id_fkey""
    constraint_start = error_message.find('"', error_message.find("constraint")) + 1
    constraint_end = error_message.find('"', constraint_start)
    constraint_name = error_message[constraint_start:constraint_end] if constraint_start > 0 and constraint_end > 0 else ""

    # Extract the column name directly from the error message
    # Example: "Key (user_id)=(6) is not present in table "users""
    key_start = error_message.find('(', error_message.find("Key")) + 1
    key_end = error_message.find(')', key_start)
    column_name = error_message[key_start:key_end] if key_start > 0 and key_end > 0 else ""

    # Extract the invalid value from the error message
    # Example: "Key (user_id)=(69) is not present in table "users""
    value_start = error_message.find('=', key_end) + 1 if key_end > 0 else 0
    if value_start > 0:
        # Check if the value is enclosed in parentheses
        if error_message[value_start:].strip().startswith('('):
            value_start = error_message.find('(', value_start) + 1
            value_end = error_message.find(')', value_start)
        else:
            # If not in parentheses, find the end of the value (space, newline, etc.)
            value_end = error_message.find(' ', value_start)
            if value_end == -1:  # No space found, try to find end of line
                value_end = error_message.find('\n', value_start)
            if value_end == -1:  # No newline found, use the rest of the string
                value_end = len(error_message)
        invalid_value = error_message[value_start:value_end].strip()
    else:
        invalid_value = ""

    print(f"Identified constraint: {constraint_name}, column: {column_name}, invalid value: {invalid_value}")

    # Debug information about the DataFrame schema
    print(f"DataFrame columns: {df.columns}")

    # Verify the column exists in the DataFrame
    if column_name and column_name not in df.columns:
        print(f"Warning: Column '{column_name}' not found in DataFrame. Available columns: {df.columns}")
        column_name = ""  # Reset column_name to avoid filtering on a non-existent column

    if column_name and invalid_value:
        # Split the invalid records from valid ones
        invalid_records = df.filter(col(column_name) == invalid_value)
        valid_records = df.filter(col(column_name) != invalid_value)

        # Send invalid records to the dead letter queue
        invalid_count = invalid_records.count()
        if invalid_count > 0:
            print(f"Found {invalid_count} records with invalid {column_name}={invalid_value}")
            send_to_dead_letter_queue(invalid_records, epoch_id, table_name, 
                                     f"Foreign key violation: {column_name}={invalid_value}")

        # Try to write the valid records to PostgreSQL
        valid_count = valid_records.count()
        if valid_count > 0:
            print(f"Attempting to write {valid_count} valid records to {table_name}")
            try:
                valid_records.write.jdbc(
                    url=postgres_properties["url"],
                    table=table_name,
                    mode="append",
                    properties=properties
                )
                print(f"Successfully wrote {valid_count} valid records to {table_name}")
            except Exception as e:
                print(f"Error writing valid records: {str(e)}")
                # If there's still an error, send all valid records to the dead letter queue
                send_to_dead_letter_queue(valid_records, epoch_id, table_name, f"Secondary error: {str(e)}")
    else:
        # If we couldn't parse the error message, send all records to the dead letter queue
        print("Could not parse error message to identify invalid records")
        send_to_dead_letter_queue(df, epoch_id, table_name, error_message)

def send_to_dead_letter_queue(df, epoch_id, table_name, error_message):
    """
    Send records to the dead letter queue.

    Args:
        df: DataFrame with records to send
        epoch_id: Batch ID
        table_name: Target table name
        error_message: Error message to include
    """
    # Create a DataFrame with the error information
    error_info = {
        "table": table_name,
        "batch_id": epoch_id,
        "error_message": error_message,
        "timestamp": str(time.time())
    }

    # Add error information to each row
    df_with_error = df.withColumn("error_info", lit(json.dumps(error_info)))

    # Write the failed records to a dead letter queue topic in Kafka
    df_with_error.selectExpr("to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("topic", "dead-letter-queue") \
        .save()

    print(f"Wrote {df.count()} records to dead-letter-queue topic")

# Write user engagement metrics to Postgres in real-time with error handling
user_engagement_query = user_engagement_df \
    .transform(transform_time_window) \
    .writeStream \
    .foreachBatch(lambda df, epoch_id: process_batch_with_dlq(
        df=df,
        epoch_id=epoch_id,
        table_name="user_engagement_metrics",
        properties={
            "user": postgres_properties["user"],
            "password": postgres_properties["password"],
            "driver": postgres_properties["driver"]
        }
    )) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/user_engagement") \
    .start()

# Write content performance metrics to Postgres in real-time with error handling
content_performance_query = content_performance_df \
    .transform(transform_time_window) \
    .writeStream \
    .foreachBatch(lambda df, epoch_id: process_batch_with_dlq(
        df=df,
        epoch_id=epoch_id,
        table_name="content_performance_metrics",
        properties={
            "user": postgres_properties["user"],
            "password": postgres_properties["password"],
            "driver": postgres_properties["driver"]
        }
    )) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/content_performance") \
    .start()

# Write device metrics to Postgres in real-time with error handling
device_metrics_query = device_metrics_df \
    .transform(transform_time_window) \
    .writeStream \
    .foreachBatch(lambda df, epoch_id: process_batch_with_dlq(
        df=df,
        epoch_id=epoch_id,
        table_name="device_metrics",
        properties={
            "user": postgres_properties["user"],
            "password": postgres_properties["password"],
            "driver": postgres_properties["driver"]
        }
    )) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/device_metrics") \
    .start()

# Write location metrics to Postgres in real-time with error handling
location_metrics_query = location_metrics_df \
    .transform(transform_time_window) \
    .writeStream \
    .foreachBatch(lambda df, epoch_id: process_batch_with_dlq(
        df=df,
        epoch_id=epoch_id,
        table_name="location_metrics",
        properties={
            "user": postgres_properties["user"],
            "password": postgres_properties["password"],
            "driver": postgres_properties["driver"]
        }
    )) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/location_metrics") \
    .start()

# Write hourly metrics to Postgres in real-time with error handling
hourly_metrics_query = hourly_metrics_df \
    .transform(transform_time_window) \
    .writeStream \
    .foreachBatch(lambda df, epoch_id: process_batch_with_dlq(
        df=df,
        epoch_id=epoch_id,
        table_name="hourly_metrics",
        properties={
            "user": postgres_properties["user"],
            "password": postgres_properties["password"],
            "driver": postgres_properties["driver"]
        }
    )) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/hourly_metrics") \
    .start()

# Write raw events to Postgres in real-time with error handling and dead letter queue
raw_events_query = parsed_stream_df \
    .writeStream \
    .foreachBatch(lambda df, epoch_id: process_batch_with_dlq(
        df=df,
        epoch_id=epoch_id,
        table_name="stream_events",
        properties={
            "user": postgres_properties["user"],
            "password": postgres_properties["password"],
            "driver": postgres_properties["driver"],
            "stringtype": "unspecified"
        }
    )) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/raw_events") \
    .start()

# Print the streaming queries info
print("Active streaming queries:")
for query in spark.streams.active:
    print(f"Query ID: {query.id}, Name: {query.name}")

# Wait for all queries to terminate
spark.streams.awaitAnyTermination()
