from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, count, avg, sum, expr, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType

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
parsed_stream_df = parsed_stream_df.withWatermark("timestamp", "10 minutes")

# Create a temporary view for SQL queries
parsed_stream_df.createOrReplaceTempView("stream_events")

# Define various metrics to compute

# 1. User engagement metrics - 5 minute windows
user_engagement_df = spark.sql("""
    SELECT 
        window(timestamp, '5 minutes') as time_window,
        user_id,
        COUNT(*) as stream_count,
        SUM(stream_duration_seconds) as total_listen_time,
        AVG(stream_duration_seconds) as avg_listen_time,
        SUM(CASE WHEN is_skipped THEN 1 ELSE 0 END) / COUNT(*) as skip_rate,
        SUM(CASE WHEN is_liked THEN 1 ELSE 0 END) / COUNT(*) as like_rate,
        SUM(CASE WHEN is_shared THEN 1 ELSE 0 END) / COUNT(*) as share_rate,
        SUM(CASE WHEN is_added_to_playlist THEN 1 ELSE 0 END) / COUNT(*) as playlist_add_rate
    FROM stream_events
    GROUP BY window(timestamp, '5 minutes'), user_id
""")

# 2. Content performance metrics - 5 minute windows
content_performance_df = spark.sql("""
    SELECT 
        window(timestamp, '5 minutes') as time_window,
        song_id,
        artist_id,
        COUNT(*) as stream_count,
        SUM(CASE WHEN is_complete_play THEN 1 ELSE 0 END) / COUNT(*) as completion_rate,
        SUM(CASE WHEN is_skipped THEN 1 ELSE 0 END) / COUNT(*) as skip_rate,
        SUM(CASE WHEN is_liked THEN 1 ELSE 0 END) / COUNT(*) as like_rate
    FROM stream_events
    GROUP BY window(timestamp, '5 minutes'), song_id, artist_id
""")

# 3. Device metrics - 5 minute windows
device_metrics_df = spark.sql("""
    SELECT 
        window(timestamp, '5 minutes') as time_window,
        device_id,
        COUNT(*) as stream_count,
        AVG(stream_duration_seconds) as avg_listen_time,
        SUM(CASE WHEN is_complete_play THEN 1 ELSE 0 END) / COUNT(*) as completion_rate
    FROM stream_events
    GROUP BY window(timestamp, '5 minutes'), device_id
""")

# 4. Location metrics - 5 minute windows
location_metrics_df = spark.sql("""
    SELECT 
        window(timestamp, '5 minutes') as time_window,
        location_id,
        COUNT(*) as stream_count,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT song_id) as unique_songs
    FROM stream_events
    GROUP BY window(timestamp, '5 minutes'), location_id
""")

# 5. Hourly metrics - 1 hour windows
hourly_metrics_df = spark.sql("""
    SELECT 
        window(timestamp, '1 hour') as time_window,
        COUNT(*) as stream_count,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT song_id) as unique_songs,
        COUNT(DISTINCT artist_id) as unique_artists,
        SUM(stream_duration_seconds) as total_listen_time,
        AVG(stream_duration_seconds) as avg_listen_time,
        SUM(CASE WHEN is_skipped THEN 1 ELSE 0 END) / COUNT(*) as skip_rate,
        SUM(CASE WHEN is_liked THEN 1 ELSE 0 END) / COUNT(*) as like_rate
    FROM stream_events
    GROUP BY window(timestamp, '1 hour')
""")

# Function to handle time_window column for JDBC
def transform_time_window(df):
    return df.withColumnRenamed("time_window", "window_struct") \
             .withColumn("time_window", col("window_struct.start")) \
             .drop("window_struct")

# Write user engagement metrics to Postgres in real-time
user_engagement_query = user_engagement_df \
    .transform(transform_time_window) \
    .writeStream \
    .foreachBatch(lambda df, epoch_id: df.write \
        .jdbc(
            url=postgres_properties["url"],
            table="user_engagement_metrics",
            mode="append",
            properties={
                "user": postgres_properties["user"],
                "password": postgres_properties["password"],
                "driver": postgres_properties["driver"]
            }
        )
    ) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/user_engagement") \
    .start()

# Write content performance metrics to Postgres in real-time
content_performance_query = content_performance_df \
    .transform(transform_time_window) \
    .writeStream \
    .foreachBatch(lambda df, epoch_id: df.write \
        .jdbc(
            url=postgres_properties["url"],
            table="content_performance_metrics",
            mode="append",
            properties={
                "user": postgres_properties["user"],
                "password": postgres_properties["password"],
                "driver": postgres_properties["driver"]
            }
        )
    ) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/content_performance") \
    .start()

# Write device metrics to Postgres in real-time
device_metrics_query = device_metrics_df \
    .transform(transform_time_window) \
    .writeStream \
    .foreachBatch(lambda df, epoch_id: df.write \
        .jdbc(
            url=postgres_properties["url"],
            table="device_metrics",
            mode="append",
            properties={
                "user": postgres_properties["user"],
                "password": postgres_properties["password"],
                "driver": postgres_properties["driver"]
            }
        )
    ) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/device_metrics") \
    .start()

# Write location metrics to Postgres in real-time
location_metrics_query = location_metrics_df \
    .transform(transform_time_window) \
    .writeStream \
    .foreachBatch(lambda df, epoch_id: df.write \
        .jdbc(
            url=postgres_properties["url"],
            table="location_metrics",
            mode="append",
            properties={
                "user": postgres_properties["user"],
                "password": postgres_properties["password"],
                "driver": postgres_properties["driver"]
            }
        )
    ) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/location_metrics") \
    .start()

# Write hourly metrics to Postgres in real-time
hourly_metrics_query = hourly_metrics_df \
    .transform(transform_time_window) \
    .writeStream \
    .foreachBatch(lambda df, epoch_id: df.write \
        .jdbc(
            url=postgres_properties["url"],
            table="hourly_metrics",
            mode="append",
            properties={
                "user": postgres_properties["user"],
                "password": postgres_properties["password"],
                "driver": postgres_properties["driver"]
            }
        )
    ) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/hourly_metrics") \
    .start()

# Write raw events to Postgres in real-time
raw_events_query = parsed_stream_df \
    .writeStream \
    .foreachBatch(lambda df, epoch_id: df.write \
        .jdbc(
            url=postgres_properties["url"],
            table="stream_events",
            mode="append",
            properties={
                "user": postgres_properties["user"],
                "password": postgres_properties["password"],
                "driver": postgres_properties["driver"]
            }
        )
    ) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/raw_events") \
    .start()

# Print the streaming queries info
print("Active streaming queries:")
for query in spark.streams.active:
    print(f"Query ID: {query.id}, Name: {query.name}")

# Wait for all queries to terminate
spark.streams.awaitAnyTermination()
