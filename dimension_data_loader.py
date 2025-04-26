from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DateType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DimensionDataLoader") \
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

# Define schemas for dimension data
users_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("username", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("subscription_type", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("country", StringType(), True)
])

artists_schema = StructType([
    StructField("artist_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("is_band", BooleanType(), True)
])

albums_schema = StructType([
    StructField("album_id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("artist_id", IntegerType(), True),
    StructField("release_date", StringType(), True),
    StructField("total_tracks", IntegerType(), True),
    StructField("album_type", StringType(), True)
])

songs_schema = StructType([
    StructField("song_id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("duration_seconds", IntegerType(), True),
    StructField("release_date", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("language", StringType(), True),
    StructField("explicit", BooleanType(), True)
])

devices_schema = StructType([
    StructField("device_id", IntegerType(), True),
    StructField("device_type", StringType(), True),
    StructField("os", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("app_version", StringType(), True)
])

locations_schema = StructType([
    StructField("location_id", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
    StructField("city", StringType(), True),
    StructField("timezone", StringType(), True)
])

# Define a function to load dimension data from Kafka to PostgreSQL
def load_dimension_data(topic, schema, table):
    print(f"Loading {table} data from Kafka topic '{topic}'...")
    
    # Read from Kafka
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    # Parse JSON data
    parsed_df = df \
        .selectExpr("CAST(value AS STRING) as json_data") \
        .select(from_json("json_data", schema).alias("data")) \
        .select("data.*")
    
    # Convert date strings to proper date format if needed
    if "registration_date" in parsed_df.columns:
        parsed_df = parsed_df.withColumn("registration_date", col("registration_date").cast("date"))
    if "release_date" in parsed_df.columns:
        parsed_df = parsed_df.withColumn("release_date", col("release_date").cast("date"))
    
    # Show sample data
    print(f"Sample {table} data:")
    parsed_df.show(5, truncate=False)
    
    # Count records
    count = parsed_df.count()
    print(f"Found {count} {table} records")
    
    if count > 0:
        try:
            # Write to PostgreSQL
            parsed_df.write \
                .jdbc(
                    url=postgres_properties["url"],
                    table=table,
                    mode="overwrite",
                    properties={
                        "user": postgres_properties["user"],
                        "password": postgres_properties["password"],
                        "driver": postgres_properties["driver"]
                    }
                )
            print(f"Successfully loaded {count} {table} records to PostgreSQL")
        except Exception as e:
            print(f"Error writing {table} data to PostgreSQL: {e}")
    else:
        print(f"No {table} data found in Kafka topic '{topic}'")

# Load all dimension data
dimension_data = [
    ("users", users_schema, "users"),
    ("artists", artists_schema, "artists"),
    ("albums", albums_schema, "albums"),
    ("songs", songs_schema, "songs"),
    ("devices", devices_schema, "devices"),
    ("locations", locations_schema, "locations")
]

for topic, schema, table in dimension_data:
    load_dimension_data(topic, schema, table)

print("Dimension data loading complete")