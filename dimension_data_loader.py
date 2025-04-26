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
# This function handles the complexity of loading dimension data while preserving foreign key constraints
# It uses different strategies based on the table and its dependencies:
# 1. For the "users" table (which has foreign key constraints from other tables):
#    - Identifies which users are new and only appends those
# 2. For other dimension tables:
#    - Checks for duplicates before appending to avoid duplicate records
# 3. Includes error handling that falls back to simple append mode if the optimized approach fails
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
            # First, try to truncate the table (which preserves the schema and constraints)
            # but only if it's safe to do so (i.e., for dimension tables without dependencies)
            if table in ["users", "artists", "albums", "songs", "devices", "locations"]:
                try:
                    # Get the primary key column (assuming it's table_name + "_id")
                    pk_column = f"{table[:-1]}_id" if table.endswith('s') else f"{table}_id"

                    # Create a temporary view of the data
                    parsed_df.createOrReplaceTempView(f"{table}_temp_view")

                    # For each table, use a different approach based on its dependencies
                    if table == "users":
                        # For users table, which has dependencies, use a more careful approach
                        # First, identify which user_ids are in the new data
                        user_ids = [row.user_id for row in parsed_df.select("user_id").collect()]

                        # If there are too many user_ids, just use append mode
                        if len(user_ids) > 100:
                            parsed_df.write \
                                .jdbc(
                                    url=postgres_properties["url"],
                                    table=table,
                                    mode="append",
                                    properties={
                                        "user": postgres_properties["user"],
                                        "password": postgres_properties["password"],
                                        "driver": postgres_properties["driver"]
                                    }
                                )
                        else:
                            # For a smaller number of users, we can be more precise
                            # First, check which user_ids already exist
                            existing_users_df = spark.read \
                                .jdbc(
                                    url=postgres_properties["url"],
                                    table=table,
                                    properties={
                                        "user": postgres_properties["user"],
                                        "password": postgres_properties["password"],
                                        "driver": postgres_properties["driver"]
                                    }
                                )
                            existing_users_df.createOrReplaceTempView("existing_users")

                            # Filter out users that already exist
                            new_users_df = spark.sql(f"""
                                SELECT t.* FROM {table}_temp_view t
                                LEFT JOIN existing_users e ON t.user_id = e.user_id
                                WHERE e.user_id IS NULL
                            """)

                            # Append only the new users
                            if new_users_df.count() > 0:
                                new_users_df.write \
                                    .jdbc(
                                        url=postgres_properties["url"],
                                        table=table,
                                        mode="append",
                                        properties={
                                            "user": postgres_properties["user"],
                                            "password": postgres_properties["password"],
                                            "driver": postgres_properties["driver"]
                                        }
                                    )
                    else:
                        # For other dimension tables, check for duplicates before appending
                        try:
                            # Read existing records
                            existing_df = spark.read \
                                .jdbc(
                                    url=postgres_properties["url"],
                                    table=table,
                                    properties={
                                        "user": postgres_properties["user"],
                                        "password": postgres_properties["password"],
                                        "driver": postgres_properties["driver"]
                                    }
                                )

                            # Create a temporary view of existing data
                            existing_df.createOrReplaceTempView(f"existing_{table}")

                            # Filter out records that already exist
                            new_records_df = spark.sql(f"""
                                SELECT t.* FROM {table}_temp_view t
                                LEFT JOIN existing_{table} e ON t.{pk_column} = e.{pk_column}
                                WHERE e.{pk_column} IS NULL
                            """)

                            # Append only the new records
                            new_count = new_records_df.count()
                            if new_count > 0:
                                print(f"Found {new_count} new records for {table}")
                                new_records_df.write \
                                    .jdbc(
                                        url=postgres_properties["url"],
                                        table=table,
                                        mode="append",
                                        properties={
                                            "user": postgres_properties["user"],
                                            "password": postgres_properties["password"],
                                            "driver": postgres_properties["driver"]
                                        }
                                    )
                            else:
                                print(f"No new records to add for {table}")
                        except Exception as e:
                            print(f"Error checking for duplicates in {table}, falling back to append mode: {e}")
                            # Fall back to simple append mode
                            parsed_df.write \
                                .jdbc(
                                    url=postgres_properties["url"],
                                    table=table,
                                    mode="append",
                                    properties={
                                        "user": postgres_properties["user"],
                                        "password": postgres_properties["password"],
                                        "driver": postgres_properties["driver"]
                                    }
                                )
                except Exception as e:
                    print(f"Error with optimized approach for {table}, falling back to append mode: {e}")
                    # Fall back to simple append mode
                    parsed_df.write \
                        .jdbc(
                            url=postgres_properties["url"],
                            table=table,
                            mode="append",
                            properties={
                                "user": postgres_properties["user"],
                                "password": postgres_properties["password"],
                                "driver": postgres_properties["driver"]
                            }
                        )
            else:
                # For non-dimension tables, just use append mode
                parsed_df.write \
                    .jdbc(
                        url=postgres_properties["url"],
                        table=table,
                        mode="append",
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
