from pyspark.sql import SparkSession
import os
import time

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("LoadParquetToPostgres") \
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

# Paths to parquet data
parquet_base_path = "/tmp/music_streaming_analytics"
dimension_data_path = "/tmp/dimension_data"

# Function to load dimension data
def load_dimension_data():
    """Load dimension data from parquet files to Postgres"""
    dimension_tables = ["users", "artists", "albums", "songs", "devices", "locations"]
    
    for table in dimension_tables:
        try:
            path = f"{dimension_data_path}/{table}"
            if os.path.exists(path):
                print(f"Loading {table} dimension data...")
                df = spark.read.parquet(path)
                
                # Write to Postgres
                df.write \
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
                print(f"Successfully loaded {table} dimension data")
            else:
                print(f"Warning: {path} does not exist, skipping {table} dimension data")
        except Exception as e:
            print(f"Error loading {table} dimension data: {e}")

# Function to load metrics data
def load_metrics_data():
    """Load metrics data from parquet files to Postgres"""
    metrics_tables = {
        "user_engagement": "user_engagement_metrics",
        "content_performance": "content_performance_metrics",
        "device_metrics": "device_metrics",
        "location_metrics": "location_metrics",
        "hourly_metrics": "hourly_metrics"
    }
    
    for parquet_dir, table_name in metrics_tables.items():
        try:
            path = f"{parquet_base_path}/{parquet_dir}"
            if os.path.exists(path):
                print(f"Loading {parquet_dir} metrics data...")
                df = spark.read.parquet(path)
                
                # Handle the window column which is a struct in parquet but needs to be a timestamp in Postgres
                if "time_window" in df.columns:
                    df = df.withColumnRenamed("time_window", "window_struct")
                    df = df.withColumn("time_window", df["window_struct.start"])
                    df = df.drop("window_struct")
                
                # Write to Postgres
                df.write \
                    .jdbc(
                        url=postgres_properties["url"],
                        table=table_name,
                        mode="append",
                        properties={
                            "user": postgres_properties["user"],
                            "password": postgres_properties["password"],
                            "driver": postgres_properties["driver"]
                        }
                    )
                print(f"Successfully loaded {parquet_dir} metrics data")
            else:
                print(f"Warning: {path} does not exist, skipping {parquet_dir} metrics data")
        except Exception as e:
            print(f"Error loading {parquet_dir} metrics data: {e}")

# Function to load raw events data
def load_raw_events():
    """Load raw events data from parquet files to Postgres"""
    try:
        path = f"{parquet_base_path}/raw_events"
        if os.path.exists(path):
            print("Loading raw events data...")
            df = spark.read.parquet(path)
            
            # Write to Postgres
            df.write \
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
            print("Successfully loaded raw events data")
        else:
            print(f"Warning: {path} does not exist, skipping raw events data")
    except Exception as e:
        print(f"Error loading raw events data: {e}")

def main():
    """Main function to load all data with retry logic"""
    max_retries = 5
    retry_interval = 10  # seconds
    
    # First, try to load dimension data
    for attempt in range(1, max_retries + 1):
        print(f"Attempt {attempt}/{max_retries} to load dimension data")
        try:
            load_dimension_data()
            break
        except Exception as e:
            print(f"Error: {e}")
            if attempt < max_retries:
                print(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                print("Failed to load dimension data after maximum retries")
    
    # Then, load metrics data
    for attempt in range(1, max_retries + 1):
        print(f"Attempt {attempt}/{max_retries} to load metrics data")
        try:
            load_metrics_data()
            break
        except Exception as e:
            print(f"Error: {e}")
            if attempt < max_retries:
                print(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                print("Failed to load metrics data after maximum retries")
    
    # Finally, load raw events data
    for attempt in range(1, max_retries + 1):
        print(f"Attempt {attempt}/{max_retries} to load raw events data")
        try:
            load_raw_events()
            break
        except Exception as e:
            print(f"Error: {e}")
            if attempt < max_retries:
                print(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                print("Failed to load raw events data after maximum retries")
    
    print("Data loading process completed")

if __name__ == "__main__":
    main()