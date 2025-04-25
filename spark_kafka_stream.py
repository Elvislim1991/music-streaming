from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkStructuredStreamingTest") \
    .getOrCreate()

# Read streaming data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert the binary 'value' column to string
df_parsed = df.selectExpr("CAST(value AS STRING) as message")

# Output the streaming data to the console
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
