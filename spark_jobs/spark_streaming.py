import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, FloatType

# Load environment variables
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather-topic")

# Define the JSON schema for the Kafka message
weather_schema = StructType() \
    .add("city", StringType()) \
    .add("temperature", FloatType()) \
    .add("description", StringType()) \
    .add("timestamp", StringType())

# Create Spark session
spark = SparkSession.builder \
    .appName("Kafka-Spark-Streaming-Weather") \
    .getOrCreate()

# Read stream from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("kafka.request.timeout.ms", "30000") \
    .option("kafka.session.timeout.ms", "60000") \
    .option("kafka.retries", "5") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse the Kafka message value (which is in JSON)
parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), weather_schema).alias("data")) \
    .select("data.*")

# Write to Parquet every 5 minutes
query = parsed_df.writeStream \
    .format("parquet") \
    .option("path", "/opt/airflow/data/weather/") \
    .option("checkpointLocation", "/opt/airflow/data/checkpoints/") \
    .outputMode("append") \
    .trigger(processingTime="5 minutes") \
    .start()

# Await termination
query.awaitTermination()
