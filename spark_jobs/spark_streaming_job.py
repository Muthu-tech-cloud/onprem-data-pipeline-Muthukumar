from pyspark.sql import SparkSession

class SparkKafkaStreamer:
    def __init__(self):
        self.spark = SparkSession.builder.appName("KafkaStreamer").getOrCreate()

    def stream(self):
        df = self.spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "weather-topic").load()

        df.selectExpr("CAST(value AS STRING)").writeStream \
            .format("parquet") \
            .option("path", "output/parquet") \
            .option("checkpointLocation", "output/checkpoint") \
            .trigger(processingTime="5 minutes") \
            .start().awaitTermination()

if __name__ == "__main__":
    SparkKafkaStreamer().stream()
