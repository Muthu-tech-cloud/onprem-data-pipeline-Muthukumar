from pyspark.sql import SparkSession
import os

# File and MySQL configurations
csv_file_path = "/opt/airflow/faker_csv/user_weather.csv"
mysql_url = "jdbc:mysql://mysql:3306/weather_db"
mysql_table = "user_weather"
mysql_user = os.getenv("MYSQL_ROOT_USER", "root")
mysql_password = os.getenv("MYSQL_ROOT_PASSWORD", "root")

# Spark session with MySQL connector
spark = SparkSession.builder \
    .appName("BatchETL") \
    .config("spark.jars", "/opt/airflow/jars/mysql-connector-j-9.3.0.jar") \
    .getOrCreate()

# Read from CSV
csv_df = spark.read.option("header", True).csv(csv_file_path)
csv_df.show()

# Read from MySQL
mysql_df = spark.read \
    .format("jdbc") \
    .option("url", mysql_url) \
    .option("dbtable", mysql_table) \
    .option("user", mysql_user) \
    .option("password", mysql_password) \
    .load()
mysql_df.show()

# Join or union (example)
combined_df = csv_df.unionByName(mysql_df, allowMissingColumns=True)
combined_df.show()

# Save result
combined_df.write.mode("overwrite").parquet("/opt/airflow/data/batch_output/")

print("✅ Batch ETL completed and saved to /opt/airflow/data/batch_output/")
