from pyspark.sql import SparkSession
import os

# File and MySQL configurations
csv_file_path = "/opt/airflow/faker_csv/user_weather.csv"
mysql_url = "jdbc:mysql://mysql:3306/weather_db"
mysql_table = "user_weather"
mysql_user = os.getenv("MYSQL_ROOT_USER", "root")
mysql_password = os.getenv("MYSQL_ROOT_PASSWORD", "root")

# Create Spark session with Hive and MySQL support
spark = SparkSession.builder \
    .appName("BatchETL") \
    .enableHiveSupport() \
    .config("spark.jars", "/opt/airflow/jars/mysql-connector-j-9.3.0.jar") \
    .getOrCreate()

# Read CSV file
csv_df = spark.read.option("header", True).csv(csv_file_path)

# Read MySQL table
mysql_df = spark.read \
    .format("jdbc") \
    .option("url", mysql_url) \
    .option("dbtable", mysql_table) \
    .option("user", mysql_user) \
    .option("password", mysql_password) \
    .load()

# Combine both
combined_df = csv_df.unionByName(mysql_df, allowMissingColumns=True)

#1. Save to Hive table
combined_df.write.mode("overwrite").saveAsTable("final_table")

#2. Save to Parquet (backup)
combined_df.write.mode("overwrite").parquet("/opt/airflow/data/batch_output/")

#3. Save to MySQL final_table
combined_df.write \
    .format("jdbc") \
    .option("url", mysql_url) \
    .option("dbtable", "final_table") \
    .option("user", mysql_user) \
    .option("password", mysql_password) \
    .mode("overwrite") \
    .save()

# 4. Export Hive table to CSV
spark.sql("SELECT * FROM final_table") \
    .coalesce(1) \
    .write \
    .option("header", True) \
    .mode("overwrite") \
    .csv("/opt/airflow/data/hive_export_csv/")

print("Batch ETL completed: Hive table, MySQL, Parquet, and CSV export done.")
