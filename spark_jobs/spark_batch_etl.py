from pyspark.sql import SparkSession

class SparkBatchETL:
    def __init__(self):
        self.spark = SparkSession.builder.appName("BatchETL").enableHiveSupport().getOrCreate()

    def run_etl(self):
        csv_df = self.spark.read.csv("output/fake_weather.csv", header=True)
        mysql_df = self.spark.read.format("jdbc").options(
            url="jdbc:mysql://localhost:3306/sensordb",
            driver="com.mysql.cj.jdbc.Driver",
            dbtable="sensor_table",
            user="root",
            password="root"
        ).load()

        final_df = csv_df.join(mysql_df, "City").select("Name", "City", "Temperature", "SensorValue")

        final_df.write.mode("overwrite").saveAsTable("final_table")

        final_df.write.mode("overwrite").format("jdbc").options(
            url="jdbc:mysql://localhost:3306/sensordb",
            driver="com.mysql.cj.jdbc.Driver",
            dbtable="final_table",
            user="root",
            password="root"
        ).save()

        final_df.write.csv("output/final_csv", header=True)
        print("[Spark] Batch ETL completed.")

if __name__ == "__main__":
    SparkBatchETL().run_etl()
