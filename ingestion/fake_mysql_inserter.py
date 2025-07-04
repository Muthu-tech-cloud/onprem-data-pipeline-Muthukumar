import pymysql
import time
from faker import Faker
from config.db_config import Config

class FakeMySQLInserter:
    def __init__(self):
        self.conn = pymysql.connect(**Config.MYSQL_CONFIG)
        self.cursor = self.conn.cursor()
        self.fake = Faker()
        self.create_table()

    def create_table(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS sensor_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                City VARCHAR(255),
                SensorValue FLOAT
            )
        """)

    def insert_fake_data(self):
        for _ in range(5):
            city = self.fake.city()
            value = round(self.fake.pyfloat(min_value=25, max_value=60), 2)
            self.cursor.execute("INSERT INTO sensor_table (City, SensorValue) VALUES (%s, %s)", (city, value))
        self.conn.commit()
        print("[MySQL] Inserted fake sensor data.")

if __name__ == "__main__":
    inserter = FakeMySQLInserter()
    while True:
        inserter.insert_fake_data()
        time.sleep(60)
