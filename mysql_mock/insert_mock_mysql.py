import mysql.connector
import random
import os

class WeatherDataInserter:
    def __init__(self):
        self.host = os.getenv("MYSQL_HOST", "mysql")
        self.user = os.getenv("MYSQL_ROOT_USER", "root")
        self.password = os.getenv("MYSQL_ROOT_PASSWORD", "root")
        self.database = "weather_db"

    def create_database(self):
        connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password
        )
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        print(f"✅ Database '{self.database}' ensured.")
        cursor.close()
        connection.close()

    def insert_fake_data(self):
        connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
        cursor = connection.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_weather (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50),
            city VARCHAR(50),
            temperature FLOAT
        )
        """)

        fake_names = ["Alice", "Bob", "Charlie", "David", "Eve"]
        cities = ["Chennai", "Delhi", "Mumbai", "Bangalore", "Kolkata"]

        for _ in range(5):
            name = random.choice(fake_names)
            city = random.choice(cities)
            temperature = round(random.uniform(20, 40), 2)

            cursor.execute(
                "INSERT INTO user_weather (name, city, temperature) VALUES (%s, %s, %s)",
                (name, city, temperature)
            )

        connection.commit()
        cursor.close()
        connection.close()
        print("✅ Inserted fake data into MySQL table: user_weather")

    def run(self):
        self.create_database()
        self.insert_fake_data()

# Run the class
if __name__ == "__main__":
    inserter = WeatherDataInserter()
    inserter.run()
