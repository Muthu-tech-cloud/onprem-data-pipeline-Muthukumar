import os
import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load from the mounted .env file
load_dotenv('/opt/airflow/.env')
print("[DEBUG] KAFKA_SERVER =", os.getenv('KAFKA_SERVER'))

class WeatherKafkaProducer:
    def __init__(self):
        self.api_key = os.getenv('WEATHER_API_KEY')
        self.city = os.getenv('CITY')
        self.kafka_topic = os.getenv('KAFKA_TOPIC')
        self.kafka_server = os.getenv('KAFKA_SERVER')

        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def fetch_weather(self):
        url = (
            f"http://api.openweathermap.org/data/2.5/weather"
            f"?q={self.city}&appid={self.api_key}"
        )
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def send_to_kafka(self, data):
        self.producer.send(self.kafka_topic, data)
        self.producer.flush()
        print(f"✔️ Sent data to Kafka topic '{self.kafka_topic}'")

    def run(self):
        print("📡 Fetching weather data...")
        data = self.fetch_weather()
        print("📦 Weather Data:", data)
        self.send_to_kafka(data)

if __name__ == "__main__":
    producer = WeatherKafkaProducer()
    producer.run()
