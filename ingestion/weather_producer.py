import requests, json, os
from kafka import KafkaProducer
from dotenv import load_dotenv
from time import sleep

load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY", "demo_key")
CITY = "Chennai"
KAFKA_TOPIC = "weather-topic"
BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_weather():
    url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}"
    data = requests.get(url).json()
    return data

while True:
    weather = fetch_weather()
    producer.send(KAFKA_TOPIC, weather)
    print("Sent weather data to Kafka.")
    sleep(5)
