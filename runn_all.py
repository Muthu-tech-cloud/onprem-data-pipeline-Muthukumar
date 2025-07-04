from ingestion.weather_producer import WeatherKafkaProducer
from ingestion.fake_csv_generator import FakeCSVGenerator
from ingestion.fake_mysql_inserter import FakeMySQLInserter

if __name__ == "__main__":
    WeatherKafkaProducer().push_to_kafka()
    FakeCSVGenerator().generate()
    FakeMySQLInserter().insert_fake_data()
    print("All ingestion tasks completed.")