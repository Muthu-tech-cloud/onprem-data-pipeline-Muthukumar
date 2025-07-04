from faker import Faker
import csv, os, time

class FakeCSVGenerator:
    def __init__(self, path="output/fake_weather.csv"):
        self.path = path
        self.fake = Faker()

    def generate(self):
        os.makedirs("output", exist_ok=True)
        with open(self.path, mode="a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Name", "City", "Temperature"])
            for _ in range(5):
                writer.writerow([
                    self.fake.name(),
                    self.fake.city(),
                    round(self.fake.pyfloat(min_value=25, max_value=40), 2)
                ])
        print("[CSV] Fake weather data written.")

if __name__ == "__main__":
    generator = FakeCSVGenerator()
    while True:
        generator.generate()
        time.sleep(60)
