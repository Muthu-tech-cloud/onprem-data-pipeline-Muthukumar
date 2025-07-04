from faker import Faker
import pandas as pd
import random
import os

def generate_csv():
    fake = Faker()
    rows = []

    for _ in range(100):
        rows.append({
            "user_id": fake.uuid4(),
            "name": fake.name(),
            "city": fake.city(),
            "temperature": round(random.uniform(20.0, 40.0), 2),
            "humidity": random.randint(40, 90),
            "timestamp": fake.iso8601()
        })

    # Ensure directory exists
    output_dir = "/opt/airflow/faker_csv"
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, "user_weather.csv")

    df = pd.DataFrame(rows)
    df.to_csv(file_path, index=False)
    print(f"user_weather.csv generated at: {file_path}")

# Optional standalone run
if __name__ == "__main__":
    generate_csv()
