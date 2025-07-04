from faker import Faker
import pandas as pd
import random

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

df = pd.DataFrame(rows)
df.to_csv("faker_csv/user_weather.csv", index=False)
print("✅ user_weather.csv generated.")
