import random
import json
from datetime import datetime, timezone
from faker import Faker

fake = Faker()

CUSTOMER_IDS = [f"C{str(i).zfill(3)}" for i in range(1, 21)]

def generate_heartbeat():
    customer_id = random.choice(CUSTOMER_IDS)
    heart_rate = random.randint(40, 200)
    timestamp = datetime.now(timezone.utc).isoformat()

    return {
        "customer_id": customer_id,
        "timestamp": timestamp,
        "heart_rate": heart_rate,
        "name": fake.name()
    }

if __name__ == "__main__":
    for _ in range(5):
        record = generate_heartbeat()
        print(json.dumps(record, indent=2))