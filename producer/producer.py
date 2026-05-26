import json
import time
from kafka import KafkaProducer
from generator import generate_heartbeat

KAFKA_BROKER = "localhost:9092"
TOPIC = "heartbeats"
INTERVAL_SECONDS = 1

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8")
    )

def run():
    producer = create_producer()
    print(f"Producer started. Sending to topic '{TOPIC}' every {INTERVAL_SECONDS}s...")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            record = generate_heartbeat()
            producer.send(
                TOPIC,
                key=record["customer_id"],
                value=record
            )
            print(f"Sent → customer: {record['customer_id']} | "
                  f"heart_rate: {record['heart_rate']} bpm | "
                  f"time: {record['timestamp']}")
            time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed.")

if __name__ == "__main__":
    run()