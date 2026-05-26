import json
import psycopg2
from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
TOPIC = "heartbeats"
GROUP_ID = "heartbeat-consumer-group"

DB_CONFIG = {
    "host": "localhost",
    "port": 5434,
    "dbname": "heartbeat_db",
    "user": "admin",
    "password": "admin123"
}

HEART_RATE_MIN = 40
HEART_RATE_MAX = 200
ANOMALY_LOW = 50
ANOMALY_HIGH = 160


def classify(heart_rate):
    if heart_rate < ANOMALY_LOW or heart_rate > ANOMALY_HIGH:
        return "anomaly"
    return "normal"


def connect_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    return conn


def insert_record(cursor, record, status):
    cursor.execute("""
        INSERT INTO heartbeats (customer_id, name, heart_rate, status, timestamp)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        record["customer_id"],
        record.get("name", "Unknown"),
        record["heart_rate"],
        status,
        record["timestamp"]
    ))


def validate(record):
    if record.get("customer_id") is None:
        return False, "missing customer_id"
    if record.get("heart_rate") is None:
        return False, "missing heart_rate"
    if not (HEART_RATE_MIN <= record["heart_rate"] <= HEART_RATE_MAX):
        return False, f"heart_rate {record['heart_rate']} out of range"
    return True, "ok"


def run():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )

    conn = connect_db()
    cursor = conn.cursor()

    print(f"Consumer started. Listening to topic '{TOPIC}'...")
    print("Press Ctrl+C to stop.\n")

    try:
        for message in consumer:
            record = message.value
            valid, reason = validate(record)

            if not valid:
                print(f"SKIPPED  | {reason} | raw: {record}")
                continue

            status = classify(record["heart_rate"])
            insert_record(cursor, record, status)

            flag = "ANOMALY" if status == "anomaly" else "OK     "
            print(f"{flag} | customer: {record['customer_id']} | "
                  f"heart_rate: {record['heart_rate']} bpm | "
                  f"time: {record['timestamp']}")

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        cursor.close()
        conn.close()
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    run()