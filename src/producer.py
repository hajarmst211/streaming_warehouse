import json
import os
import sys
import time
from dotenv import load_dotenv
from faker import Faker
from kafka import KafkaProducer
from datetime import datetime, timezone
import random

load_dotenv()
kafka_port = os.getenv("KAFKA_PORT", "9092")

fake = Faker()


def generate_transaction():
    is_fraudulent = random.random() < 0.05

    if is_fraudulent:
        amount = round(fake.pyfloat(min_value=10001.0, max_value=50000.0, right_digits=2), 2)
    else:
        amount = round(fake.pyfloat(min_value=1.0, max_value=1000.0, right_digits=2), 2)

    return {
        "transaction_id": f"tx_{fake.random_int(min=10000, max=99999)}",
        "user_id": f"usr_{fake.random_int(min=100, max=999)}",
        "amount": amount,
        "currency": fake.currency_code(),
        "merchant": fake.company(),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "location": fake.country_code(),
    }

def send_messages(count=10):
    producer = KafkaProducer(
        bootstrap_servers=[f"localhost:{kafka_port}"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    topic_name = "transactions_raw"

    print(f"Sending {count} messages to '{topic_name}'...")

    for i in range(1, count + 1):
        payload = generate_transaction()
        producer.send(topic_name, value=payload)
        print(f"[{i}/{count}] Sent: {payload['transaction_id']}")
        time.sleep(0.1)  

    producer.flush()
    producer.close()
    print("All messages sent successfully!")


if __name__ == "__main__":

    if len(sys.argv) > 1:
        num_messages = int(sys.argv[1])
    else:
        num_messages = 10  

    send_messages(num_messages)