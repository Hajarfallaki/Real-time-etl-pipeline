import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

TRANSACTION_TYPES = ["PAYMENT", "TRANSFER", "WITHDRAWAL", "DEPOSIT"]
STATUSES = ["SUCCESS", "FAILED"]


def generate_transaction(transaction_number):
    return {
        "transaction_id": f"TX{transaction_number:04d}",
        "customer_id": f"C{random.randint(1, 100):03d}",
        "amount": round(random.uniform(10, 2000), 2),
        "type": random.choice(TRANSACTION_TYPES),
        "status": random.choice(STATUSES),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def send_transactions():
    transaction_number = 1
    try:
        while True:
            transaction = generate_transaction(transaction_number)
            producer.send("transactions", value=transaction)
            producer.flush()
            print(f"Transaction envoyée : {transaction}")
            transaction_number += 1
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nProducer arrêté.")
    finally:
        producer.close()


if __name__ == "__main__":
    send_transactions()