import json
import random
import time
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer

# Configuration for Redpanda running locally
KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "transaction-producer-sim",
}

TOPIC_NAME = "transactions"

# High-risk merchant categories and locations for transaction simulation
CATEGORIES = ["electronics", "groceries", "travel", "gaming", "luxury_goods", "transfers"]
LOCATIONS = ["Lagos", "London", "New York", "Tokyo", "Berlin", "Toronto"]
USER_POOL = [f"usr_{i:04d}" for i in range(1, 101)]  # 100 simulated users


def delivery_report(err, msg):
    """Callback triggered on successful or failed message delivery."""
    if err is not None:
        print(f"[-] Delivery failed for record {msg.key()}: {err}")
    else:
        print(
            f"[+] Produced -> Topic: {msg.topic()} | Partition: [{msg.partition()}] | Offset: {msg.offset()}"
        )


def generate_transaction():
    """Generates a realistic transaction payload."""
    user_id = random.choice(USER_POOL)
    
    # 5% chance to simulate an anomaly/fraudulent amount spikes
    is_anomaly = random.random() < 0.05
    amount = round(random.uniform(500.0, 5000.0) if is_anomaly else random.uniform(5.0, 300.0), 2)

    return {
        "transaction_id": f"tx_{uuid.uuid4().hex[:10]}",
        "user_id": user_id,
        "amount": amount,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "location": random.choice(LOCATIONS),
        "merchant_category": random.choice(CATEGORIES),
    }


def run_producer():
    producer = Producer(KAFKA_CONFIG)
    print(f"[*] Starting streaming transaction producer to topic '{TOPIC_NAME}'...")
    print("[*] Press Ctrl+C to stop.\n")

    try:
        while True:
            payload = generate_transaction()
            payload_bytes = json.dumps(payload).encode("utf-8")

            # Produce record keyed by user_id to ensure partition ordering per user
            producer.produce(
                topic=TOPIC_NAME,
                key=payload["user_id"].encode("utf-8"),
                value=payload_bytes,
                on_delivery=delivery_report,
            )

            # Serve delivery callbacks from previous asynchronous produces
            producer.poll(0)

            print(f"Pushed: {payload['user_id']} | ${payload['amount']} | {payload['merchant_category']}")
            
            # Emit 1 to 3 records per second
            time.sleep(random.uniform(0.3, 1.0))

    except KeyboardInterrupt:
        print("\n[*] Stopping transaction producer...")
    finally:
        print("[*] Flushing remaining records...")
        producer.flush(timeout=5)
        print("[+] Producer shutdown cleanly.")


if __name__ == "__main__":
    run_producer()
