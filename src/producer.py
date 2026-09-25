import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "financial_transactions"

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def generate_synthetic_transaction():
    user_ids = [f"user_{i}" for i in range(1001, 1050)]
    merchant_categories = ["grocery", "electronics", "travel", "dining", "crypto_exchange"]
    
    user_id = random.choice(user_ids)
    amount = round(random.uniform(5.0, 300.0), 2)
    
    if random.random() < 0.02:
        amount = round(random.uniform(2500.0, 10000.0), 2)

    return {
        "transaction_id": f"tx_{int(time.time() * 1000)}",
        "user_id": user_id,
        "amount": amount,
        "merchant_category": random.choice(merchant_categories),
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }

def start_producer():
    print(f"[*] Connecting to Kafka broker at {KAFKA_BROKER}...")

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=json_serializer,
        api_version=(2, 5, 0)
    )
    
    print(f"[+] Successfully connected! Publishing stream to topic: '{TOPIC_NAME}'...")

    try:
        while True:
            event = generate_synthetic_transaction()
            producer.send(TOPIC_NAME, value=event)
            print(f"[STREAM] Published: User={event['user_id']} | Amount=${event['amount']} | Time={event['timestamp']}")
            time.sleep(0.2)
    except KeyboardInterrupt:
        print("\n[*] Stopping producer...")
        producer.close()

if __name__ == "__main__":
    start_producer()