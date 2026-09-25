import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime, timezone

KAFKA_TOPIC = "financial_transactions"
KAFKA_SERVER = "localhost:9092"
API_URL = "http://localhost:8000/predict"

def verify_live_pipeline():
    print("==================================================================")
    print("      MLOps Integration Test: Live Closed-Loop Verification")
    print("==================================================================\n")

    # Initializing the  Kafka Producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("[+] Kafka Producer connected to Redpanda.")
    except Exception as e:
        print(f"[!] Failed to connect to Redpanda: {e}")
        return

    target_user = "user_e2e_test"

    # Baseline API Check (Zero active streaming history)
    print(f"\n[*] Checking baseline API prediction for {target_user}...")
    res = requests.post(API_URL, json={"user_id": target_user, "current_tx_amount": 150.0}).json()
    print(f"    Baseline Response: Fraud Prob = {res['fraud_probability']} | Features = {res['features_used']}")

    # Simulate High-Velocity Attack Stream (10 Rapid High-Value Transactions)
    print(f"\n[*] Simulating real-time transaction burst for {target_user} (10 txs @ $850 each)...")
    for i in range(10):
        tx_payload = {
            "transaction_id": f"tx_e2e_{i}",
            "user_id": target_user,
            "amount": 850.0,
            "merchant_category": "electronics",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        producer.send(KAFKA_TOPIC, tx_payload)
        time.sleep(0.2)
    producer.flush()
    print("[+] All 10 micro-batch events emitted to Redpanda.")

    # Poll API to observe live Feast online feature update and probability surge
    print("\n[*] Polling FastAPI /predict endpoint every 2s for live feature store sync...")
    for poll in range(1, 8):
        time.sleep(2)
        res = requests.post(API_URL, json={"user_id": target_user, "current_tx_amount": 1200.0}).json()
        
        tx_count = res["features_used"]["tx_count_10m"]
        total_amt = res["features_used"]["total_amount_10m"]
        fraud_prob = res["fraud_probability"]
        is_fraud = res["is_fraud"]

        print(f"    Poll #{poll} | Features: count_10m={tx_count}, sum_10m=${total_amt:.2f} | Fraud Prob: {fraud_prob} | Flagged: {is_fraud}")

        if is_fraud:
            print("\n[SUCCESS] Pipeline verified! Live stream aggregates pushed to Feast and triggered model threshold.")
            break

if __name__ == "__main__":
    verify_live_pipeline()
