import os
import sqlite3
from feast import FeatureStore

# 1. Fetch active users directly from SQLite to guarantee a match
db_path = os.path.abspath("feature_repository/data/online.db")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get table name
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
tables = cursor.fetchall()

if not tables:
    print("[!] Online database is empty. Let PySpark run for 30 seconds while producer is active.")
    exit()

table_name = tables[0][0]

# Retrieve stored keys from SQLite
cursor.execute(f"SELECT entity_key FROM {table_name} LIMIT 10")
rows = cursor.fetchall()

# Feast serializes keys; let's query all 50 users to hit active ones
entity_rows = [{"user_id": f"user_{i}"} for i in range(1001, 1051)]

repo_path = os.path.abspath("feature_repository")
store = FeatureStore(repo_path=repo_path)

features_to_fetch = [
    "user_transaction_feature_view:tx_count_10m",
    "user_transaction_feature_view:total_amount_10m",
]

print("[*] Fetching online features from Feast...")

response = store.get_online_features(
    features=features_to_fetch,
    entity_rows=entity_rows,
)

online_data = response.to_dict()

print("\n[+] Online Feature Retrieval Results:")
users = online_data["user_id"]
counts = online_data["tx_count_10m"]
amounts = online_data["total_amount_10m"]

found = False
for i in range(len(users)):
    if counts[i] is not None:
        found = True
        print(f" User: {users[i]} | 10m Tx Count: {counts[i]} | 10m Total Amt: ${amounts[i]:.2f}")

if not found:
    print("[!] All queried user IDs returned None. Ensure streaming.py is actively pushing batches.")