import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from feast import FeatureStore

def setup_offline_data():
    """Generates synthetic historical feature records for Feast's FileSource."""
    os.makedirs("feature_repository/data", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    
    print("[*] Generating historical batch feature records...")
    now = datetime.now(timezone.utc)
    
    # Generate historical aggregated feature records for 50 users over the past 7 days
    data = []
    for user_num in range(1001, 1051):
        user_id = f"user_{user_num}"
        for day in range(7):
            for hour in range(0, 24, 4):
                ts = now - timedelta(days=day, hours=hour)
                tx_count = np.random.randint(1, 15)
                total_amount = float(np.random.uniform(20.0, 1500.0))
                data.append({
                    "user_id": user_id,
                    "datetime": ts,
                    "tx_count_10m": tx_count,
                    "total_amount_10m": total_amount
                })
                
    df_features = pd.DataFrame(data)
    parquet_path = os.path.abspath("feature_repository/data/transaction_features.parquet")
    df_features.to_parquet(parquet_path)
    print(f"[+] Saved {len(df_features)} historical feature rows to {parquet_path}")

def generate_training_matrix():
    """Point-in-time joins entity transactions with Feast historical features."""
    repo_path = os.path.abspath("feature_repository")
    store = FeatureStore(repo_path=repo_path)
    
    print("[*] Creating synthetic transaction observations (Entity DataFrame)...")
    now = datetime.now(timezone.utc)
    
    # Create 500 ground-truth transaction observation records with fraud labels
    observations = []
    for _ in range(500):
        user_num = np.random.randint(1001, 1051)
        user_id = f"user_{user_num}"
        ts = now - timedelta(days=np.random.randint(0, 6), hours=np.random.randint(0, 23))
        
        # Synthetic fraud logic for ground truth label
        tx_amount = float(np.random.uniform(5.0, 2000.0))
        is_fraud = 1 if (tx_amount > 1200.0 and np.random.rand() > 0.3) else 0
        
        observations.append({
            "user_id": user_id,
            "timestamp": ts,
            "current_tx_amount": tx_amount,
            "is_fraud": is_fraud
        })
        
    entity_df = pd.DataFrame(observations)
    
    print("[*] Calling Feast get_historical_features() for point-in-time join...")
    features_to_fetch = [
        "user_transaction_feature_view:tx_count_10m",
        "user_transaction_feature_view:total_amount_10m"
    ]
    
    training_data = store.get_historical_features(
        entity_df=entity_df,
        features=features_to_fetch
    ).to_df()
    
    # Fill any null historical features with default 0 values
    training_data["tx_count_10m"] = training_data["tx_count_10m"].fillna(0).astype(int)
    training_data["total_amount_10m"] = training_data["total_amount_10m"].fillna(0.0).astype(float)
    
    output_csv = "data/train_dataset.csv"
    training_data.to_csv(output_csv, index=False)
    
    print(f"\n[+] Successfully generated training dataset at {output_csv}!")
    print(f"[*] Total rows: {len(training_data)} | Fraud cases: {training_data['is_fraud'].sum()}")
    print("\nSample training records:")
    print(training_data[["user_id", "timestamp", "current_tx_amount", "tx_count_10m", "total_amount_10m", "is_fraud"]].head())

if __name__ == "__main__":
    setup_offline_data()
    generate_training_matrix()