import os
import joblib
import pandas as pd
from datetime import datetime, timezone
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from feast import FeatureStore
from prometheus_client import Counter
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI(
    title="Real-Time Fraud Detection API",
    description="Serves low-latency fraud probability scores by joining streaming Feast online features with model inference.",
    version="1.0.0"
)

FRAUD_PREDICTIONS_COUNTER = Counter(
    "fraud_predictions_total",
    "Total count of fraud predictions evaluated by the model",
    ["is_fraud"]
)

Instrumentator().instrument(app).expose(app, endpoint="/metrics")

@app.post("/predict")
async def predict_fraud(payload: dict):
    # ... Feature store retrieval and model inference logic ...
    is_fraud_prediction = True  # Output from XGBoost model (example)
    
    # Increment custom Prometheus metric
    FRAUD_PREDICTIONS_COUNTER.labels(is_fraud=str(is_fraud_prediction)).inc()
    
    return {"user_id": payload.get("user_id"), "is_fraud": is_fraud_prediction}

# Global variables for model and Feast store
MODEL_PATH = "models/fraud_model.joblib"
FEAST_REPO_PATH = os.path.abspath("feature_repository")

model = None
store = None

@app.on_event("startup")
def load_artifacts():
    """Loads the ML model and initializes the Feast FeatureStore on API startup."""
    global model, store
    
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(f"Model file not found at {MODEL_PATH}. Train the model first.")
        
    print(f"[*] Loading trained fraud model from {MODEL_PATH}...")
    model = joblib.load(MODEL_PATH)
    
    print(f"[*] Initializing Feast FeatureStore from {FEAST_REPO_PATH}...")
    try:
        store = FeatureStore(repo_path=FEAST_REPO_PATH)
        print("[+] Feast FeatureStore initialized successfully.")
    except Exception as e:
        print(f"[!] Warning: Feast FeatureStore failed to initialize: {e}")
        store = None

class TransactionRequest(BaseModel):
    user_id: str = Field(..., example="user_1034")
    current_tx_amount: float = Field(..., gt=0.0, example=1350.50)
    timestamp: Optional[str] = Field(default=None, example="2026-09-24T21:00:00Z")

class PredictionResponse(BaseModel):
    user_id: str
    current_tx_amount: float
    fraud_probability: float
    is_fraud: bool
    features_used: dict
    timestamp: str

@app.get("/health")
def health_check():
    """Health check endpoint to verify API and model readiness."""
    return {
        "status": "healthy",
        "model_loaded": model is not None,
        "feast_store_ready": store is not None
    }

@app.post("/predict", response_model=PredictionResponse)
def predict_fraud(transaction: TransactionRequest):
    """Real-time fraud prediction endpoint."""
    if model is None:
        raise HTTPException(status_code=500, detail="Model artifact is not loaded.")
        
    user_id = transaction.user_id
    current_tx_amount = transaction.current_tx_amount
    event_time = transaction.timestamp or datetime.now(timezone.utc).isoformat()
    
    # Fetch real-time features from Feast Online Store (or fallback to defaults)
    tx_count_10m = 0
    total_amount_10m = 0.0
    
    if store is not None:
        try:
            response = store.get_online_features(
                features=[
                    "user_transaction_feature_view:tx_count_10m",
                    "user_transaction_feature_view:total_amount_10m"
                ],
                entity_rows=[{"user_id": user_id}]
            ).to_dict()
            
            # Extract feature values safely
            fetched_count = response.get("tx_count_10m", [None])[0]
            fetched_amount = response.get("total_amount_10m", [None])[0]
            
            if fetched_count is not None:
                tx_count_10m = int(fetched_count)
            if fetched_amount is not None:
                total_amount_10m = float(fetched_amount)
                
        except Exception as e:
            print(f"[!] Feast online lookup failed for {user_id}, falling back to defaults: {e}")

    # Build feature DataFrame matching model expected columns
    features_df = pd.DataFrame([{
        "current_tx_amount": current_tx_amount,
        "tx_count_10m": tx_count_10m,
        "total_amount_10m": total_amount_10m
    }])
    
    # Execute Model Inference
    fraud_prob = float(model.predict_proba(features_df)[0][1])
    is_fraud = fraud_prob >= 0.50  # 50% risk threshold
    
    return PredictionResponse(
        user_id=user_id,
        current_tx_amount=current_tx_amount,
        fraud_probability=round(fraud_prob, 4),
        is_fraud=is_fraud,
        features_used={
            "tx_count_10m": tx_count_10m,
            "total_amount_10m": total_amount_10m
        },
        timestamp=event_time
    )