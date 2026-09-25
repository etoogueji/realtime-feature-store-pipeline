import os
import joblib
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score, confusion_matrix

def train_fraud_model():
    dataset_path = "data/train_dataset.csv"
    
    if not os.path.exists(dataset_path):
        raise FileNotFoundError(f"Training dataset not found at {dataset_path}. Run generate_training_data.py first.")
        
    print(f"[*] Loading training dataset from {dataset_path}...")
    df = pd.read_csv(dataset_path)
    
    # Define feature set and target label
    feature_cols = ["current_tx_amount", "tx_count_10m", "total_amount_10m"]
    target_col = "is_fraud"
    
    X = df[feature_cols]
    y = df[target_col]
    
    # Train / Test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    print(f"[*] Training dataset size: {len(X_train)} | Test set size: {len(X_test)}")
    
    # Train Random Forest Classifier
    print("[*] Training Random Forest Fraud Model...")
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=6,
        random_state=42,
        class_weight="balanced"
    )
    model.fit(X_train, y_train)
    
    # Evaluate
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]
    
    auc_score = roc_auc_score(y_test, y_proba)
    
    print("\n================ MODEL EVALUATION ================")
    print(f"ROC-AUC Score: {auc_score:.4f}\n")
    print("Classification Report:")
    print(classification_report(y_test, y_pred))
    
    print("Confusion Matrix:")
    print(confusion_matrix(y_test, y_pred))
    print("==================================================\n")
    
    # Save Model Artifacts
    os.makedirs("models", exist_ok=True)
    model_path = "models/fraud_model.joblib"
    joblib.dump(model, model_path)
    print(f"[+] Model successfully saved to {model_path}!")

if __name__ == "__main__":
    train_fraud_model()