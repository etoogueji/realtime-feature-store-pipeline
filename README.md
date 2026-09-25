# Real-Time Financial Fraud & Anomaly Detection Pipeline

A production-grade MLOps pipeline for low-latency streaming feature engineering, real-time fraud inference, and observability using PySpark, Redpanda, Feast, FastAPI, and Prometheus.

## Architecture

```text
[ Synthetic Generator ] ──► [ Redpanda (Kafka) ]
                                  │
                                  ▼
                     [ PySpark Structured Streaming ]
                                  │ (10m Tumbling Windows)
                                  ▼
                       [ Feast Online Store ]
                                  │
                                  ▼
 [ User Event ] ──► [ FastAPI Inference Service ] ──► [ Prometheus Metrics ]
                         (XGBoost Fraud Model)               │
                                                             ▼
                                                    [ Grafana Dashboard ]
