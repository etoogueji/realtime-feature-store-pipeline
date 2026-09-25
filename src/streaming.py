import os
import sys
import pandas as pd
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from feast import FeatureStore

# 1. Initialize Feast Feature Store
repo_path = os.path.abspath("feature_repository")
store = FeatureStore(repo_path=repo_path)

# 2. Define Schema matching producer.py
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])

# 3. Initialize PySpark Session with Kafka package
spark = SparkSession.builder \
    .appName("RedpandaFeastStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.json4s:json4s-jackson_2.12:3.7.0-M11") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("[+] PySpark Session initialized.")

# 4. Stream from Redpanda / Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "financial_transactions") \
    .option("startingOffsets", "latest") \
    .load()

# 5. Parse JSON & Create Time Window
parsed_df = raw_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*").withColumn("event_time", col("timestamp").cast("timestamp"))

windowed_features = parsed_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        window(col("event_time"), "10 minutes", "10 seconds"),
        col("user_id")
    ) \
    .agg(
        count("transaction_id").alias("tx_count_10m"),
        sum("amount").alias("total_amount_10m")
    )

# 6. Push Function for Feast
def push_to_feast(df, batch_id):
    row_count = df.count()
    print(f"[*] Micro-batch {batch_id} triggered with {row_count} calculated feature updates.")
    
    if row_count == 0:
        return

    rows = df.collect()
    records = []
    current_time = datetime.now(timezone.utc)

    for row in rows:
        records.append({
            "user_id": str(row["user_id"]),
            "event_timestamp": current_time,
            "tx_count_10m": int(row["tx_count_10m"]),
            "total_amount_10m": float(row["total_amount_10m"]) if row["total_amount_10m"] else 0.0
        })

    pdf = pd.DataFrame(records)

    try:
        store.push("user_transaction_push_source", pdf, to="online_store")
        print(f"  [+] SUCCESS: Pushed Batch {batch_id} ({len(pdf)} rows) to Feast SQLite!")
    except Exception as e:
        print(f"  [!] ERROR pushing to Feast: {e}")

print("[*] Starting PySpark Stream Sink to Feast...")

# IMPORTANT: outputMode("update") ensures real-time micro-batch emission
query = windowed_features.writeStream \
    .outputMode("update") \
    .foreachBatch(push_to_feast) \
    .start()

query.awaitTermination()