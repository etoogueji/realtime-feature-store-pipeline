import os
import sys

# Maintain your local JAVA_HOME configuration
JAVA_PATH = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.12.7-hotspot"  # Verify folder name matches your JDK installation
if os.path.exists(JAVA_PATH):
    os.environ["JAVA_HOME"] = JAVA_PATH
    os.environ["PATH"] = os.path.join(JAVA_PATH, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, sum as _sum, avg
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

# Fixed Kafka Package Dependency
KAFKA_JAR_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

def create_spark_session():
    print("[*] Initializing PySpark Session with Kafka dependencies...")
    spark = (
        SparkSession.builder
        .appName("RealTimeFraudFeatureProcessor")
        .config("spark.jars.packages", KAFKA_JAR_PACKAGE)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("[+] PySpark Session successfully initialized!")
    return spark

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

def process_stream():
    spark = create_spark_session()

    print("[*] Subscribing to Redpanda topic 'financial_transactions'...")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:19092")
        .option("subscribe", "financial_transactions")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_payload")
        .select(from_json(col("json_payload"), transaction_schema).alias("data"))
        .select("data.*")
    )

    windowed_features = (
        parsed_stream
        .withWatermark("timestamp", "1 minute")
        .groupBy(
            window(col("timestamp"), "10 minutes", "10 seconds"), 
            col("user_id")
        )
        .agg(
            count("transaction_id").alias("transaction_count_10m"),
            _sum("amount").alias("total_amount_10m"),
            avg("amount").alias("avg_amount_10m")
        )
        .select(
            col("user_id"),
            col("window.end").alias("feature_timestamp"),
            col("transaction_count_10m"),
            col("total_amount_10m"),
            col("avg_amount_10m")
        )
    )

    print("[*] Starting PySpark Stream Console Writer...")
    query = (
        windowed_features.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .start()
    )
    query.awaitTermination()

if __name__ == "__main__":
    process_stream()