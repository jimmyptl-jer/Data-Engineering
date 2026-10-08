# Databricks notebook source
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, TimestampType
BOOTSTRAP_SERVERS = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"

TOPIC = "stock.company.earnings"

API_KEY = "FOXBCYKSLJBCM7BA" 
API_SECRET = "cfltTefCzxLz4UA9vOkZQzOSgSPbrpYrGUPQi5GaFOFVhob0Qvtm30BR4S+w+WTg"


KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": BOOTSTRAP_SERVERS,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{API_KEY}" '
        f'password="{API_SECRET}";'
    ),
    "subscribe": TOPIC,
    "startingOffsets": "earliest",
}

# 1. Define the schema matching your Kafka message
schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("volume", LongType()) \
    .add("timestamp", TimestampType())

# 2. Read raw stream from Kafka
df_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "pkc-921jm.us-east-2.aws.confluent.cloud:9092:9092")
    .option("subscribe", "stock.company.earnings")
    .option("startingOffsets", "latest")   # or "earliest" for full backlog
    .load()
)

display(df_stream)

bronze_df = df_stream.select(
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("data"),
    col("topic").alias("Source"),
    col("timestamp")
)


query = (
    bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/bronze_stock")
        .toTable("bronze_stock")
)

print(query)
