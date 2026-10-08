from databricks.sdk.runtime import spark
from pyspark.sql.functions import col

# =====================================================================
# AWS Configuration
# =====================================================================

AWS_ACCESS_KEY = "AKIAR2HNCS6I3MXUYCSM" 
AWS_SECRET_KEY = "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"

S3_BUCKET = "graywolf-crypto-market-data"

spark.conf.set("fs.s3a.access.key", AWS_ACCESS_KEY)
spark.conf.set("fs.s3a.secret.key", AWS_SECRET_KEY)
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
spark.conf.set("fs.s3a.path.style.access", "true")

# =====================================================================
# Kafka Configuration
# =====================================================================

BOOTSTRAP_SERVERS = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"

TOPIC = "crypto_data"

API_KEY = "SEB2YTKCRA5NV3M4" 
API_SECRET = "cflteU7ubHJA0cCYQQwTvhkGu+A79z/CcGRXPP2IgdMZ8zSSplVGX+5ObTSotsKg"

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

# =====================================================================
# Read Kafka Stream
# =====================================================================

def read_kafka_stream():
    """
    Reads Kafka messages from Confluent Cloud as a streaming DataFrame.
    """

    kafka_df = (
        spark.readStream
             .format("kafka")
             .options(**KAFKA_OPTIONS)
             .load()
    )

    messages = (
        kafka_df.select(
            col("timestamp"),
            col("partition"),
            col("offset"),
            col("key").cast("string").alias("key"),
            col("value").cast("string").alias("value"),
        )
    )

    return messages


# =====================================================================
# Write Stream to Amazon S3
# =====================================================================

def write_to_s3(messages):
    """
    Writes the streaming DataFrame to Amazon S3 in Parquet format.
    """

    query = (
        messages.writeStream
            .format("parquet")
            .outputMode("append")
            .option(
                "path",
                f"s3a://{S3_BUCKET}/bronze/crypto_data/"
            )
            .option(
                "checkpointLocation",
                f"s3a://{S3_BUCKET}/checkpoints/crypto_data/"
            )
            .trigger(processingTime="30 seconds")
            .start()
    )

    print("Streaming has started...")

    query.awaitTermination()


# =====================================================================
# Main
# =====================================================================

if __name__ == "__main__":

    # Read messages from Kafka
    messages = read_kafka_stream()

    # Optional: View schema
    messages.printSchema()

    # Start streaming to S3
    write_to_s3(messages)