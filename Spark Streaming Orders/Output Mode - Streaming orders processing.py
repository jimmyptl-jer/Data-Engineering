# Databricks notebook source
# DBTITLE 1,Imports
# Common imports used throughout the notebook:
# - SQL functions (col, trim, to_date, ...) used in the flatten steps
# - SQL types used to define the explicit JSON schema
# - `F` alias for the full functions module
from pyspark.sql.functions import (
    col,
    current_date,
    current_timestamp,
    dayofmonth,
    from_utc_timestamp,
    lower,
    regexp_replace,
    split,
    to_date,
    trim,
    upper,
    year,
    month,
    when
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType
)

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Define the order JSON schema
# Explicit schema for the raw order JSON files (one nested document per file).
# An explicit schema is required for the streaming read and is faster and more
# reliable than inferSchema.
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    ArrayType
)

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("customer", StructType([
        StructField("customer_id", LongType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("address", StructType([
            StructField("city", StringType()),
            StructField("country", StringType()),
            StructField("postal_code", StringType()),
        ])),
    ])),
    StructField("items", ArrayType(StructType([
        StructField("item_id", StringType()),
        StructField("product_name", StringType()),
        StructField("quantity", LongType()),
        StructField("price", DoubleType()),
    ]))),
    StructField("payment", StructType([
        StructField("method", StringType()),
        StructField("transaction_id", StringType()),
    ])),
    StructField("metadata", ArrayType(StructType([
        StructField("key", StringType()),
        StructField("value", StringType()),
    ]))),
])

# (The batch read lives in the 'Batch orders processing' notebook)

# COMMAND ----------

# DBTITLE 1,Read orders as a streaming DataFrame via Auto Loader
# STREAMING read of the same order files using Auto Loader.
# - multiLine=true (a plain JSON format option - NOT prefixed with cloudFiles -
#   format options pass straight through to the JSON parser) is required because
#   each file is a single pretty-printed JSON document
# - an explicit schema is mandatory: schema inference is unavailable for streaming
#   sources on serverless compute


base = "/Volumes/spark-streaming/default/streaming"

streaming_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("multiLine", "true")
    .schema(order_schema)
    .load(f"{base}/landing/") 
)

# COMMAND ----------

# DBTITLE 1,Flatten streaming orders (same logic as stream_df)
# STREAMING flatten - mirrors the batch pipeline in the 'Batch orders processing'
# notebook: same transformations applied to streaming_df.
# Explodes are supported in streaming queries, so the logic is identical.
# 1: flatten structs (customer, payment) and explode items
streaming_items_df = streaming_df.select(
    trim(col("order_id")).alias("order_id"),
    F.to_date(col("timestamp")).alias("order_date"),
    col("customer.customer_id").alias("customer_id"),
    col("customer.email").alias("customer_email"),
    col("customer.address.city").alias("customer_address_city"),
    col("customer.address.country").alias("customer_address_country"),
    col("customer.address.postal_code").alias("customer_address_postal_code"),
    col("payment.method").alias("payment_method"),
    col("payment.transaction_id").alias("payment_transaction_id"),
    col("metadata"),
    F.explode(col("items")).alias("item"),
)

# 2: explode metadata (same Cartesian-product caveat as the batch pipeline)
streaming_meta_df = streaming_items_df.select(
    "*",
    F.explode(col("metadata")).alias("meta"),
)

# 3: extract fields from the exploded item and metadata structs into flat columns
streaming_flatten_df = streaming_meta_df.select(
    "order_id",
    "order_date",
    "customer_id",
    "customer_email",
    "customer_address_city",
    "customer_address_country",
    "customer_address_postal_code",
    "payment_method",
    "payment_transaction_id",
    col("meta.key").alias("metadata_key"),
    col("meta.value").alias("metadata_value"),
    col("item.item_id").alias("customer_items_id"),
    col("item.product_name").alias("customer_items_product_name"),
    col("item.quantity").alias("customer_items_quantity"),
    col("item.price").alias("customer_items_price"),
)

streaming_flatten_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Write flattened stream to Delta (append mode)
# STREAMING WRITE (sink): write the flattened streaming data into a Delta table.
# - outputMode("append"): only new rows are emitted to the sink (the default and
#   the simplest output mode - update/complete are for aggregations)
# - checkpointLocation: tracks which input files have already been processed,
#   so re-running the cell resumes instead of reprocessing everything
# - trigger(availableNow=True): run as a one-shot bounded job - process all
#   currently available files, then stop (instead of running continuously)

streaming_query = (
    streaming_flatten_df.writeStream
    .outputMode("append")
    .option("checkpointLocation", f"{base}/_meta/json_stream/_checkpoint")
    .trigger(availableNow=True)
    .toTable("`spark-streaming`.jsonstream.json_stream")
)


print("Rows in the streaming sink table:", spark.table("`spark-streaming`.jsonstream.json_stream").count())

# COMMAND ----------

# DBTITLE 1,Read the Delta table back as a stream
# STREAMING READ (source): consume the Delta table written above as a stream.
# Delta tables are streaming sources too - each new commit in the sink table
# becomes available to this query as it lands.
# A streaming DataFrame cannot use batch actions (.show()), so we drain it
# one-shot into an in-memory view and then query that view.
#
# NOTE: the memory sink cannot restart from an existing checkpoint (its state is
# gone once the query stops), so each run uses a fresh checkpoint directory
# instead of resuming a previous one.
import uuid

delta_stream_df = (
    spark.readStream
    .format("delta")
    .table("`spark-streaming`.jsonstream.json_stream")
)

read_query = (
    delta_stream_df.writeStream
    .format("memory")
    .queryName("orders_stream_view")
    .outputMode("append")
    .option(
        "checkpointLocation",
        f"/Volumes/spark-streaming/default/streaming/.checkpoints/read_delta_stream/{uuid.uuid4().hex[:8]}",
    )
    .trigger(availableNow=True)
    .start()
)
read_query.awaitTermination()

spark.sql("SELECT * FROM orders_stream_view").show(truncate=False)