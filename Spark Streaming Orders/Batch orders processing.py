# Databricks notebook source
# DBTITLE 1,Batch Orders Processing
# MAGIC %md
# MAGIC # Batch Orders Processing
# MAGIC
# MAGIC Loads the multiline order JSON files from the `spark-streaming` volume, flattens them with `select` and `explode`, and overwrites the `spark-streaming.jsonstream.json` Delta table.
# MAGIC
# MAGIC The streaming counterpart lives in the *Streaming orders processing* notebook.

# COMMAND ----------

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

# DBTITLE 1,Define order schema and load batch data
# Explicit schema for the raw order JSON files (one nested document per file).
# An explicit schema is faster and more reliable than inferSchema.
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

# BATCH read of the multiline order JSON files
stream_df = (
    spark.read.format("json")
    .option("multiLine", "true")
    .schema(order_schema)
    .load("/Volumes/spark-streaming/default/streaming")
)

# COMMAND ----------

# DBTITLE 1,Inspect raw batch schema
# Inspect the raw (nested) schema of the batch DataFrame
stream_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Flatten orders with select
# Step 1: flatten the nested structs (customer, payment) into top-level columns
# and explode the items array - one row per line item
df_items = stream_df.select(
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

# Step 2: explode the metadata array - NOTE: a second explode creates a Cartesian
# product with items (2 items x 2 metadata entries = 4 rows per order)
df_meta = df_items.select(
    "*",
    F.explode(col("metadata")).alias("meta"),
)

# Step 3: extract the fields of the exploded item and metadata structs
# into flat scalar columns
flatten_df = df_meta.select(
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

flatten_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Inspect flattened batch schema
# Verify the flattened schema
flatten_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Write flattened batch data to Delta
# BATCH write: overwrite the Delta table with the full flatten result each run
(
    flatten_df
    .write
    .format("delta")
    .option("overwriteSchema", "true")
    .mode("overwrite")
    .saveAsTable("`spark-streaming`.jsonstream.json")
)