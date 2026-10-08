# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Step 1 — Bronze Extraction
# MAGIC
# MAGIC This notebook reads raw Alpha Vantage daily stock data from the Bronze S3 layer, parses the nested JSON structure, explodes the time series into individual rows, and extracts OHLCV fields. The result is written to an intermediate Delta table for the next step.

# COMMAND ----------

# DBTITLE 1,Imports & Setup
# MAGIC %md
# MAGIC ## Imports & Spark Session Setup

# COMMAND ----------

# DBTITLE 1,Imports & Spark Session
from pyspark.sql.functions import (
    col,
    explode,
    to_json,
    from_json
)
from pyspark.sql.types import (
    MapType,
    StringType,
    StructType,
    StructField
)
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("StockMarket-BronzeExtraction")
    .getOrCreate()
)

print("Spark Session Started")
print(f"Spark Version: {spark.version}")

S3_INPUT_PATH = "s3://graywolf--data--lake/stock/bronze/source=alphavantage/dataset=daily_time_series/"
INTERMEDIATE_TABLE = "stock_catalog.bronze.daily_stock_extracted"

# COMMAND ----------

# DBTITLE 1,Read Bronze Data
# MAGIC %md
# MAGIC ## Read Bronze Data
# MAGIC
# MAGIC Load raw Alpha Vantage JSON data from the Bronze S3 layer.

# COMMAND ----------

# DBTITLE 1,Read Bronze Data
daily_df = (
    spark.read
    .format("json")
    .option("multiLine", "true")
    .option("recursiveFileLookup", "true")
    .load(S3_INPUT_PATH)
)

print("[BRONZE] Data loaded successfully")
daily_df.printSchema()
display(daily_df)

# COMMAND ----------

# DBTITLE 1,Select Metadata
# MAGIC %md
# MAGIC ## Select Metadata
# MAGIC
# MAGIC Extract stock symbol and last refreshed date from the nested `Meta Data` structure.

# COMMAND ----------

# DBTITLE 1,Select Metadata
print("********************************************")
print("[SCHEMA] Meta Data datatype:")
print(daily_df.schema["Meta Data"].dataType)
print("********************************************")

print("********************************************")
print("[SCHEMA] Time Series (Daily) datatype:")
print(daily_df.schema["Time Series (Daily)"].dataType)
print("********************************************")

daily_df = daily_df.select(
    col("`Meta Data`.`2. Symbol`").alias("symbol"),
    col("`Meta Data`.`3. Last Refreshed`").alias("last_refreshed"),
    col("`Time Series (Daily)`")
)

print("[TRANSFORMATION 1] Metadata selected successfully")
daily_df.printSchema()
daily_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Convert Time Series to MapType
# MAGIC %md
# MAGIC ## Convert Time Series to MapType
# MAGIC
# MAGIC Convert the nested `Time Series (Daily)` structure into a `MapType` for easier explosion.

# COMMAND ----------

# DBTITLE 1,Convert Time Series to MapType
ohlcv_schema = StructType([
    StructField("1. open", StringType()),
    StructField("2. high", StringType()),
    StructField("3. low", StringType()),
    StructField("4. close", StringType()),
    StructField("5. volume", StringType())
])

print("[SCHEMA] OHLCV schema defined")

daily_df = daily_df.withColumn(
    "time_series_map",
    from_json(
        to_json(col("`Time Series (Daily)`")),
        MapType(
            StringType(),
            ohlcv_schema
        )
    )
)

print("[TRANSFORMATION 2] Time Series converted to MapType")
daily_df.printSchema()
daily_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Explode Time Series
# MAGIC %md
# MAGIC ## Explode Time Series
# MAGIC
# MAGIC Explode the MapType so each stock trading day becomes its own row.

# COMMAND ----------

# DBTITLE 1,Explode Time Series
daily_df = daily_df.select(
    col("symbol"),
    col("last_refreshed"),
    explode(
        col("time_series_map")
    ).alias(
        "day_date",
        "daily_data"
    )
)

print("[TRANSFORMATION 3] Time Series exploded successfully")
daily_df.printSchema()
display(daily_df)

# COMMAND ----------

# DBTITLE 1,Extract OHLCV Fields
# MAGIC %md
# MAGIC ## Extract OHLCV Fields
# MAGIC
# MAGIC Extract open, high, low, close, and volume from the nested daily data structure.

# COMMAND ----------

# DBTITLE 1,Extract OHLCV Fields
daily_df = daily_df.select(
    col("symbol"),
    col("last_refreshed"),
    col("day_date"),
    col("daily_data.`1. open`").alias("open"),
    col("daily_data.`2. high`").alias("high"),
    col("daily_data.`3. low`").alias("low"),
    col("daily_data.`4. close`").alias("close"),
    col("daily_data.`5. volume`").alias("volume")
)

print("[TRANSFORMATION 4] OHLCV fields extracted successfully")
daily_df.printSchema()
display(daily_df)

# COMMAND ----------

# DBTITLE 1,Write to Intermediate Table
# MAGIC %md
# MAGIC ## Write to Intermediate Delta Table
# MAGIC
# MAGIC Write the extracted data to an intermediate Delta table so the next notebook (Data Cleaning) can pick it up.

# COMMAND ----------

# DBTITLE 1,Write to Intermediate Table
(
    daily_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(INTERMEDIATE_TABLE)
)

print(f"[BRONZE] Extracted data written to {INTERMEDIATE_TABLE}")
print(f"[BRONZE] Row count: {daily_df.count()}")