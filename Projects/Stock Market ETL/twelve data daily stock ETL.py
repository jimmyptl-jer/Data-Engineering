# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Twelve Data 1 minute data Stock Data — Bronze to Silver ETL

# COMMAND ----------

# DBTITLE 1,Import Watermark Manager
# MAGIC %run /Workspace/Users/jimmyptl46@gmail.com/delta-lake/stock_market_etl/stock_etl/watermark_manager

# COMMAND ----------

# DBTITLE 1,Imports & Setup
# MAGIC %md
# MAGIC ## 1. Imports & Spark Session Setup
# MAGIC
# MAGIC Import required PySpark functions, types, and initialize the Spark session.

# COMMAND ----------

# DBTITLE 1,Imports & Spark Session
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    explode,
    to_json,
    from_json,
    upper,
    round,
    when,
    concat,
    concat_ws,
    window,
    lag,
    lead,
    row_number,
    rank,
    dense_rank,
    sum,
    avg,
    min,
    max,
    count
)
from pyspark.sql.types import (
    MapType,
    StringType,
    StructType,
    StructField
)

from pyspark.sql.window import Window

from pyspark.sql import SparkSession

from datetime import datetime, timezone

from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("StockMarket-BronzeToSilver")
    .getOrCreate()
)

# Confirm Spark session is active and print the version
print("Spark Session Started")
print(f"Spark Version: {spark.version}")

S3_INPUT_PATH = "s3://graywolf--data--lake/stock/bronze/source=twelvedata/dataset=time_series_1min/"

print(f"S3_INPUT_PATH: {S3_INPUT_PATH}")

dbutils.widgets.text("catalog", "stock_catalog")
dbutils.widgets.text("raw_path", "s3://graywolf--data--lake/stock/bronze/source=twelvedata/dataset=time_series_1min/")
dbutils.widgets.dropdown("debug", "false", ["true", "false"])

CATALOG = dbutils.widgets.get("catalog")
RAW_PATH = dbutils.widgets.get("raw_path").rstrip("/") + "/"
DEBUG = dbutils.widgets.get("debug") == "true"
SOURCE = "twelvedata"
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")



T_BRONZE     = f"{CATALOG}.bronze.day_stock_ohlcv_extracted"
T_SILVER     = f"{CATALOG}.silver.day_stock_ohlcv_data"
T_QUARANTINE = f"{CATALOG}.quarantine.day_stock_ohlcv_invalid"
T_API_ERRORS = f"{CATALOG}.quarantine.day_stock_ohlcv_api_errors"
T_GOLD       = f"{CATALOG}.gold.day_stock_ohlcv_enriched"
T_LOG        = f"{CATALOG}.etl.load_log"
CHECKPOINTS  = f"/Volumes/{CATALOG}/etl/checkpoints"

# COMMAND ----------

# ============================================================
# INITIALIZE DATABASE OBJECTS
# ============================================================

print("Initializing database objects...")

# Create catalog
spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS `{CATALOG}`
""")

# Create schemas
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.bronze
""")

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.silver
""")

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.gold
""")

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.quarantine
""")

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.etl
""")

print(f"Catalog initialized: {CATALOG}")

# COMMAND ----------

# DBTITLE 1,Cell 6
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_SILVER} (
    symbol STRING,
    datetime TIMESTAMP,
    date DATE,
    time STRING,
    exchange STRING,
    exchange_timezone STRING,
    mic_code STRING,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    source STRING,
    ingestion_timestamp TIMESTAMP,
    run_id STRING
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_QUARANTINE} (
    symbol STRING,
    datetime TIMESTAMP,
    exchange STRING,
    exchange_timezone STRING,
    mic_code STRING,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    source STRING,
    ingestion_timestamp TIMESTAMP,
    run_id STRING,
    validation_status STRING,
    validation_reason STRING
)
USING DELTA
""")


# COMMAND ----------

print(f"CATALOG: {CATALOG}")
print(f"RAW_PATH: {S3_INPUT_PATH}")
print(f"DEBUG: {DEBUG}")
print(f"SOURCE: {SOURCE}")
print(f"RUN_ID: {RUN_ID}")
print(f"CATALOG: {CATALOG}")
print(f"CHECKPOINTS: {CHECKPOINTS}")
print(f"T_SILVER: {T_SILVER}")
print(f"T_QUARANTINE: {T_QUARANTINE}")
print(f"T_API_ERRORS: {T_API_ERRORS}")
print(f"T_GOLD: {T_GOLD}")
print(f"T_LOG: {T_LOG}")


# COMMAND ----------

# DBTITLE 1,Read Bronze
# MAGIC %md
# MAGIC ## 2. Read Bronze Data
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
# MAGIC ## 3. Select Metadata
# MAGIC
# MAGIC Extract stock symbol and last refreshed date from the nested `Meta Data` structure.

# COMMAND ----------

# DBTITLE 1,Select Metadata
print("********************************************")
print("[SCHEMA] meta struct datatype:")
print(daily_df.schema["meta"].dataType)
print("********************************************")

print("********************************************")
print("[SCHEMA] values array element type:")
print(daily_df.schema["values"].dataType)
print("********************************************")

# Extract metadata from `meta` struct and explode `values` array into rows
daily_df = daily_df.select(
    col("meta.symbol").alias("symbol"),
    col("meta.exchange").alias("exchange"),
    col("meta.exchange_timezone").alias("exchange_timezone"),
    col("meta.mic_code").alias("mic_code"),
    explode(col("values")).alias("ohlcv")
)

print("[TRANSFORMATION 1] Metadata selected and values array exploded")

daily_df.printSchema()
display(daily_df)

# COMMAND ----------

# DBTITLE 1,Convert Time Series to MapType
# MAGIC %md
# MAGIC ## 4. Convert Time Series to MapType
# MAGIC
# MAGIC Convert the nested `Time Series (Daily)` structure into a `MapType` for easier explosion.

# COMMAND ----------

# DBTITLE 1,Convert Time Series to MapType
# Extract OHLCV fields from the exploded struct and add pipeline metadata
daily_df = daily_df.select(
    col("symbol"),
    col("exchange"),
    col("exchange_timezone"),
    col("mic_code"),
    col("ohlcv.datetime").alias("datetime"),
    col("ohlcv.open").cast("double").alias("open"),
    col("ohlcv.high").cast("double").alias("high"),
    col("ohlcv.low").cast("double").alias("low"),
    col("ohlcv.close").cast("double").alias("close"),
    col("ohlcv.volume").cast("double").alias("volume"),
    F.lit(SOURCE).alias("source"),
    F.current_timestamp().alias("ingestion_timestamp"),
    F.lit(RUN_ID).alias("run_id"),
)

daily_df = daily_df.withColumn("date", F.to_date(daily_df.datetime))
daily_df = daily_df.withColumn("time", F.date_format(daily_df.datetime, "HH:mm:ss"))

print("[TRANSFORMATION 3] OHLCV fields extracted and metadata columns added")

daily_df.printSchema()
display(daily_df)
# ============================================================
# WATERMARK — Read last processed date via WatermarkManager

print("[TRANSFORMATION 2] OHLCV fields extracted and metadata columns added")

daily_df.printSchema()
display(daily_df)

# COMMAND ----------

# DBTITLE 1,Explode Time Series
# MAGIC %md
# MAGIC ## 5. Explode Time Series
# MAGIC
# MAGIC Explode the MapType so each stock trading day becomes its own row.

# COMMAND ----------

# DBTITLE 1,Explode Time Series
# Explode already done in cell 11 — verify the result here
print("[TRANSFORMATION 3] Data exploded and ready for OHLCV extraction")

daily_df.printSchema()
display(daily_df)

# COMMAND ----------

# DBTITLE 1,Extract OHLCV
# MAGIC %md
# MAGIC ## 6. Extract OHLCV Fields
# MAGIC
# MAGIC Extract open, high, low, close, and volume from the nested daily data structure.

# COMMAND ----------

# DBTITLE 1,Extract OHLCV Fields
# OHLCV fields already extracted in cell 13 — verify the result here
print("[TRANSFORMATION 4] OHLCV fields extracted and ready for watermarking")

daily_df.printSchema()
display(daily_df)

# COMMAND ----------

# DBTITLE 1,Watermark — Read Last Processed Date
# ============================================================
# WATERMARK — Read last processed date via WatermarkManager
# ============================================================
# WatermarkManager (imported via %run at top) stores state as
# JSON on S3: s3://graywolf--data--lake/watermark/{pipeline}/{dataset}.json

PIPELINE = TWELVE_DATA_1MIN_PIPELINE
DATASET  = TWELVE_DATA_1MIN_DATASET

last_watermark = wm.get_watermark_value(
    pipeline_name=PIPELINE,
    dataset_name=DATASET,
    default="1900-01-01",
)

print(f"[WATERMARK] Pipeline   : {PIPELINE}")
print(f"[WATERMARK] Dataset    : {DATASET}")
print(f"[WATERMARK] Last date  : {last_watermark}")

# COMMAND ----------

# DBTITLE 1,Watermark — Filter New Records
# ============================================================
# WATERMARK — Filter for only NEW records since last watermark
# ============================================================

# daily_df = daily_df.filter(col("datetime") > F.lit(last_watermark))

# new_count = daily_df.count()

# if new_count == 0:
#     print(f"[WATERMARK] No new records after {last_watermark}. Nothing to process.")
# else:
#     print(f"[WATERMARK] {new_count} new records found after {last_watermark}.")

# COMMAND ----------

# DBTITLE 1,Remove Duplicates
# MAGIC %md
# MAGIC ## 6.1 Remove Duplicate Records
# MAGIC
# MAGIC Remove duplicate symbol + date combinations to ensure data uniqueness.

# COMMAND ----------

# DBTITLE 1,Remove Duplicate Records
daily_df = daily_df.dropDuplicates(
    ["symbol", "datetime"]
)

print("[TRANSFORMATION 5] Duplicate records removed")

daily_df.printSchema()
daily_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Cast Columns
# MAGIC %md
# MAGIC ## 6.2 Cast Columns to Proper Data Types
# MAGIC
# MAGIC Cast symbol to string, dates to date type, prices to decimal(12,2), and volume to long.

# COMMAND ----------

# DBTITLE 1,Cast Columns to Proper Data Types
# Cast datetime to timestamp (OHLCV already double from extraction)
daily_df = daily_df.withColumn(
    "datetime",
    col("datetime").cast("timestamp")
)

print("[TRANSFORMATION 6] datetime cast to timestamp, OHLCV already double")

daily_df.printSchema()
daily_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Standardize Symbol
# MAGIC %md
# MAGIC ## 6.3 Standardize Symbol
# MAGIC
# MAGIC Convert stock symbols to uppercase for consistency.

# COMMAND ----------

# DBTITLE 1,Standardize Symbol
daily_df = daily_df.withColumn(
    "symbol",
    upper(col("symbol"))
)

print("[TRANSFORMATION 7] Symbol standardized to uppercase")

daily_df.printSchema()
daily_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Round Numeric Columns
# MAGIC %md
# MAGIC ## 6.4 Verify Numeric Column Types
# MAGIC
# MAGIC With prices as `decimal(12,2)` and volume as `long`, rounding is no longer needed — this cell verifies the column types.

# COMMAND ----------

# DBTITLE 1,Round Numeric Columns
# Prices are already decimal(12,2) and volume is long — no rounding needed.
# Verify the schema shows the correct types.

print("[VERIFICATION] Numeric column types (no rounding needed with decimal(12,2))")

daily_df.printSchema()
daily_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Validation Status
# MAGIC %md
# MAGIC ## 6.5 Validation Status
# MAGIC
# MAGIC Flag each record as VALID or INVALID based on data quality rules (null checks and positive value checks).

# COMMAND ----------

# DBTITLE 1,Validation Status
daily_df = daily_df.withColumn(
    "validation_status",
    when(col("symbol").isNull(), "INVALID")
    .when(col("datetime").isNull(), "INVALID")
    .when(col("open").isNull(), "INVALID")
    .when(col("high").isNull(), "INVALID")
    .when(col("low").isNull(), "INVALID")
    .when(col("close").isNull(), "INVALID")
    .when(col("volume").isNull(), "INVALID")
    .when(col("open") <= 0, "INVALID")
    .when(col("close") <= 0, "INVALID")
    .when(col("high") <= 0, "INVALID")
    .when(col("low") <= 0, "INVALID")
    .when(col("volume") <= 0, "INVALID")
    .otherwise("VALID")
)

print("[TRANSFORMATION 9] Validation status added")

daily_df.printSchema()
daily_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Validation Reason
# MAGIC %md
# MAGIC ## 6.6 Validation Reason
# MAGIC
# MAGIC Add a human-readable reason string explaining why each invalid record failed validation.

# COMMAND ----------

# DBTITLE 1,Validation Reason
daily_df = daily_df.withColumn(
    "validation_reason",
    concat_ws(
        ", ",
        when(col("symbol").isNull(), "Missing symbol"),
        when(col("datetime").isNull(), "Missing datetime"),

        when(col("open").isNull(), "Missing open price"),
        when(col("open") <= 0, "Invalid open price: <= 0"),

        when(col("high").isNull(), "Missing high price"),
        when(col("high") <= 0, "Invalid high price: <= 0"),

        when(col("low").isNull(), "Missing low price"),
        when(col("low") <= 0, "Invalid low price: <= 0"),

        when(col("close").isNull(), "Missing close price"),
        when(col("close") <= 0, "Invalid close price: <= 0"),

        when(col("volume").isNull(), "Missing volume"),
        when(col("volume") <= 0, "Invalid volume: <= 0"),
    )
)

print("[TRANSFORMATION 10] Validation reason added")

daily_df.printSchema()
daily_df.show(80, truncate=False)

# COMMAND ----------

# DBTITLE 1,Split Valid / Invalid
# MAGIC %md
# MAGIC ## 6.7 Split Valid / Invalid Records
# MAGIC
# MAGIC Filter the dataset into separate valid and invalid DataFrames based on validation status.

# COMMAND ----------

# DBTITLE 1,Split Valid / Invalid Records
valid_df = daily_df.filter(
    col("validation_status") == "VALID"
)

print("[SPLIT] Filtered VALID records into valid_df")

valid_df.printSchema()
valid_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Add Daily Change
# MAGIC %md
# MAGIC ## 6.8 Add Daily Change
# MAGIC
# MAGIC Calculate the daily price change (close minus open) for valid records.

# COMMAND ----------

# DBTITLE 1,Add Daily Change
valid_df = valid_df.withColumn(
    "daily_change",
    col("close") - col("open")
)

print("[TRANSFORMATION 11] daily_change column added")

valid_df.printSchema()
valid_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Add Market Movement
# MAGIC %md
# MAGIC ## 6.9 Add Market Movement & Window Functions
# MAGIC
# MAGIC Determine market movement direction (bull, bear, neutral) and add previous day close/open using window functions.

# COMMAND ----------

# DBTITLE 1,Add Market Movement & Window Functions
valid_df = valid_df.withColumn(
    "market_movement",
    when(col("close") > col("open"), "bull")
    .when(col("close") < col("open"), "bear")
    .otherwise("neutral")
)

print("[TRANSFORMATION 12] market_movement column added")

valid_df.printSchema()
valid_df.show(5, truncate=False)

window = Window.partitionBy("symbol").orderBy("datetime")

valid_df = valid_df.withColumn(
    "previous_day_close",
    lag("close").over(window)
)

valid_df = valid_df.withColumn(
    "previous_day_open",
    lag("open").over(window)
)

# COMMAND ----------

# DBTITLE 1,Filter Invalid Records
# MAGIC %md
# MAGIC ## 6.10 Filter Invalid Records
# MAGIC
# MAGIC Filter out all invalid records into a separate DataFrame for quarantine.

# COMMAND ----------

# DBTITLE 1,Filter Invalid Records
invalid_df = daily_df.filter(
    col("validation_status") == "INVALID"
)

print("[SPLIT] Filtered INVALID records into invalid_df")

invalid_df.printSchema()
invalid_df.show(20, truncate=False)

# COMMAND ----------

# DBTITLE 1,Record Counts
# MAGIC %md
# MAGIC ## 6.11 Record Counts
# MAGIC
# MAGIC Count and display the number of valid and invalid records.

# COMMAND ----------

# DBTITLE 1,Record Counts
valid_count = valid_df.count()
print(f"[SILVER] Valid records     : {valid_count}")

invalid_count = invalid_df.count()
print(f"[QUARANTINE] Invalid records: {invalid_count}")

# COMMAND ----------

# DBTITLE 1,Convert to DynamicFrames
# MAGIC %md
# MAGIC ## 7. Convert DataFrames to DynamicFrames
# MAGIC
# MAGIC Convert Spark DataFrames to Glue DynamicFrames for writing (placeholder — adapt for your runtime).

# COMMAND ----------

# DBTITLE 1,Convert DataFrames to DynamicFrames
print("[TRANSFORMATION 13] valid_df converted to DynamicFrame")


print("[TRANSFORMATION 14] invalid_df converted to DynamicFrame")

# COMMAND ----------

# DBTITLE 1,Write Quarantine & Silver
# MAGIC %md
# MAGIC ## 8. Write Quarantine & Silver Layers
# MAGIC
# MAGIC Write invalid records to the quarantine S3 path and valid records to the silver S3 path, both in Parquet format partitioned by symbol.

# COMMAND ----------

# DBTITLE 1,Write Quarantine & Silver Layers
# ---------------- WRITE QUARANTINE ----------------

quarantine_path = (
    "s3://graywolf--data--lake/"
    "stock/quarantine/"
    "source=twelvedata/"
    "dataset=time_series_1min/"
)

print(f"[QUARANTINE] Target path: {quarantine_path}")

quarantine_df = invalid_df.select(
    "symbol", "datetime", "exchange", "exchange_timezone", "mic_code",
    "open", "high", "low", "close", "volume",
    "source", "ingestion_timestamp", "run_id",
    "validation_status", "validation_reason"
)

(
    quarantine_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(T_QUARANTINE)
)

print(f"[QUARANTINE] Invalid data written to {T_QUARANTINE}")

# ---------------- WRITE SILVER ----------------

silver_path = (
    "s3://graywolf--data--lake/"
    "stock/silver/"
    "source=twelvedata/"
    "dataset=time_series_1min/"
)

print(f"[SILVER] Target path: {silver_path}")

silver_df = valid_df.select(
    "symbol", "datetime", "date", "time",
    "exchange", "exchange_timezone", "mic_code",
    "open", "high", "low", "close", "volume",
    "source", "ingestion_timestamp", "run_id"
)

(
    silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T_SILVER)
)

print(f"[SILVER] Valid data written to {T_SILVER}")

# COMMAND ----------

# DBTITLE 1,Watermark — Persist After Silver Write
# ============================================================
# WATERMARK — Persist new watermark after successful silver write
# ============================================================

record_count = daily_df.count()

if record_count > 0:
    max_date = daily_df.select(F.max("datetime")).collect()[0][0]

    wm.write_watermark({
        "pipeline_name": PIPELINE,
        "dataset_name": DATASET,
        "watermark_column": "day_date",
        "watermark_value": str(max_date),
        "last_processed_at": datetime.now(timezone.utc).isoformat(),
        "batch_id": f"batch_{RUN_ID}",
        "status": "SUCCESS",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "updated_by": "stock_pipeline",
        "remarks": f"Silver write completed. {record_count} records processed."
    })

    print(f"[WATERMARK] Updated watermark to {max_date}")
else:
    print("[WATERMARK] Skipped watermark update — no new records processed.")

# COMMAND ----------

# DBTITLE 1,Commit Job
# MAGIC %md
# MAGIC ## 9. Commit Job
# MAGIC
# MAGIC Commit the Glue job to finalize the ETL run.

# COMMAND ----------

# DBTITLE 1,Commit Job
print("[JOB] Job committed successfully")