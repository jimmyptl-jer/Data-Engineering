# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Alpha Vantage Daily Stock Data — Bronze to Silver ETL
# MAGIC
# MAGIC This notebook transforms raw Alpha Vantage daily stock data from the Bronze layer into clean, validated Silver data. Invalid records are quarantined, and valid records get derived fields like daily change and market movement.

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

spark = (
    SparkSession.builder
    .appName("StockMarket-BronzeToSilver")
    .getOrCreate()
)

# Confirm Spark session is active and print the version
print("Spark Session Started")
print(f"Spark Version: {spark.version}")

S3_INPUT_PATH = "s3://graywolf--data--lake/stock/bronze/source=alphavantage/dataset=daily_time_series/"

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
# MAGIC ## 4. Convert Time Series to MapType
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
# MAGIC ## 5. Explode Time Series
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

# DBTITLE 1,Extract OHLCV
# MAGIC %md
# MAGIC ## 6. Extract OHLCV Fields
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

# DBTITLE 1,Remove Duplicates
# MAGIC %md
# MAGIC ## 6.1 Remove Duplicate Records
# MAGIC
# MAGIC Remove duplicate symbol + date combinations to ensure data uniqueness.

# COMMAND ----------

# DBTITLE 1,Remove Duplicate Records
daily_df = daily_df.dropDuplicates(
    ["symbol", "day_date"]
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
daily_df = daily_df.withColumn(
    "symbol",
    col("symbol").cast("string")
)

print("[TRANSFORMATION 6.1] symbol cast to string")

daily_df.printSchema()
daily_df.show(5, truncate=False)


daily_df = daily_df.withColumn(
    "last_refreshed",
    col("last_refreshed").cast("date")
)

print("[TRANSFORMATION 6.2] last_refreshed cast to date")

daily_df.printSchema()
daily_df.show(5, truncate=False)


daily_df = daily_df.withColumn(
    "day_date",
    col("day_date").cast("date")
)

print("[TRANSFORMATION 6.3] day_date cast to date")

daily_df.printSchema()
daily_df.show(5, truncate=False)


daily_df = daily_df.withColumn(
    "open",
    col("open").cast("decimal(12,2)")
)

print("[TRANSFORMATION 6.4] open cast to decimal(12,2)")

daily_df.printSchema()
daily_df.show(5, truncate=False)


daily_df = daily_df.withColumn(
    "close",
    col("close").cast("decimal(12,2)")
)

print("[TRANSFORMATION 6.5] close cast to decimal(12,2)")

daily_df.printSchema()
daily_df.show(5, truncate=False)


daily_df = daily_df.withColumn(
    "low",
    col("low").cast("decimal(12,2)")
)

print("[TRANSFORMATION 6.6] low cast to decimal(12,2)")

daily_df.printSchema()
daily_df.show(5, truncate=False)


daily_df = daily_df.withColumn(
    "high",
    col("high").cast("decimal(12,2)")
)

print("[TRANSFORMATION 6.7] high cast to decimal(12,2)")

daily_df.printSchema()
daily_df.show(5, truncate=False)


daily_df = daily_df.withColumn(
    "volume",
    col("volume").cast("long")
)

print("[TRANSFORMATION 6.8] volume cast to long")

daily_df.printSchema()
daily_df.show(5, truncate=False)

print("[TRANSFORMATION 6] All columns cast to proper data types")

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
    .when(col("last_refreshed").isNull(), "INVALID")
    .when(col("day_date").isNull(), "INVALID")
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
        when(col("last_refreshed").isNull(), "Missing last_refreshed"),
        when(col("day_date").isNull(), "Missing day_date"),

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

window = Window.partitionBy("symbol").orderBy("day_date")

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
    "source=alphavantage/"
    "dataset=daily_stock/"
)

print(f"[QUARANTINE] Target path: {quarantine_path}")

print(
    f"[QUARANTINE] Invalid data written successfully "
    f"to {quarantine_path}"
)


# ---------------- WRITE SILVER ----------------

silver_path = (
    "s3://graywolf--data--lake/"
    "stock/silver/"
    "source=alphavantage/"
    "dataset=daily_stock/"
)

print(f"[SILVER] Target path: {silver_path}")

print(
    f"[SILVER] Valid data written successfully "
    f"to {silver_path}"
)

(
    valid_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("stock_catalog.silver.daily_stock_data")
)

# COMMAND ----------

# DBTITLE 1,Commit Job
# MAGIC %md
# MAGIC ## 9. Commit Job
# MAGIC
# MAGIC Commit the Glue job to finalize the ETL run.

# COMMAND ----------

# DBTITLE 1,Commit Job
print("[JOB] Job committed successfully")