# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Step 2 — Data Cleaning
# MAGIC
# MAGIC This notebook reads the extracted data from Step 1, removes duplicate records, casts columns to proper data types, standardizes stock symbols to uppercase, and verifies numeric column types. The cleaned data is written to an intermediate Delta table for validation.

# COMMAND ----------

# DBTITLE 1,Imports & Setup
# MAGIC %md
# MAGIC ## Imports & Spark Session Setup

# COMMAND ----------

# DBTITLE 1,Imports & Spark Session
from pyspark.sql.functions import col, upper
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("StockMarket-DataCleaning")
    .getOrCreate()
)

print("Spark Session Started")
print(f"Spark Version: {spark.version}")

INPUT_TABLE = "stock_catalog.bronze.daily_stock_extracted"
OUTPUT_TABLE = "stock_catalog.bronze.daily_stock_cleaned"

# COMMAND ----------

# DBTITLE 1,Read Extracted Data
# MAGIC %md
# MAGIC ## Read Extracted Data
# MAGIC
# MAGIC Read the extracted data from the intermediate Delta table created in Step 1.

# COMMAND ----------

# DBTITLE 1,Read Extracted Data
daily_df = spark.table(INPUT_TABLE)

print(f"[CLEAN] Data loaded from {INPUT_TABLE}")
daily_df.printSchema()
display(daily_df)

# COMMAND ----------

# DBTITLE 1,Remove Duplicates
# MAGIC %md
# MAGIC ## Remove Duplicate Records
# MAGIC
# MAGIC Remove duplicate symbol + date combinations to ensure data uniqueness.

# COMMAND ----------

# DBTITLE 1,Remove Duplicate Records
daily_df = daily_df.dropDuplicates(["symbol", "day_date"])

print("[TRANSFORMATION 1] Duplicate records removed")
daily_df.printSchema()
daily_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Cast Columns
# MAGIC %md
# MAGIC ## Cast Columns to Proper Data Types
# MAGIC
# MAGIC Cast symbol to string, dates to date type, prices to decimal(12,2), and volume to long.

# COMMAND ----------

# DBTITLE 1,Cast Columns to Proper Data Types
daily_df = daily_df.withColumn("symbol", col("symbol").cast("string"))
print("[TRANSFORMATION 2.1] symbol cast to string")

daily_df = daily_df.withColumn("last_refreshed", col("last_refreshed").cast("date"))
print("[TRANSFORMATION 2.2] last_refreshed cast to date")

daily_df = daily_df.withColumn("day_date", col("day_date").cast("date"))
print("[TRANSFORMATION 2.3] day_date cast to date")

daily_df = daily_df.withColumn("open", col("open").cast("decimal(12,2)"))
print("[TRANSFORMATION 2.4] open cast to decimal(12,2)")

daily_df = daily_df.withColumn("close", col("close").cast("decimal(12,2)"))
print("[TRANSFORMATION 2.5] close cast to decimal(12,2)")

daily_df = daily_df.withColumn("low", col("low").cast("decimal(12,2)"))
print("[TRANSFORMATION 2.6] low cast to decimal(12,2)")

daily_df = daily_df.withColumn("high", col("high").cast("decimal(12,2)"))
print("[TRANSFORMATION 2.7] high cast to decimal(12,2)")

daily_df = daily_df.withColumn("volume", col("volume").cast("long"))
print("[TRANSFORMATION 2.8] volume cast to long")

print("[TRANSFORMATION 2] All columns cast to proper data types")
daily_df.printSchema()
daily_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Standardize Symbol
# MAGIC %md
# MAGIC ## Standardize Symbol
# MAGIC
# MAGIC Convert stock symbols to uppercase for consistency.

# COMMAND ----------

# DBTITLE 1,Standardize Symbol
daily_df = daily_df.withColumn("symbol", upper(col("symbol")))

print("[TRANSFORMATION 3] Symbol standardized to uppercase")
daily_df.printSchema()
daily_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Verify Numeric Types
# MAGIC %md
# MAGIC ## Verify Numeric Column Types
# MAGIC
# MAGIC With prices as `decimal(12,2)` and volume as `long`, rounding is no longer needed — this cell verifies the column types.

# COMMAND ----------

# DBTITLE 1,Verify Numeric Column Types
print("[VERIFICATION] Numeric column types (no rounding needed with decimal(12,2))")
daily_df.printSchema()
daily_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Write Cleaned Table
# MAGIC %md
# MAGIC ## Write to Cleaned Delta Table
# MAGIC
# MAGIC Write the cleaned data to an intermediate Delta table for the validation step.

# COMMAND ----------

# DBTITLE 1,Write to Cleaned Delta Table
(
    daily_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(OUTPUT_TABLE)
)

print(f"[CLEAN] Cleaned data written to {OUTPUT_TABLE}")
print(f"[CLEAN] Row count: {daily_df.count()}")