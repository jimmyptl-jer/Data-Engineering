# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Step 3 — Data Validation
# MAGIC
# MAGIC This notebook reads the cleaned data from Step 2, applies data quality validation rules (null checks and positive value checks), adds a human-readable validation reason, splits records into valid and invalid DataFrames, and writes them to separate Delta tables.

# COMMAND ----------

# DBTITLE 1,Imports & Setup
# MAGIC %md
# MAGIC ## Imports & Spark Session Setup

# COMMAND ----------

# DBTITLE 1,Imports & Spark Session
from pyspark.sql.functions import col, when, concat_ws
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("StockMarket-DataValidation")
    .getOrCreate()
)

print("Spark Session Started")
print(f"Spark Version: {spark.version}")

INPUT_TABLE = "stock_catalog.bronze.daily_stock_cleaned"
VALID_TABLE = "stock_catalog.silver.daily_stock_valid"
INVALID_TABLE = "stock_catalog.quarantine.daily_stock_invalid"

# COMMAND ----------

# DBTITLE 1,Read Cleaned Data
# MAGIC %md
# MAGIC ## Read Cleaned Data
# MAGIC
# MAGIC Read the cleaned data from the intermediate Delta table created in Step 2.

# COMMAND ----------

# DBTITLE 1,Read Cleaned Data
daily_df = spark.table(INPUT_TABLE)

print(f"[VALIDATION] Data loaded from {INPUT_TABLE}")
daily_df.printSchema()
display(daily_df)

# COMMAND ----------

# DBTITLE 1,Validation Status
# MAGIC %md
# MAGIC ## Validation Status
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

print("[TRANSFORMATION 1] Validation status added")
daily_df.printSchema()
daily_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Validation Reason
# MAGIC %md
# MAGIC ## Validation Reason
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

print("[TRANSFORMATION 2] Validation reason added")
daily_df.printSchema()
daily_df.show(20, truncate=False)

# COMMAND ----------

# DBTITLE 1,Split Valid / Invalid
# MAGIC %md
# MAGIC ## Split Valid / Invalid Records
# MAGIC
# MAGIC Filter the dataset into separate valid and invalid DataFrames based on validation status.

# COMMAND ----------

# DBTITLE 1,Split Valid / Invalid Records
valid_df = daily_df.filter(col("validation_status") == "VALID")
print("[SPLIT] Filtered VALID records into valid_df")
valid_df.printSchema()
valid_df.show(5, truncate=False)

invalid_df = daily_df.filter(col("validation_status") == "INVALID")
print("[SPLIT] Filtered INVALID records into invalid_df")
invalid_df.printSchema()
invalid_df.show(20, truncate=False)

# COMMAND ----------

# DBTITLE 1,Record Counts
# MAGIC %md
# MAGIC ## Record Counts
# MAGIC
# MAGIC Count and display the number of valid and invalid records.

# COMMAND ----------

# DBTITLE 1,Record Counts
valid_count = valid_df.count()
print(f"[SILVER] Valid records     : {valid_count}")

invalid_count = invalid_df.count()
print(f"[QUARANTINE] Invalid records: {invalid_count}")

# COMMAND ----------

# DBTITLE 1,Write Valid & Invalid Tables
# MAGIC %md
# MAGIC ## Write Valid & Invalid Delta Tables
# MAGIC
# MAGIC Write valid records to the silver layer and invalid records to the quarantine layer as separate Delta tables.

# COMMAND ----------

# DBTITLE 1,Write Valid & Invalid Delta Tables
# Write valid records to silver
(
    valid_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(VALID_TABLE)
)
print(f"[SILVER] Valid data written to {VALID_TABLE}")

spark.sql("create schema if not exists stock_catalog.quarantine")

# Write invalid records to quarantine
(
    invalid_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(INVALID_TABLE)
)
print(f"[QUARANTINE] Invalid data written to {INVALID_TABLE}")