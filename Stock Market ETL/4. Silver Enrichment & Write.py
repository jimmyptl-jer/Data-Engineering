# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Step 4 — Silver Enrichment & Write
# MAGIC
# MAGIC This notebook reads the validated records from Step 3, enriches them with derived fields (daily change, market movement, previous day close/open via window functions), and writes the final enriched dataset to the silver Delta table.

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
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("StockMarket-SilverEnrichment")
    .getOrCreate()
)

print("Spark Session Started")
print(f"Spark Version: {spark.version}")

INPUT_TABLE = "stock_catalog.silver.daily_stock_valid"
OUTPUT_TABLE = "stock_catalog.silver.daily_stock_data"

# COMMAND ----------

# DBTITLE 1,Read Validated Data
# MAGIC %md
# MAGIC ## Read Validated Data
# MAGIC
# MAGIC Read the valid records from the silver Delta table created in Step 3.

# COMMAND ----------

# DBTITLE 1,Read Validated Data
valid_df = spark.table(INPUT_TABLE)

print(f"[SILVER] Valid data loaded from {INPUT_TABLE}")
valid_df.printSchema()
display(valid_df)

# COMMAND ----------

# DBTITLE 1,Add Daily Change
# MAGIC %md
# MAGIC ## Add Daily Change
# MAGIC
# MAGIC Calculate the daily price change (close minus open) for valid records.

# COMMAND ----------

# DBTITLE 1,Add Daily Change
valid_df = valid_df.withColumn(
    "daily_change",
    col("close") - col("open")
)

print("[TRANSFORMATION 1] daily_change column added")
valid_df.printSchema()
valid_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Add Market Movement
# MAGIC %md
# MAGIC ## Add Market Movement & Window Functions
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

print("[TRANSFORMATION 2] market_movement column added")

w = Window.partitionBy("symbol").orderBy("day_date")

valid_df = valid_df.withColumn(
    "previous_day_close",
    lag("close").over(w)
)

valid_df = valid_df.withColumn(
    "previous_day_open",
    lag("open").over(w)
)

print("[TRANSFORMATION 3] Window function columns added")
valid_df.printSchema()
valid_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Final Record Count
# MAGIC %md
# MAGIC ## Final Record Count
# MAGIC
# MAGIC Count and display the number of enriched silver records.

# COMMAND ----------

# DBTITLE 1,Final Record Count
silver_count = valid_df.count()
print(f"[SILVER] Enriched records: {silver_count}")

# COMMAND ----------

# DBTITLE 1,Write Silver Table
# MAGIC %md
# MAGIC ## Write Final Silver Table
# MAGIC
# MAGIC Write the enriched data to the final silver Delta table.

# COMMAND ----------

# DBTITLE 1,Write Final Silver Table
(
    valid_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(OUTPUT_TABLE)
)

print(f"[SILVER] Enriched data written to {OUTPUT_TABLE}")
print("[JOB] Pipeline completed successfully")