# Databricks notebook source
# DBTITLE 1,Load Bronze Data from Unity Catalog
# ============================================================
# 1. LOAD BRONZE DATA FROM UNITY CATALOG
# ============================================================
# Import Spark SQL functions for transformations
from pyspark.sql import functions as F

from pyspark.sql.functions import decimal

# Read the bronze stock table from Unity Catalog
bronze_df = spark.table(
    "stock_catalog.bronze.raw_stock_table"
)

# Preview the first 10 rows and print the schema for inspection
bronze_df.show(10)
bronze_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Select & Cast Columns for Silver Layer
# ============================================================
# 2. SELECT & CAST COLUMNS FOR SILVER LAYER
# ============================================================
# Select relevant columns from bronze and cast to proper types:
#   - trade_date: convert to date type
#   - price columns: cast to decimal(18,4) for precision
#   - volume: cast to long for large integer support
#   - Preserve _ingestion_timestamp for lineage tracking
silver_df = bronze_df.select(
    F.col("symbol"),
    F.col("company_name"),
    F.col("exchange"),
    F.to_date("trade_date").alias("trade_date"),
    F.col("open_price").cast("decimal(18,4)").alias("open_price"),
    F.col("high_price").cast("decimal(18,4)").alias("high_price"),
    F.col("low_price").cast("decimal(18,4)").alias("low_price"),
    F.col("close_price").cast("decimal(18,4)").alias("close_price"),
    F.col("volume").cast("long").alias("volume"),
    F.col("source"),
    F.col("_ingestion_timestamp")
)

# COMMAND ----------

# DBTITLE 1,Normalize Symbol to Uppercase
# ============================================================
# 3. NORMALIZE SYMBOL COLUMN
# ============================================================
# Convert symbol to uppercase for consistent formatting (e.g., "aapl" → "AAPL")
silver_df  = silver_df.withColumn("symbol", F.upper(F.col("symbol")))


# COMMAND ----------

# DBTITLE 1,Add Validation Status (VALID/INVALID)
# ============================================================
# 4. DATA QUALITY VALIDATION — Flag each row as VALID or INVALID
# ============================================================
# Check all critical columns for nulls; if any is null, mark the row "INVALID"
# Otherwise mark it "VALID"
silver_df = silver_df.withColumn(
    "validation_status",
    F.when(F.col("symbol").isNull(), "INVALID")
    .when(F.col("company_name").isNull(), "INVALID")
    .when(F.col("exchange").isNull(), "INVALID")
    .when(F.col("trade_date").isNull(), "INVALID")
    .when(F.col("open_price").isNull(), "INVALID")
    .when(F.col("high_price").isNull(), "INVALID")
    .when(F.col("low_price").isNull(), "INVALID")
    .when(F.col("close_price").isNull(), "INVALID")
    .when(F.col("volume").isNull(), "INVALID")
    .when(F.col("source").isNull(), "INVALID")
    .otherwise("VALID")
)

# COMMAND ----------

# DBTITLE 1,Add Validation Message (Error Details)
# ============================================================
# 5. DATA QUALITY VALIDATION — Build detailed error messages
# ============================================================
# Concatenate all null-field names into a single comma-separated message
# This gives context on WHY a row was marked INVALID (e.g., "symbol is null, volume is null")
silver_df = silver_df.withColumn(
    "validation_message",
    F.concat_ws(
        ", ",
        F.when(F.col("symbol").isNull(), F.lit("symbol is null")),
        F.when(F.col("company_name").isNull(), F.lit("company_name is null")),
        F.when(F.col("exchange").isNull(), F.lit("exchange is null")),
        F.when(F.col("trade_date").isNull(), F.lit("trade_date is null")),
        F.when(F.col("open_price").isNull(), F.lit("open_price is null")),
        F.when(F.col("high_price").isNull(), F.lit("high_price is null")),
        F.when(F.col("low_price").isNull(), F.lit("low_price is null")),
        F.when(F.col("close_price").isNull(), F.lit("close_price is null")),
        F.when(F.col("volume").isNull(), F.lit("volume is null")),
        F.when(F.col("source").isNull(), F.lit("source is null")),
    )
)

# COMMAND ----------

# DBTITLE 1,Calculate Daily Price Change
# ============================================================
# 6. CALCULATE DAILY PRICE CHANGE
# ============================================================
# Compute the difference between open and close prices, rounded to 2 decimal places
# Positive value = price dropped (open > close); Negative value = price rose (open < close)
silver_df = silver_df.withColumn(
    "daily_change",
    F.round(F.col("open_price") - F.col("close_price"), 2)
)

# COMMAND ----------

# DBTITLE 1,Classify Market Movement (Bull/Bear/Neutral)
# ============================================================
# 7. CLASSIFY MARKET MOVEMENT DIRECTION
# ============================================================
# Categorize the day's price movement based on open vs close:
#   - "bull"  → close < open  (price fell, bearish market)
#   - "bear"  → close > open  (price rose, bullish market)
#   - "neutral" → close == open (no change)
# Note: Labels appear reversed — verify against your business convention
silver_df = silver_df.withColumn(
    "market_movement",
    F.when(F.col("close_price") > F.col("open_price"), "bear")
    .when(F.col("close_price") < F.col("open_price"), "bull")
    .otherwise("neutral")
)

# COMMAND ----------

# DBTITLE 1,Inspect Silver DataFrame Schema
# Print the final silver DataFrame schema to verify all transformations and new columns
silver_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Display Silver DataFrame
# Display the fully transformed silver DataFrame for visual inspection
display(silver_df)

# COMMAND ----------

# DBTITLE 1,Write Silver Data to Delta Table
# ============================================================
# 8. WRITE SILVER DATA TO DELTA TABLE IN UNITY CATALOG
# ============================================================
# Persist the transformed silver DataFrame as a Delta table
# Using overwrite mode with schema overwrite enabled for full refresh
(
    silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("stock_catalog.silver.stock_data")
)