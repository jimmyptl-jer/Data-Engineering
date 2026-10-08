# Databricks notebook source
# DBTITLE 1,Imports & Spark Session
# ============================================================
# 1. IMPORT REQUIRED LIBRARIES
# ============================================================
# Import SparkSession for creating the Spark entry point
from pyspark.sql import SparkSession
# Import common DataFrame functions for transformations
from pyspark.sql.functions import (
    col,
    explode,
    from_json,
    lit,
    current_timestamp
)
# Import Spark SQL types for schema definitions
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    MapType
)

# Import all functions as F for convenience (e.g., F.current_timestamp)
from pyspark.sql import functions as F

# ============================================================
# 2. CREATE SPARK SESSION
# ============================================================
# Initialize a SparkSession with a descriptive app name
spark = (
    SparkSession.builder
    .appName("StockMarket-BronzeToSilver")
    .getOrCreate()
)

# Confirm Spark session is active and print the version
print("Spark Session Started")
print(f"Spark Version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,Configuration & Read Raw Data from S3
# ============================================================
# 3. CONFIGURATION — Define S3 source paths
# ============================================================
# S3 path for raw Alpha Vantage JSON files (nested by source and dataset)
S3_INPUT_PATH = "s3://graywolf--data--lake/stock/bronze/source=alphavantage/dataset=daily_time_series/"

# S3 path for the stock market CSV file
S3_INPUT_CSV_FILE = "s3://graywolf--data--lake/stock/stock_market.csv"

# ============================================================
# 4. READ RAW DATA FROM S3
# ============================================================
# Read multi-line JSON files recursively from the Alpha Vantage S3 path
raw_df = (
    spark.read
    .format("json")
    .option("multiLine", "true")
    .option("recursiveFileLookup", "true")
    .load(S3_INPUT_PATH)
)

# Read the stock market CSV file with header and automatic schema inference
stock_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(S3_INPUT_CSV_FILE)
)


# COMMAND ----------

# DBTITLE 1,Inspect Stock Data Schema
# Print the schema of the stock market CSV DataFrame to inspect column names and types
# raw_df.printSchema()
stock_df.printSchema()
# bronze_df = stock_df

# COMMAND ----------

# DBTITLE 1,Add Ingestion Timestamp (Bronze Layer)
# ============================================================
# 5. BUILD BRONZE LAYER — Add metadata column to raw stock data
# ============================================================
# Add an ingestion timestamp column to track when each record was loaded
bronze_df = (
    stock_df
    .withColumn(
        "_ingestion_timestamp",
        F.current_timestamp()
    )
)

# COMMAND ----------

# DBTITLE 1,Inspect Bronze DataFrame Schema
# Print the schema of the bronze DataFrame to verify the ingestion timestamp column was added
bronze_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Write Bronze Data to Delta Table
# ============================================================
# 6. WRITE BRONZE DATA TO DELTA TABLE
# ============================================================
# Persist the bronze DataFrame as a Delta table in Unity Catalog
# Using overwrite mode with schema overwrite enabled for full refresh
(
    bronze_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("stock_catalog.bronze.raw_stock_table")
)

# COMMAND ----------

# DBTITLE 1,Query Bronze Stock Data
# MAGIC %sql
# MAGIC -- Query the bronze stock table to preview the first 10 rows (including _ingestion_timestamp)
# MAGIC SELECT *
# MAGIC FROM stock_catalog.bronze.raw_stock_table
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Display Raw Data & Record Count

table_name = "stock_catalog.bronze.raw_stock_table"

print("Before:", spark.table(table_name).count())   # expect 500


# COMMAND ----------

