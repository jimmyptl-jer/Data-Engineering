# Databricks notebook source
# ============================================================

# STEP 1: Create Spark Session

# ============================================================

from pyspark.sql import SparkSession

print("\n" + "=" * 60)
print("📌 STEP 1: Initializing Spark Session")
print("=" * 60)

spark = SparkSession.builder.appName("HealthDataProcessing").getOrCreate()

print("✅ Spark Session created successfully")

# ============================================================

# STEP 2: Read CSV File (Initial Exploration Phase)

# ============================================================

print("\n" + "=" * 60)
print("📌 STEP 2: Reading CSV file with inferred schema")
print("=" * 60)

df = (
spark.read
.format("csv")
.option("header", "true")
.option("inferSchema", "true")   # fixed casing
.load("/Volumes/workspace/default/data_engineering/Teen_Mental_Health_Dataset.csv")
)

print("✅ CSV file loaded successfully")

# ============================================================

# STEP 3: Schema Inspection

# ============================================================

print("\n" + "=" * 60)
print("📌 STEP 3: Printing Schema")
print("=" * 60)

df.printSchema()

# ============================================================

# STEP 4: Column List

# ============================================================

print("\n" + "=" * 60)
print("📌 STEP 4: Listing All Columns")
print("=" * 60)

columns = df.columns
print(f"✅ Total Columns: {len(columns)}")
print("🔹 Column Names:")
print(df.columns)

print(df.dtypes)

# ============================================================

# STEP 5: Data Preview

# ============================================================

print("\n" + "=" * 60)
print("📌 STEP 5: Displaying Data Preview")
print("=" * 60)

display(df)
