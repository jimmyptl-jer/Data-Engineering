# Databricks notebook source
# ============================================================
# STEP 1: Create Spark Session
# ============================================================

from pyspark.sql import SparkSession 

print("\n" + "="*60)
print("📌 STEP 1: Initializing Spark Session")
print("="*60)

spark = SparkSession.builder \
    .appName("CustomerDataProcessing") \
    .getOrCreate()


# ============================================================
# STEP 2: Read CSV File (Initial Exploration Phase)
# ============================================================

print("\n" + "="*60)
print("📌 STEP 2: Reading CSV file with inferred schema")
print("="*60)

df = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/workspace/default/data_engineering/WA_Fn-UseC_-Telco-Customer-Churn.csv")
)

print("\n🔹 Raw Data Preview")
display(df)

print("\n🔹 Inferred Schema")
df.printSchema()


# ============================================================
# STEP 3: Import Required Functions and Types
# ============================================================

print("\n" + "="*60)
print("📌 STEP 3: Importing required functions and data types")
print("="*60)

from pyspark.sql.functions import *
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)


# ============================================================
# STEP 4: Define Custom Schema (Production Approach)
# ============================================================

print("\n" + "="*60)
print("📌 STEP 4: Defining custom schema")
print("="*60)

schema = StructType([
    StructField("customerID",        StringType(),  True),
    StructField("gender",            StringType(),  True),
    StructField("SeniorCitizen",     IntegerType(), True),
    StructField("Partner",           StringType(),  True),
    StructField("Dependents",        StringType(),  True),
    StructField("tenure",            IntegerType(), True),
    StructField("PhoneService",      StringType(),  True),
    StructField("MultipleLines",     StringType(),  True),
    StructField("InternetService",   StringType(),  True),
    StructField("OnlineSecurity",    StringType(),  True),
    StructField("OnlineBackup",      StringType(),  True),
    StructField("DeviceProtection",  StringType(),  True),
    StructField("TechSupport",       StringType(),  True),
    StructField("StreamingTV",       StringType(),  True),
    StructField("StreamingMovies",   StringType(),  True),
    StructField("Contract",          StringType(),  True),
    StructField("PaperlessBilling",  StringType(),  True),
    StructField("PaymentMethod",     StringType(),  True),
    StructField("MonthlyCharges",    DoubleType(),  True),
    StructField("TotalCharges",      StringType(),  True),
    StructField("Churn",             StringType(),  True),
])


# ============================================================
# STEP 5: Read CSV Again Using Defined Schema
# ============================================================

print("\n" + "="*60)
print("📌 STEP 5: Reading CSV using custom schema")
print("="*60)

df = (
    spark.read
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load("/Volumes/workspace/default/data_engineering/WA_Fn-UseC_-Telco-Customer-Churn.csv")
)

print("\n🔹 Data After Applying Custom Schema")
display(df)

print("\n🔹 Schema After Applying Custom Schema")
df.printSchema()


# ============================================================
# STEP 6: Casting Columns to Boolean
# ============================================================

print("\n" + "="*60)
print("📌 STEP 6: Casting columns to Boolean type")
print("="*60)

df = df.withColumn("SeniorCitizen", col("SeniorCitizen").cast("boolean"))

df = df.withColumn("Partner",          when(col("Partner")          == "Yes", True).otherwise(False))
df = df.withColumn("Dependents",       when(col("Dependents")       == "Yes", True).otherwise(False))
df = df.withColumn("PhoneService",     when(col("PhoneService")     == "Yes", True).otherwise(False))
df = df.withColumn("OnlineSecurity",   when(col("OnlineSecurity")   == "Yes", True).otherwise(False))
df = df.withColumn("OnlineBackup",     when(col("OnlineBackup")     == "Yes", True).otherwise(False))
df = df.withColumn("DeviceProtection", when(col("DeviceProtection") == "Yes", True).otherwise(False))
df = df.withColumn("TechSupport",      when(col("TechSupport")      == "Yes", True).otherwise(False))
df = df.withColumn("StreamingTV",      when(col("StreamingTV")      == "Yes", True).otherwise(False))
df = df.withColumn("StreamingMovies",  when(col("StreamingMovies")  == "Yes", True).otherwise(False))
df = df.withColumn("PaperlessBilling", when(col("PaperlessBilling") == "Yes", True).otherwise(False))
df = df.withColumn("Churn",            when(col("Churn")            == "Yes", True).otherwise(False))

print("\n🔹 Data After Boolean Transformation")
display(df)


# ============================================================
# STEP 6.1: Add updated_at column
# ============================================================

print("\n" + "="*60)
print("📌 STEP 6.1: Adding updated_at column")
print("="*60)

df = df.withColumn("updated_at", current_timestamp())

print("\n🔹 Data After Adding updated_at")
display(df)


# ============================================================
# STEP 7: Final Output Verification
# ============================================================

print("\n" + "="*60)
print("📌 STEP 7: Final Schema After Transformations")
print("="*60)

df.printSchema()

print("\n🔹 Final Data Preview")
display(df)


# ============================================================
# STEP 8: Handle Missing Values
# ============================================================

print("\n" + "="*60)
print("📌 STEP 8: Handling Missing Values")
print("="*60)

df = df.fillna({"MultipleLines": "No"})

print("\n🔹 Data After fillna")
display(df)


# ============================================================
# STEP 9: Print Columns
# ============================================================

print("\n" + "="*60)
print("📌 STEP 9: Listing All Columns")
print("="*60)

for i, col_name in enumerate(df.columns):
    print(f"{i}: {col_name}")

# COMMAND ----------



