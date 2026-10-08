# Databricks notebook source
# MAGIC %md
# MAGIC   In PySpark, handling malformed data depends on the source (CSV, JSON, Parquet, etc.), but the most common approaches are:
# MAGIC
# MAGIC ## 1. Using `mode` while reading data
# MAGIC
# MAGIC ### CSV Example
# MAGIC
# MAGIC ```python
# MAGIC df = spark.read \
# MAGIC     .option("header", "true") \
# MAGIC     .option("mode", "PERMISSIVE") \
# MAGIC     .csv("data.csv")
# MAGIC ```
# MAGIC
# MAGIC Available modes:
# MAGIC
# MAGIC | Mode          | Description                                                      |
# MAGIC | ------------- | ---------------------------------------------------------------- |
# MAGIC | PERMISSIVE    | Default. Keeps malformed records and sets invalid fields to null |
# MAGIC | DROPMALFORMED | Drops malformed rows completely                                  |
# MAGIC | FAILFAST      | Throws an exception immediately                                  |
# MAGIC
# MAGIC Example:
# MAGIC
# MAGIC ```python
# MAGIC df = spark.read \
# MAGIC     .option("header", "true") \
# MAGIC     .option("mode", "DROPMALFORMED") \
# MAGIC     .csv("data.csv")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Capture malformed records
# MAGIC
# MAGIC Create a column to store bad rows:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.types import *
# MAGIC
# MAGIC schema = StructType([
# MAGIC     StructField("id", IntegerType(), True),
# MAGIC     StructField("name", StringType(), True),
# MAGIC     StructField("salary", DoubleType(), True)
# MAGIC ])
# MAGIC
# MAGIC df = spark.read \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode", "PERMISSIVE") \
# MAGIC     .option("columnNameOfCorruptRecord", "_corrupt_record") \
# MAGIC     .csv("data.csv")
# MAGIC ```
# MAGIC
# MAGIC Example malformed row:
# MAGIC
# MAGIC ```csv
# MAGIC 1,John,50000
# MAGIC 2,Mary,abc
# MAGIC ```
# MAGIC
# MAGIC Output:
# MAGIC
# MAGIC | id | name | salary | _corrupt_record |
# MAGIC | -- | ---- | ------ | --------------- |
# MAGIC | 1  | John | 50000  | null            |
# MAGIC | 2  | Mary | null   | 2,Mary,abc      |
# MAGIC
# MAGIC View bad records:
# MAGIC
# MAGIC ```python
# MAGIC bad_records = df.filter("_corrupt_record IS NOT NULL")
# MAGIC bad_records.show(truncate=False)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. Data validation after loading
# MAGIC
# MAGIC Suppose salary should be numeric and positive:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC valid_df = df.filter(
# MAGIC     (col("salary").isNotNull()) &
# MAGIC     (col("salary") > 0)
# MAGIC )
# MAGIC
# MAGIC invalid_df = df.subtract(valid_df)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. Safe casting
# MAGIC
# MAGIC When source columns arrive as strings:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC df = df.withColumn(
# MAGIC     "salary",
# MAGIC     col("salary").cast("double")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC Failed casts become `null`.
# MAGIC
# MAGIC Identify them:
# MAGIC
# MAGIC ```python
# MAGIC invalid_rows = df.filter(col("salary").isNull())
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. JSON malformed records
# MAGIC
# MAGIC ```python
# MAGIC df = spark.read \
# MAGIC     .option("mode", "PERMISSIVE") \
# MAGIC     .option("columnNameOfCorruptRecord", "_corrupt_record") \
# MAGIC     .json("data.json")
# MAGIC ```
# MAGIC
# MAGIC Then:
# MAGIC
# MAGIC ```python
# MAGIC df.filter("_corrupt_record IS NOT NULL").show(truncate=False)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Production Pattern
# MAGIC
# MAGIC A common ETL approach is:
# MAGIC
# MAGIC ```python
# MAGIC raw_df = spark.read \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode", "PERMISSIVE") \
# MAGIC     .option("columnNameOfCorruptRecord", "_corrupt_record") \
# MAGIC     .csv(input_path)
# MAGIC
# MAGIC good_records = raw_df.filter("_corrupt_record IS NULL")
# MAGIC bad_records = raw_df.filter("_corrupt_record IS NOT NULL")
# MAGIC
# MAGIC good_records.write.mode("append").parquet(good_path)
# MAGIC bad_records.write.mode("append").parquet(error_path)
# MAGIC ```
# MAGIC
# MAGIC This allows processing valid data while storing malformed records separately for investigation instead of failing the entire job.
# MAGIC