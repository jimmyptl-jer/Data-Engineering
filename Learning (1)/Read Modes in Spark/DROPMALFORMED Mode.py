# Databricks notebook source
# MAGIC %md
# MAGIC # DROPMALFORMED Mode — Complete Deep Dive
# MAGIC
# MAGIC ## What is DROPMALFORMED Mode?
# MAGIC
# MAGIC DROPMALFORMED is Spark's **silent cleaner** read mode. The name describes exactly what it does — it **drops** any row that is **malformed** (does not match the defined schema) and silently continues processing the remaining rows.
# MAGIC
# MAGIC No exceptions thrown. No nulls created. No corrupt record column. Bad rows simply vanish as if they never existed, and the job completes successfully with only the clean rows.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The Philosophy Behind DROPMALFORMED
# MAGIC
# MAGIC DROPMALFORMED exists for situations where the approach is:
# MAGIC
# MAGIC ```
# MAGIC "I know some rows will be bad.
# MAGIC I do not care about those rows.
# MAGIC I do not want the job to fail.
# MAGIC Just give me the clean rows and move on."
# MAGIC ```
# MAGIC
# MAGIC It is the middle ground between PERMISSIVE and FAILFAST:
# MAGIC
# MAGIC ```
# MAGIC PERMISSIVE     → Keep everything, convert bad fields to null
# MAGIC DROPMALFORMED  → Keep only good rows, silently remove bad ones
# MAGIC FAILFAST       → Stop immediately on first bad row
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Setup — Creating Test Data
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # CREATE TEST FILES
# MAGIC # ============================================================
# MAGIC
# MAGIC # Mix of good and bad rows
# MAGIC mixed_data = """emp_id,name,department,salary,age,join_date,is_active
# MAGIC 1,Alice,Engineering,95000.0,32,2019-03-15,true
# MAGIC 2,Bob,Marketing,INVALID_SALARY,26,2021-07-22,true
# MAGIC 3,Charlie,Engineering,120000.0,38,2017-01-10,true
# MAGIC NOT_A_NUMBER,Diana,HR,75000.0,35,2018-11-05,true
# MAGIC 5,Edward,Finance,88000.0,WRONG_AGE,2019-09-30,true
# MAGIC 6,Fiona,Engineering,92000.0,31,BAD_DATE,false
# MAGIC 7,George,Marketing,105000.0,42,2016-06-14,WRONG_BOOLEAN
# MAGIC 8,Hannah,HR,INVALID,ALSO_INVALID,ALSO_BAD,ALSO_WRONG
# MAGIC 9,Ivan,Finance,115000.0,45,2015-12-01,false
# MAGIC 10,Julia,Engineering,98000.0,34,2018-08-19,true"""
# MAGIC
# MAGIC # Mostly bad data
# MAGIC mostly_bad = """emp_id,name,department,salary,age,join_date,is_active
# MAGIC BAD,Alice,Engineering,95000.0,32,2019-03-15,true
# MAGIC BAD,Bob,Marketing,52000.0,26,2021-07-22,true
# MAGIC 3,Charlie,Engineering,120000.0,38,2017-01-10,true
# MAGIC BAD,Diana,HR,75000.0,BAD,2018-11-05,true
# MAGIC BAD,Edward,Finance,BAD,33,2019-09-30,true"""
# MAGIC
# MAGIC # All bad data
# MAGIC all_bad = """emp_id,name,department,salary,age,join_date,is_active
# MAGIC BAD,Alice,Engineering,BAD,BAD,BAD,BAD
# MAGIC BAD,Bob,Marketing,BAD,BAD,BAD,BAD
# MAGIC BAD,Charlie,Engineering,BAD,BAD,BAD,BAD"""
# MAGIC
# MAGIC dbutils.fs.put("/FileStore/test/mixed_employees.csv",
# MAGIC                mixed_data, overwrite=True)
# MAGIC dbutils.fs.put("/FileStore/test/mostly_bad.csv",
# MAGIC                mostly_bad, overwrite=True)
# MAGIC dbutils.fs.put("/FileStore/test/all_bad.csv",
# MAGIC                all_bad, overwrite=True)
# MAGIC
# MAGIC print("Test files created")
# MAGIC print("  mixed_employees.csv — mix of good and bad rows")
# MAGIC print("  mostly_bad.csv      — mostly bad rows")
# MAGIC print("  all_bad.csv         — every row is bad")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Schema
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.types import (
# MAGIC     StructType, StructField,
# MAGIC     IntegerType, StringType,
# MAGIC     DoubleType, BooleanType, DateType
# MAGIC )
# MAGIC from pyspark.sql.functions import (
# MAGIC     col, count, when, isnull,
# MAGIC     isnotnull, lit, current_timestamp
# MAGIC )
# MAGIC
# MAGIC schema = StructType([
# MAGIC     StructField("emp_id",     IntegerType(), True),
# MAGIC     StructField("name",       StringType(),  True),
# MAGIC     StructField("department", StringType(),  True),
# MAGIC     StructField("salary",     DoubleType(),  True),
# MAGIC     StructField("age",        IntegerType(), True),
# MAGIC     StructField("join_date",  DateType(),    True),
# MAGIC     StructField("is_active",  BooleanType(), True)
# MAGIC ])
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 1 — Basic DROPMALFORMED
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # BASIC DROPMALFORMED READ
# MAGIC # ============================================================
# MAGIC
# MAGIC df_drop = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode", "DROPMALFORMED") \
# MAGIC     .load("/FileStore/test/mixed_employees.csv")
# MAGIC
# MAGIC print("=== DROPMALFORMED MODE — RESULT ===")
# MAGIC df_drop.show(truncate=False)
# MAGIC print(f"Rows returned: {df_drop.count()}")
# MAGIC ```
# MAGIC
# MAGIC Output:
# MAGIC ```
# MAGIC +------+-------+-----------+--------+---+----------+---------+
# MAGIC |emp_id|name   |department |salary  |age|join_date |is_active|
# MAGIC +------+-------+-----------+--------+---+----------+---------+
# MAGIC |1     |Alice  |Engineering|95000.0 |32 |2019-03-15|true     |
# MAGIC |3     |Charlie|Engineering|120000.0|38 |2017-01-10|true     |
# MAGIC |9     |Ivan   |Finance    |115000.0|45 |2015-12-01|false    |
# MAGIC |10    |Julia  |Engineering|98000.0 |34 |2018-08-19|true     |
# MAGIC +------+-------+-----------+--------+---+----------+---------+
# MAGIC Rows returned: 4
# MAGIC ```
# MAGIC
# MAGIC Rows 2, 4, 5, 6, 7, 8 are completely gone. No nulls. No errors. Only perfectly clean rows remain.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 2 — What Exactly Gets Dropped
# MAGIC
# MAGIC Understanding exactly which rows get dropped and why:
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # UNDERSTAND WHAT GETS DROPPED
# MAGIC # ============================================================
# MAGIC
# MAGIC # Read same file with PERMISSIVE to see all rows
# MAGIC schema_with_corrupt = StructType(
# MAGIC     schema.fields + [
# MAGIC         StructField("_corrupt_record", StringType(), True)
# MAGIC     ]
# MAGIC )
# MAGIC
# MAGIC df_permissive = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema_with_corrupt) \
# MAGIC     .option("mode", "PERMISSIVE") \
# MAGIC     .option("columnNameOfCorruptRecord", "_corrupt_record") \
# MAGIC     .load("/FileStore/test/mixed_employees.csv")
# MAGIC
# MAGIC df_drop = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode", "DROPMALFORMED") \
# MAGIC     .load("/FileStore/test/mixed_employees.csv")
# MAGIC
# MAGIC total_rows   = df_permissive.count()
# MAGIC kept_rows    = df_drop.count()
# MAGIC dropped_rows = total_rows - kept_rows
# MAGIC
# MAGIC print("=" * 60)
# MAGIC print("     DROPMALFORMED — WHAT HAPPENED")
# MAGIC print("=" * 60)
# MAGIC print(f"  Total rows in file  : {total_rows}")
# MAGIC print(f"  Rows kept           : {kept_rows}")
# MAGIC print(f"  Rows dropped        : {dropped_rows}")
# MAGIC print(f"  Drop percentage     : {round(dropped_rows/total_rows*100,1)}%")
# MAGIC print("=" * 60)
# MAGIC
# MAGIC print("\n=== ALL ROWS (via PERMISSIVE) ===")
# MAGIC df_permissive.show(truncate=False)
# MAGIC
# MAGIC print("\n=== ROWS THAT WERE DROPPED ===")
# MAGIC # These are rows with corrupt record captured by PERMISSIVE
# MAGIC df_permissive \
# MAGIC     .filter(col("_corrupt_record").isNotNull()) \
# MAGIC     .select("_corrupt_record") \
# MAGIC     .show(truncate=False)
# MAGIC
# MAGIC print("\n=== ROWS THAT WERE KEPT (DROPMALFORMED) ===")
# MAGIC df_drop.show(truncate=False)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 3 — DROPMALFORMED on Edge Cases
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # EDGE CASE 1 — MOSTLY BAD DATA
# MAGIC # ============================================================
# MAGIC
# MAGIC print("=== MOSTLY BAD DATA ===")
# MAGIC
# MAGIC df_mostly_bad = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode", "DROPMALFORMED") \
# MAGIC     .load("/FileStore/test/mostly_bad.csv")
# MAGIC
# MAGIC print(f"Rows in file   : 5")
# MAGIC print(f"Rows returned  : {df_mostly_bad.count()}")
# MAGIC df_mostly_bad.show(truncate=False)
# MAGIC
# MAGIC # ============================================================
# MAGIC # EDGE CASE 2 — ALL BAD DATA
# MAGIC # ============================================================
# MAGIC
# MAGIC print("\n=== ALL BAD DATA ===")
# MAGIC
# MAGIC df_all_bad = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode", "DROPMALFORMED") \
# MAGIC     .load("/FileStore/test/all_bad.csv")
# MAGIC
# MAGIC print(f"Rows in file   : 3")
# MAGIC print(f"Rows returned  : {df_all_bad.count()}")
# MAGIC df_all_bad.show(truncate=False)
# MAGIC # Returns EMPTY DataFrame — no error thrown
# MAGIC # Job succeeds with zero rows
# MAGIC ```
# MAGIC
# MAGIC Output:
# MAGIC ```
# MAGIC === ALL BAD DATA ===
# MAGIC Rows in file   : 3
# MAGIC Rows returned  : 0
# MAGIC +------+----+----------+------+---+---------+---------+
# MAGIC |emp_id|name|department|salary|age|join_date|is_active|
# MAGIC +------+----+----------+------+---+---------+---------+
# MAGIC +------+----+----------+------+---+---------+---------+
# MAGIC ```
# MAGIC
# MAGIC This is the most dangerous behaviour of DROPMALFORMED — if every single row is bad, it returns an empty DataFrame with no error. The job succeeds. Nobody knows all data was dropped.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 4 — The Hidden Danger of DROPMALFORMED
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # THE SILENT DANGER — EMPTY DATAFRAME
# MAGIC # ============================================================
# MAGIC
# MAGIC # Scenario — wrong file sent by upstream
# MAGIC # All rows are bad because wrong format was used
# MAGIC # DROPMALFORMED silently returns empty DataFrame
# MAGIC
# MAGIC wrong_format = """product_id,product_name,category,price
# MAGIC P001,Laptop,Electronics,999.99
# MAGIC P002,Phone,Electronics,599.99
# MAGIC P003,Tablet,Electronics,399.99"""
# MAGIC
# MAGIC dbutils.fs.put("/FileStore/test/wrong_file.csv",
# MAGIC                wrong_format, overwrite=True)
# MAGIC
# MAGIC # Read wrong file with employee schema
# MAGIC df_wrong = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode", "DROPMALFORMED") \
# MAGIC     .load("/FileStore/test/wrong_file.csv")
# MAGIC
# MAGIC print("=== READING WRONG FILE WITH DROPMALFORMED ===")
# MAGIC print(f"Row count: {df_wrong.count()}")
# MAGIC df_wrong.show()
# MAGIC # Returns 0 rows — no error
# MAGIC # If you do not check row count you will never know
# MAGIC # The pipeline continues and produces wrong results silently
# MAGIC ```
# MAGIC
# MAGIC This is why DROPMALFORMED must always be paired with row count validation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 5 — Tracking Dropped Rows
# MAGIC
# MAGIC Since DROPMALFORMED gives you no visibility into what was dropped, you must build that tracking yourself:
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # TRACK DROPPED ROWS MANUALLY
# MAGIC # ============================================================
# MAGIC
# MAGIC def read_with_drop_tracking(
# MAGIC         path,
# MAGIC         schema,
# MAGIC         schema_with_corrupt,
# MAGIC         source_name
# MAGIC ):
# MAGIC     """
# MAGIC     Read with DROPMALFORMED but track what was dropped
# MAGIC     using PERMISSIVE on the side
# MAGIC     """
# MAGIC
# MAGIC     print(f"\n{'='*55}")
# MAGIC     print(f"  DROPMALFORMED READ: {source_name}")
# MAGIC     print(f"{'='*55}")
# MAGIC
# MAGIC     # Main read — DROPMALFORMED for clean data
# MAGIC     df_clean = spark.read \
# MAGIC         .format("csv") \
# MAGIC         .option("header", "true") \
# MAGIC         .schema(schema) \
# MAGIC         .option("mode", "DROPMALFORMED") \
# MAGIC         .load(path)
# MAGIC
# MAGIC     # Side read — PERMISSIVE to capture what was dropped
# MAGIC     df_all = spark.read \
# MAGIC         .format("csv") \
# MAGIC         .option("header", "true") \
# MAGIC         .schema(schema_with_corrupt) \
# MAGIC         .option("mode", "PERMISSIVE") \
# MAGIC         .option("columnNameOfCorruptRecord", "_corrupt_record") \
# MAGIC         .load(path)
# MAGIC
# MAGIC     total_rows   = df_all.count()
# MAGIC     clean_rows   = df_clean.count()
# MAGIC     dropped_rows = total_rows - clean_rows
# MAGIC     drop_pct     = round(dropped_rows / total_rows * 100, 2) \
# MAGIC                    if total_rows > 0 else 0
# MAGIC
# MAGIC     print(f"  Total rows in file : {total_rows:,}")
# MAGIC     print(f"  Clean rows kept    : {clean_rows:,}")
# MAGIC     print(f"  Rows dropped       : {dropped_rows:,}")
# MAGIC     print(f"  Drop percentage    : {drop_pct}%")
# MAGIC
# MAGIC     # Show dropped rows for logging
# MAGIC     if dropped_rows > 0:
# MAGIC         print(f"\n  Dropped rows:")
# MAGIC         df_all \
# MAGIC             .filter(col("_corrupt_record").isNotNull()) \
# MAGIC             .select("_corrupt_record") \
# MAGIC             .show(truncate=False)
# MAGIC
# MAGIC         # Save dropped rows to log location
# MAGIC         df_all \
# MAGIC             .filter(col("_corrupt_record").isNotNull()) \
# MAGIC             .select(
# MAGIC                 col("_corrupt_record").alias("raw_row"),
# MAGIC                 lit(source_name).alias("source"),
# MAGIC                 current_timestamp().alias("dropped_at")
# MAGIC             ) \
# MAGIC             .write \
# MAGIC             .mode("overwrite") \
# MAGIC             .format("delta") \
# MAGIC             .save(f"/FileStore/dropped_log/{source_name}/")
# MAGIC
# MAGIC         print(f"  Dropped rows saved to: "
# MAGIC               f"/FileStore/dropped_log/{source_name}/")
# MAGIC
# MAGIC     print(f"{'='*55}\n")
# MAGIC     return df_clean
# MAGIC
# MAGIC
# MAGIC # Schema with corrupt column
# MAGIC schema_with_corrupt = StructType(
# MAGIC     schema.fields + [
# MAGIC         StructField("_corrupt_record", StringType(), True)
# MAGIC     ]
# MAGIC )
# MAGIC
# MAGIC # Run
# MAGIC df_result = read_with_drop_tracking(
# MAGIC     path                = "/FileStore/test/mixed_employees.csv",
# MAGIC     schema              = schema,
# MAGIC     schema_with_corrupt = schema_with_corrupt,
# MAGIC     source_name         = "employees"
# MAGIC )
# MAGIC
# MAGIC df_result.show(truncate=False)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 6 — Row Count Validation After DROPMALFORMED
# MAGIC
# MAGIC Always validate that the returned row count is within an acceptable range:
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # ROW COUNT VALIDATION AFTER DROPMALFORMED
# MAGIC # ============================================================
# MAGIC
# MAGIC def read_dropmalformed_with_validation(
# MAGIC         path,
# MAGIC         schema,
# MAGIC         source_name,
# MAGIC         min_expected_rows   = None,
# MAGIC         max_drop_pct        = 10.0
# MAGIC ):
# MAGIC     """
# MAGIC     Read with DROPMALFORMED and validate
# MAGIC     that not too many rows were dropped
# MAGIC     """
# MAGIC
# MAGIC     # Read the file twice
# MAGIC     # Once with DROPMALFORMED for clean data
# MAGIC     # Once with PERMISSIVE to count total rows
# MAGIC
# MAGIC     df_clean = spark.read \
# MAGIC         .format("csv") \
# MAGIC         .option("header", "true") \
# MAGIC         .schema(schema) \
# MAGIC         .option("mode", "DROPMALFORMED") \
# MAGIC         .load(path)
# MAGIC
# MAGIC     df_all = spark.read \
# MAGIC         .format("csv") \
# MAGIC         .option("header", "true") \
# MAGIC         .option("inferSchema", "true") \
# MAGIC         .option("header", "true") \
# MAGIC         .load(path)
# MAGIC
# MAGIC     total_rows = df_all.count()
# MAGIC     clean_rows = df_clean.count()
# MAGIC     drop_count = total_rows - clean_rows
# MAGIC     drop_pct   = round(drop_count / total_rows * 100, 2) \
# MAGIC                  if total_rows > 0 else 100.0
# MAGIC
# MAGIC     print(f"\n=== DROPMALFORMED VALIDATION: {source_name} ===")
# MAGIC     print(f"Total rows          : {total_rows:,}")
# MAGIC     print(f"Clean rows returned : {clean_rows:,}")
# MAGIC     print(f"Rows dropped        : {drop_count:,}")
# MAGIC     print(f"Drop percentage     : {drop_pct}%")
# MAGIC     print(f"Max allowed drop    : {max_drop_pct}%")
# MAGIC
# MAGIC     # Validation 1 — empty DataFrame check
# MAGIC     if clean_rows == 0:
# MAGIC         raise ValueError(
# MAGIC             f"DROPMALFORMED returned ZERO rows for {source_name}. "
# MAGIC             f"Entire file may be malformed or wrong file sent."
# MAGIC         )
# MAGIC
# MAGIC     # Validation 2 — minimum row count check
# MAGIC     if min_expected_rows and clean_rows < min_expected_rows:
# MAGIC         raise ValueError(
# MAGIC             f"Only {clean_rows} rows returned for {source_name}. "
# MAGIC             f"Expected at least {min_expected_rows}. "
# MAGIC             f"Too many rows dropped."
# MAGIC         )
# MAGIC
# MAGIC     # Validation 3 — drop percentage check
# MAGIC     if drop_pct > max_drop_pct:
# MAGIC         raise ValueError(
# MAGIC             f"Drop rate {drop_pct}% exceeds "
# MAGIC             f"threshold {max_drop_pct}% for {source_name}. "
# MAGIC             f"Too many malformed rows. "
# MAGIC             f"Investigate source data."
# MAGIC         )
# MAGIC
# MAGIC     print(f"Status              : ALL VALIDATIONS PASSED")
# MAGIC     return df_clean
# MAGIC
# MAGIC
# MAGIC # Test on mixed data
# MAGIC try:
# MAGIC     df = read_dropmalformed_with_validation(
# MAGIC         path              = "/FileStore/test/mixed_employees.csv",
# MAGIC         schema            = schema,
# MAGIC         source_name       = "employees",
# MAGIC         min_expected_rows = 3,
# MAGIC         max_drop_pct      = 50.0
# MAGIC     )
# MAGIC     print(f"\nFinal DataFrame:")
# MAGIC     df.show(truncate=False)
# MAGIC
# MAGIC except ValueError as e:
# MAGIC     print(f"\nVALIDATION FAILED: {e}")
# MAGIC
# MAGIC
# MAGIC # Test on all bad data — should fail validation
# MAGIC try:
# MAGIC     df = read_dropmalformed_with_validation(
# MAGIC         path              = "/FileStore/test/all_bad.csv",
# MAGIC         schema            = schema,
# MAGIC         source_name       = "employees_bad",
# MAGIC         min_expected_rows = 1,
# MAGIC         max_drop_pct      = 10.0
# MAGIC     )
# MAGIC
# MAGIC except ValueError as e:
# MAGIC     print(f"\nVALIDATION FAILED: {e}")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 7 — DROPMALFORMED on BigMart Dataset
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # DROPMALFORMED ON BIGMART — REAL WORLD
# MAGIC # ============================================================
# MAGIC
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql.functions import *
# MAGIC
# MAGIC bigmart_schema = StructType([
# MAGIC     StructField("Item_Identifier",           StringType(),  True),
# MAGIC     StructField("Item_Weight",               DoubleType(),  True),
# MAGIC     StructField("Item_Fat_Content",          StringType(),  True),
# MAGIC     StructField("Item_Visibility",           DoubleType(),  True),
# MAGIC     StructField("Item_Type",                 StringType(),  True),
# MAGIC     StructField("Item_MRP",                  DoubleType(),  True),
# MAGIC     StructField("Outlet_Identifier",         StringType(),  True),
# MAGIC     StructField("Outlet_Establishment_Year", IntegerType(), True),
# MAGIC     StructField("Outlet_Size",               StringType(),  True),
# MAGIC     StructField("Outlet_Location_Type",      StringType(),  True),
# MAGIC     StructField("Outlet_Type",               StringType(),  True),
# MAGIC     StructField("Item_Outlet_Sales",         DoubleType(),  True)
# MAGIC ])
# MAGIC
# MAGIC print("=== BIGMART — DROPMALFORMED ===")
# MAGIC
# MAGIC df_bigmart = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(bigmart_schema) \
# MAGIC     .option("mode", "DROPMALFORMED") \
# MAGIC     .load("/FileStore/tables/BigMart.csv")
# MAGIC
# MAGIC bigmart_count = df_bigmart.count()
# MAGIC
# MAGIC print(f"Rows returned  : {bigmart_count:,}")
# MAGIC print(f"Expected rows  : 8,523")
# MAGIC print(f"Rows dropped   : {8523 - bigmart_count}")
# MAGIC
# MAGIC if bigmart_count == 8523:
# MAGIC     print(f"Conclusion     : BigMart data is clean")
# MAGIC     print(f"                 No rows dropped")
# MAGIC else:
# MAGIC     print(f"Conclusion     : {8523 - bigmart_count} "
# MAGIC           f"malformed rows were dropped")
# MAGIC
# MAGIC df_bigmart.show(5, truncate=False)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 8 — All Three Modes Final Comparison
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # ALL THREE MODES — COMPLETE COMPARISON
# MAGIC # ============================================================
# MAGIC
# MAGIC path = "/FileStore/test/mixed_employees.csv"
# MAGIC
# MAGIC print("=" * 65)
# MAGIC print("    COMPLETE MODE COMPARISON — SAME FILE")
# MAGIC print("=" * 65)
# MAGIC
# MAGIC # PERMISSIVE
# MAGIC df_p = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode", "PERMISSIVE") \
# MAGIC     .load(path)
# MAGIC
# MAGIC # DROPMALFORMED
# MAGIC df_d = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode", "DROPMALFORMED") \
# MAGIC     .load(path)
# MAGIC
# MAGIC # FAILFAST
# MAGIC failfast_result = "FAILED"
# MAGIC try:
# MAGIC     df_f = spark.read \
# MAGIC         .format("csv") \
# MAGIC         .option("header", "true") \
# MAGIC         .schema(schema) \
# MAGIC         .option("mode", "FAILFAST") \
# MAGIC         .load(path)
# MAGIC     df_f.count()
# MAGIC     failfast_result = str(df_f.count())
# MAGIC except Exception:
# MAGIC     failfast_result = "FAILED — exception thrown"
# MAGIC
# MAGIC p_count      = df_p.count()
# MAGIC p_null_count = df_p.filter(
# MAGIC     isnull(col("salary")) |
# MAGIC     isnull(col("emp_id")) |
# MAGIC     isnull(col("age"))    |
# MAGIC     isnull(col("join_date")) |
# MAGIC     isnull(col("is_active"))
# MAGIC ).count()
# MAGIC
# MAGIC d_count = df_d.count()
# MAGIC
# MAGIC print(f"\n  {'Mode':<20} {'Rows':>8} {'Bad Rows':>12} {'Job Status':<20}")
# MAGIC print(f"  {'-'*60}")
# MAGIC print(f"  {'PERMISSIVE':<20} {p_count:>8} "
# MAGIC       f"{'kept as null':>12}  {'Completed':<20}")
# MAGIC print(f"  {'DROPMALFORMED':<20} {d_count:>8} "
# MAGIC       f"{'silently gone':>12}  {'Completed':<20}")
# MAGIC print(f"  {'FAILFAST':<20} {'0':>8} "
# MAGIC       f"{'exception':>12}  {'Failed':<20}")
# MAGIC
# MAGIC print(f"\n  File had 10 rows total")
# MAGIC print(f"  6 rows had bad field values")
# MAGIC print(f"  4 rows were perfectly clean")
# MAGIC
# MAGIC print(f"\n  PERMISSIVE    → all 10 rows kept, "
# MAGIC       f"{p_null_count} rows have nulls")
# MAGIC print(f"  DROPMALFORMED → only {d_count} clean rows kept, "
# MAGIC       f"6 rows silently removed")
# MAGIC print(f"  FAILFAST      → 0 rows, job failed immediately")
# MAGIC print(f"{'='*65}")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## When to Use DROPMALFORMED — Complete Guide
# MAGIC
# MAGIC ```
# MAGIC USE DROPMALFORMED WHEN:
# MAGIC
# MAGIC 1. BAD ROWS HAVE ZERO VALUE
# MAGIC    A few malformed rows in millions of records
# MAGIC    carry no useful information even partially.
# MAGIC    Dropping them is cleaner than keeping nulls.
# MAGIC
# MAGIC 2. HIGH VOLUME ANALYTICAL WORKLOADS
# MAGIC    Processing 100 million rows for a trend report.
# MAGIC    Losing 500 bad rows (0.0005%) has zero impact
# MAGIC    on the statistical result.
# MAGIC    DROPMALFORMED keeps the pipeline simple.
# MAGIC
# MAGIC 3. AGGREGATION BASED PIPELINES
# MAGIC    When you are computing counts, averages,
# MAGIC    sums across millions of rows,
# MAGIC    a few dropped rows do not change the answer
# MAGIC    meaningfully.
# MAGIC
# MAGIC 4. CONTINUOUS STREAMING INGESTION
# MAGIC    Data arrives every minute from sensors or logs.
# MAGIC    Occasional malformed messages are expected.
# MAGIC    Dropping them is cleaner than nulls in a stream.
# MAGIC
# MAGIC 5. PRE-PROCESSED DATA WITH KNOWN BAD ROW RATE
# MAGIC    Upstream told you to expect up to 2% bad rows.
# MAGIC    You agreed to drop them.
# MAGIC    DROPMALFORMED automates that agreement.
# MAGIC
# MAGIC DO NOT USE DROPMALFORMED WHEN:
# MAGIC
# MAGIC 1. EVERY ROW MATTERS
# MAGIC    Transaction records, audit logs,
# MAGIC    records that must be reconciled.
# MAGIC    A dropped row means a missing transaction.
# MAGIC
# MAGIC 2. YOU NEED TO KNOW WHAT WAS DROPPED
# MAGIC    Compliance requires you to account for
# MAGIC    every record received.
# MAGIC    Use PERMISSIVE with _corrupt_record instead.
# MAGIC
# MAGIC 3. WITHOUT ROW COUNT VALIDATION
# MAGIC    Never use DROPMALFORMED without checking
# MAGIC    that the returned row count is reasonable.
# MAGIC    Empty DataFrame with no error is
# MAGIC    the most silent failure in Spark.
# MAGIC
# MAGIC 4. WHEN DROP RATE IS UNPREDICTABLE
# MAGIC    If bad rows could be 0% today and 90% tomorrow,
# MAGIC    DROPMALFORMED will silently drop 90% with no alert.
# MAGIC    Add max_drop_pct validation always.
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## DROPMALFORMED Quick Reference
# MAGIC
# MAGIC | Aspect | Detail |
# MAGIC |---|---|
# MAGIC | Behavior on bad row | Silently removes entire row |
# MAGIC | Rows returned | Only perfectly clean rows |
# MAGIC | Nulls created | None — clean rows have no nulls |
# MAGIC | Job continues | Yes — always completes |
# MAGIC | Error thrown | Never |
# MAGIC | Visibility into drops | None — you must track manually |
# MAGIC | Empty DataFrame risk | High — all bad file = empty result |
# MAGIC | Best for | High volume analytics, known bad row rate |
# MAGIC | Must combine with | Row count validation, drop percentage check |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## One Line to Remember
# MAGIC
# MAGIC DROPMALFORMED is Spark's **silent cleaner** — it removes every malformed row without any noise, returns only perfectly clean rows, never throws an error, and always completes the job successfully, but its silence is also its biggest danger because it will return an empty DataFrame with zero errors if every single row is malformed, which is why it must always be paired with row count validation and drop percentage thresholds to catch when too much data is being silently lost.