# Databricks notebook source
# MAGIC %md
# MAGIC # FAILFAST Mode — Complete Deep Dive
# MAGIC
# MAGIC ## What is FAILFAST Mode?
# MAGIC
# MAGIC FAILFAST is Spark's **zero tolerance, strictest read mode**. The name says exactly what it does — **fail fast**. The moment Spark encounters even a single row that does not match the defined schema, it immediately throws an exception and the entire job stops.
# MAGIC
# MAGIC No partial results. No nulls. No silent corruption. The job either succeeds completely with clean data or it fails immediately the moment it sees something wrong.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The Philosophy Behind FAILFAST
# MAGIC
# MAGIC FAILFAST exists because in certain situations **a silent failure is worse than a loud failure**.
# MAGIC
# MAGIC Consider these real world scenarios:
# MAGIC
# MAGIC ```
# MAGIC Scenario 1 — Financial Report
# MAGIC Your pipeline calculates daily revenue.
# MAGIC A bad salary value silently becomes null.
# MAGIC The revenue report is wrong.
# MAGIC Nobody notices for 3 weeks.
# MAGIC Damage — wrong business decisions made.
# MAGIC
# MAGIC Scenario 2 — FAILFAST approach
# MAGIC Your pipeline calculates daily revenue.
# MAGIC A bad salary value triggers FAILFAST.
# MAGIC Job fails immediately with clear error.
# MAGIC Engineer gets alerted within minutes.
# MAGIC Upstream team fixes the data.
# MAGIC Pipeline reruns with correct data.
# MAGIC Damage — zero.
# MAGIC ```
# MAGIC
# MAGIC FAILFAST chooses loud and immediate failure over silent and hidden corruption. It is the **fail loud, fail early** philosophy.
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
# MAGIC # Perfectly clean data
# MAGIC clean_data = """emp_id,name,department,salary,age,join_date,is_active
# MAGIC 1,Alice,Engineering,95000.0,32,2019-03-15,true
# MAGIC 2,Bob,Marketing,52000.0,26,2021-07-22,true
# MAGIC 3,Charlie,Engineering,120000.0,38,2017-01-10,true
# MAGIC 4,Diana,HR,75000.0,35,2018-11-05,true
# MAGIC 5,Edward,Finance,88000.0,33,2019-09-30,true"""
# MAGIC
# MAGIC # Data with one bad row in the middle
# MAGIC one_bad_row = """emp_id,name,department,salary,age,join_date,is_active
# MAGIC 1,Alice,Engineering,95000.0,32,2019-03-15,true
# MAGIC 2,Bob,Marketing,52000.0,26,2021-07-22,true
# MAGIC 3,Charlie,Engineering,INVALID_SALARY,38,2017-01-10,true
# MAGIC 4,Diana,HR,75000.0,35,2018-11-05,true
# MAGIC 5,Edward,Finance,88000.0,33,2019-09-30,true"""
# MAGIC
# MAGIC # Data with bad row at the very end
# MAGIC bad_at_end = """emp_id,name,department,salary,age,join_date,is_active
# MAGIC 1,Alice,Engineering,95000.0,32,2019-03-15,true
# MAGIC 2,Bob,Marketing,52000.0,26,2021-07-22,true
# MAGIC 3,Charlie,Engineering,120000.0,38,2017-01-10,true
# MAGIC 4,Diana,HR,75000.0,35,2018-11-05,true
# MAGIC BROKEN_ROW,Edward,Finance,88000.0,33,2019-09-30,true"""
# MAGIC
# MAGIC # Data with bad row at the very start
# MAGIC bad_at_start = """emp_id,name,department,salary,age,join_date,is_active
# MAGIC NOT_A_NUMBER,Alice,Engineering,95000.0,32,2019-03-15,true
# MAGIC 2,Bob,Marketing,52000.0,26,2021-07-22,true
# MAGIC 3,Charlie,Engineering,120000.0,38,2017-01-10,true"""
# MAGIC
# MAGIC # Write all files
# MAGIC dbutils.fs.put("/FileStore/test/clean_employees.csv",
# MAGIC                clean_data, overwrite=True)
# MAGIC
# MAGIC dbutils.fs.put("/FileStore/test/one_bad_row.csv",
# MAGIC                one_bad_row, overwrite=True)
# MAGIC
# MAGIC dbutils.fs.put("/FileStore/test/bad_at_end.csv",
# MAGIC                bad_at_end, overwrite=True)
# MAGIC
# MAGIC dbutils.fs.put("/FileStore/test/bad_at_start.csv",
# MAGIC                bad_at_start, overwrite=True)
# MAGIC
# MAGIC print("All test files created")
# MAGIC print("  clean_employees.csv  — all rows perfect")
# MAGIC print("  one_bad_row.csv      — bad row in middle")
# MAGIC print("  bad_at_end.csv       — bad row at end")
# MAGIC print("  bad_at_start.csv     — bad row at start")
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
# MAGIC from pyspark.sql.functions import col, count, when, isnull
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
# MAGIC ## Part 1 — FAILFAST on Clean Data
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # FAILFAST ON PERFECTLY CLEAN DATA
# MAGIC # ============================================================
# MAGIC
# MAGIC print("=== FAILFAST ON CLEAN DATA ===")
# MAGIC
# MAGIC try:
# MAGIC     df_clean = spark.read \
# MAGIC         .format("csv") \
# MAGIC         .option("header", "true") \
# MAGIC         .schema(schema) \
# MAGIC         .option("mode", "FAILFAST") \
# MAGIC         .load("/FileStore/test/clean_employees.csv")
# MAGIC
# MAGIC     # Action triggers actual read and validation
# MAGIC     df_clean.show(truncate=False)
# MAGIC     print(f"Total rows: {df_clean.count()}")
# MAGIC     print("SUCCESS — all rows are clean")
# MAGIC
# MAGIC except Exception as e:
# MAGIC     print(f"FAILED: {e}")
# MAGIC ```
# MAGIC
# MAGIC Output:
# MAGIC ```
# MAGIC === FAILFAST ON CLEAN DATA ===
# MAGIC +------+-------+-----------+--------+---+----------+---------+
# MAGIC |emp_id|name   |department |salary  |age|join_date |is_active|
# MAGIC +------+-------+-----------+--------+---+----------+---------+
# MAGIC |1     |Alice  |Engineering|95000.0 |32 |2019-03-15|true     |
# MAGIC |2     |Bob    |Marketing  |52000.0 |26 |2021-07-22|true     |
# MAGIC |3     |Charlie|Engineering|120000.0|38 |2017-01-10|true     |
# MAGIC |4     |Diana  |HR         |75000.0 |35 |2018-11-05|true     |
# MAGIC |5     |Edward |Finance    |88000.0 |33 |2019-09-30|true     |
# MAGIC +------+-------+-----------+--------+---+----------+---------+
# MAGIC Total rows: 5
# MAGIC SUCCESS — all rows are clean
# MAGIC ```
# MAGIC
# MAGIC FAILFAST works perfectly when data is clean. No difference from other modes when data is good.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 2 — FAILFAST on Bad Data
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # FAILFAST ON DATA WITH ONE BAD ROW
# MAGIC # ============================================================
# MAGIC
# MAGIC print("=== FAILFAST ON DATA WITH BAD ROW ===")
# MAGIC
# MAGIC try:
# MAGIC     df_bad = spark.read \
# MAGIC         .format("csv") \
# MAGIC         .option("header", "true") \
# MAGIC         .schema(schema) \
# MAGIC         .option("mode", "FAILFAST") \
# MAGIC         .load("/FileStore/test/one_bad_row.csv")
# MAGIC
# MAGIC     # Transformation — lazy, no execution yet
# MAGIC     df_filtered = df_bad.filter(col("salary") > 60000)
# MAGIC
# MAGIC     # ACTION — this triggers actual reading and FAILFAST check
# MAGIC     df_filtered.show()
# MAGIC
# MAGIC     print("SUCCESS")   # This line will NOT be reached
# MAGIC
# MAGIC except Exception as e:
# MAGIC     print(f"\nFAILFAST TRIGGERED")
# MAGIC     print(f"Error type : {type(e).__name__}")
# MAGIC     print(f"Error msg  : {str(e)[:400]}")
# MAGIC     print(f"\nJob stopped immediately")
# MAGIC     print(f"Zero rows processed or returned")
# MAGIC ```
# MAGIC
# MAGIC Output:
# MAGIC ```
# MAGIC === FAILFAST ON DATA WITH BAD ROW ===
# MAGIC
# MAGIC FAILFAST TRIGGERED
# MAGIC Error type : SparkException
# MAGIC Error msg  : Malformed records are detected in record parsing.
# MAGIC              Parse Mode: FAILFAST. To process malformed records as null
# MAGIC              result, try setting the option 'mode' as 'PERMISSIVE'.
# MAGIC
# MAGIC Job stopped immediately
# MAGIC Zero rows processed or returned
# MAGIC ```
# MAGIC
# MAGIC One bad row in a file of 5 rows — entire job stops. No partial results returned.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 3 — FAILFAST Regardless of Where Bad Row Is
# MAGIC
# MAGIC FAILFAST catches corruption wherever it appears — beginning, middle, or end of file:
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # FAILFAST — LOCATION DOES NOT MATTER
# MAGIC # ============================================================
# MAGIC
# MAGIC files = {
# MAGIC     "Bad row in MIDDLE" : "/FileStore/test/one_bad_row.csv",
# MAGIC     "Bad row at END"    : "/FileStore/test/bad_at_end.csv",
# MAGIC     "Bad row at START"  : "/FileStore/test/bad_at_start.csv"
# MAGIC }
# MAGIC
# MAGIC for description, path in files.items():
# MAGIC     print(f"\n=== {description} ===")
# MAGIC     try:
# MAGIC         df = spark.read \
# MAGIC             .format("csv") \
# MAGIC             .option("header", "true") \
# MAGIC             .schema(schema) \
# MAGIC             .option("mode", "FAILFAST") \
# MAGIC             .load(path)
# MAGIC
# MAGIC         # Must trigger action to actually read
# MAGIC         count = df.count()
# MAGIC         print(f"SUCCESS — {count} rows read")
# MAGIC
# MAGIC     except Exception as e:
# MAGIC         print(f"FAILED — FAILFAST triggered")
# MAGIC         print(f"Error: {str(e)[:150]}")
# MAGIC ```
# MAGIC
# MAGIC Output:
# MAGIC ```
# MAGIC === Bad row in MIDDLE ===
# MAGIC FAILED — FAILFAST triggered
# MAGIC Error: Malformed records are detected in record parsing...
# MAGIC
# MAGIC === Bad row at END ===
# MAGIC FAILED — FAILFAST triggered
# MAGIC Error: Malformed records are detected in record parsing...
# MAGIC
# MAGIC === Bad row at START ===
# MAGIC FAILED — FAILFAST triggered
# MAGIC Error: Malformed records are detected in record parsing...
# MAGIC ```
# MAGIC
# MAGIC All three fail. FAILFAST does not care where the bad row is. Any corruption anywhere in the file causes immediate failure.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 4 — FAILFAST is Lazy Too
# MAGIC
# MAGIC This is a very important and commonly misunderstood point. Even FAILFAST follows Spark's lazy evaluation. The check does not happen when you write the read statement — it happens when an action is called.
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # PROOF — FAILFAST IS STILL LAZY
# MAGIC # ============================================================
# MAGIC
# MAGIC print("Step 1 — Writing read statement")
# MAGIC df = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode", "FAILFAST") \
# MAGIC     .load("/FileStore/test/one_bad_row.csv")
# MAGIC print("Step 1 done — NO error yet even though file has bad row")
# MAGIC
# MAGIC print("\nStep 2 — Adding transformations")
# MAGIC df2 = df.filter(col("salary") > 60000)
# MAGIC df3 = df2.withColumn("bonus", col("salary") * 0.1)
# MAGIC print("Step 2 done — Still NO error")
# MAGIC
# MAGIC print("\nStep 3 — Calling action (show)")
# MAGIC try:
# MAGIC     df3.show()   # ERROR HAPPENS HERE — not above
# MAGIC     print("Success")
# MAGIC except Exception as e:
# MAGIC     print(f"FAILFAST triggered HERE at the action")
# MAGIC     print(f"Not at the read statement")
# MAGIC     print(f"Not at the transformations")
# MAGIC     print(f"Only when action was called")
# MAGIC ```
# MAGIC
# MAGIC Output:
# MAGIC ```
# MAGIC Step 1 — Writing read statement
# MAGIC Step 1 done — NO error yet even though file has bad row
# MAGIC
# MAGIC Step 2 — Adding transformations
# MAGIC Step 2 done — Still NO error
# MAGIC
# MAGIC Step 3 — Calling action (show)
# MAGIC FAILFAST triggered HERE at the action
# MAGIC Not at the read statement
# MAGIC Not at the transformations
# MAGIC Only when action was called
# MAGIC ```
# MAGIC
# MAGIC This is critical to understand. The read statement and all transformations are lazy. FAILFAST validation happens at the action — just like everything else in Spark.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 5 — Handling FAILFAST Gracefully
# MAGIC
# MAGIC In production you always wrap FAILFAST in try-except with proper alerting:
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # PRODUCTION FAILFAST PATTERN
# MAGIC # ============================================================
# MAGIC
# MAGIC import traceback
# MAGIC from datetime import datetime
# MAGIC
# MAGIC def read_with_failfast(path, schema, source_name):
# MAGIC     """
# MAGIC     Production grade FAILFAST read
# MAGIC     Fails immediately on bad data
# MAGIC     Sends clear alert with details
# MAGIC     """
# MAGIC
# MAGIC     print(f"\n{'='*55}")
# MAGIC     print(f"  FAILFAST READ: {source_name}")
# MAGIC     print(f"  Path: {path}")
# MAGIC     print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
# MAGIC     print(f"{'='*55}")
# MAGIC
# MAGIC     try:
# MAGIC         df = spark.read \
# MAGIC             .format("csv") \
# MAGIC             .option("header", "true") \
# MAGIC             .schema(schema) \
# MAGIC             .option("mode", "FAILFAST") \
# MAGIC             .load(path)
# MAGIC
# MAGIC         # Trigger actual read with action
# MAGIC         row_count = df.count()
# MAGIC
# MAGIC         print(f"  Status    : SUCCESS")
# MAGIC         print(f"  Rows read : {row_count:,}")
# MAGIC         print(f"{'='*55}\n")
# MAGIC
# MAGIC         return df
# MAGIC
# MAGIC     except Exception as e:
# MAGIC
# MAGIC         # Capture full error details
# MAGIC         error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# MAGIC         error_msg  = str(e)
# MAGIC
# MAGIC         print(f"\n  Status  : FAILED")
# MAGIC         print(f"  Source  : {source_name}")
# MAGIC         print(f"  Time    : {error_time}")
# MAGIC         print(f"  Error   : {error_msg[:300]}")
# MAGIC         print(f"\n  ACTION REQUIRED:")
# MAGIC         print(f"  1. Check source file: {path}")
# MAGIC         print(f"  2. Identify corrupt rows")
# MAGIC         print(f"  3. Fix data at source")
# MAGIC         print(f"  4. Rerun pipeline")
# MAGIC         print(f"{'='*55}\n")
# MAGIC
# MAGIC         # Re-raise to stop the pipeline
# MAGIC         raise RuntimeError(
# MAGIC             f"FAILFAST: Data quality failure in {source_name} "
# MAGIC             f"at {error_time}. Pipeline stopped. "
# MAGIC             f"Fix source data and rerun."
# MAGIC         ) from e
# MAGIC
# MAGIC
# MAGIC # ── Test the function ────────────────────────────────────────
# MAGIC
# MAGIC # Clean file — should succeed
# MAGIC try:
# MAGIC     df_success = read_with_failfast(
# MAGIC         path        = "/FileStore/test/clean_employees.csv",
# MAGIC         schema      = schema,
# MAGIC         source_name = "employees_clean"
# MAGIC     )
# MAGIC     df_success.show()
# MAGIC
# MAGIC except RuntimeError as e:
# MAGIC     print(f"Pipeline halted: {e}")
# MAGIC
# MAGIC
# MAGIC # Bad file — should fail
# MAGIC try:
# MAGIC     df_fail = read_with_failfast(
# MAGIC         path        = "/FileStore/test/one_bad_row.csv",
# MAGIC         schema      = schema,
# MAGIC         source_name = "employees_corrupt"
# MAGIC     )
# MAGIC     df_fail.show()
# MAGIC
# MAGIC except RuntimeError as e:
# MAGIC     print(f"Pipeline halted: {e}")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 6 — FAILFAST then Diagnose Pattern
# MAGIC
# MAGIC When FAILFAST triggers, you want to immediately understand what went wrong. Use PERMISSIVE to diagnose after FAILFAST fails:
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # FAILFAST THEN DIAGNOSE PATTERN
# MAGIC # ============================================================
# MAGIC
# MAGIC def failfast_with_diagnosis(path, schema, schema_with_corrupt):
# MAGIC     """
# MAGIC     Try FAILFAST first.
# MAGIC     If it fails, automatically run PERMISSIVE diagnosis
# MAGIC     to show exactly what went wrong.
# MAGIC     """
# MAGIC     print("=== ATTEMPTING FAILFAST READ ===")
# MAGIC
# MAGIC     try:
# MAGIC         df = spark.read \
# MAGIC             .format("csv") \
# MAGIC             .option("header", "true") \
# MAGIC             .schema(schema) \
# MAGIC             .option("mode", "FAILFAST") \
# MAGIC             .load(path)
# MAGIC
# MAGIC         df.count()  # Trigger action
# MAGIC         print("FAILFAST PASSED — data is clean")
# MAGIC         return df
# MAGIC
# MAGIC     except Exception as e:
# MAGIC         print(f"FAILFAST FAILED — running diagnosis\n")
# MAGIC
# MAGIC         # Automatically diagnose with PERMISSIVE
# MAGIC         df_diagnosis = spark.read \
# MAGIC             .format("csv") \
# MAGIC             .option("header", "true") \
# MAGIC             .schema(schema_with_corrupt) \
# MAGIC             .option("mode", "PERMISSIVE") \
# MAGIC             .option("columnNameOfCorruptRecord",
# MAGIC                     "_corrupt_record") \
# MAGIC             .load(path)
# MAGIC
# MAGIC         total   = df_diagnosis.count()
# MAGIC         corrupt = df_diagnosis.filter(
# MAGIC             col("_corrupt_record").isNotNull()
# MAGIC         ).count()
# MAGIC         good    = total - corrupt
# MAGIC
# MAGIC         print("=" * 55)
# MAGIC         print("        DIAGNOSIS REPORT")
# MAGIC         print("=" * 55)
# MAGIC         print(f"  Total rows    : {total}")
# MAGIC         print(f"  Good rows     : {good}")
# MAGIC         print(f"  Corrupt rows  : {corrupt}")
# MAGIC         print(f"  Corrupt rate  : {round(corrupt/total*100,1)}%")
# MAGIC         print("=" * 55)
# MAGIC
# MAGIC         print("\n  Corrupt rows detail:")
# MAGIC         df_diagnosis \
# MAGIC             .filter(col("_corrupt_record").isNotNull()) \
# MAGIC             .select("_corrupt_record") \
# MAGIC             .show(truncate=False)
# MAGIC
# MAGIC         print("\n  Null analysis:")
# MAGIC         df_diagnosis \
# MAGIC             .filter(col("_corrupt_record").isNotNull()) \
# MAGIC             .drop("_corrupt_record") \
# MAGIC             .select([
# MAGIC                 count(when(isnull(col(c)), c)).alias(c)
# MAGIC                 for c in schema.fieldNames()
# MAGIC             ]).show()
# MAGIC
# MAGIC         raise RuntimeError(
# MAGIC             f"Data quality failure. "
# MAGIC             f"{corrupt} corrupt rows found. "
# MAGIC             f"Fix source data and rerun."
# MAGIC         )
# MAGIC
# MAGIC
# MAGIC # Schema with corrupt column for diagnosis
# MAGIC schema_with_corrupt = StructType(
# MAGIC     schema.fields + [
# MAGIC         StructField("_corrupt_record", StringType(), True)
# MAGIC     ]
# MAGIC )
# MAGIC
# MAGIC # Test on bad file
# MAGIC try:
# MAGIC     df = failfast_with_diagnosis(
# MAGIC         path                = "/FileStore/test/one_bad_row.csv",
# MAGIC         schema              = schema,
# MAGIC         schema_with_corrupt = schema_with_corrupt
# MAGIC     )
# MAGIC except RuntimeError as e:
# MAGIC     print(f"\nPipeline stopped: {e}")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 7 — FAILFAST on BigMart Dataset
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # FAILFAST ON BIGMART — REAL WORLD TEST
# MAGIC # ============================================================
# MAGIC
# MAGIC from pyspark.sql.types import *
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
# MAGIC print("=== FAILFAST ON BIGMART DATASET ===")
# MAGIC
# MAGIC try:
# MAGIC     df_bigmart = spark.read \
# MAGIC         .format("csv") \
# MAGIC         .option("header", "true") \
# MAGIC         .schema(bigmart_schema) \
# MAGIC         .option("mode", "FAILFAST") \
# MAGIC         .load("/FileStore/tables/BigMart.csv")
# MAGIC
# MAGIC     row_count = df_bigmart.count()
# MAGIC
# MAGIC     print(f"SUCCESS")
# MAGIC     print(f"Total rows : {row_count:,}")
# MAGIC     print(f"Conclusion : BigMart CSV is clean")
# MAGIC     print(f"             No type mismatches found")
# MAGIC     df_bigmart.show(5, truncate=False)
# MAGIC
# MAGIC except Exception as e:
# MAGIC     print(f"FAILFAST TRIGGERED")
# MAGIC     print(f"BigMart data has type mismatches")
# MAGIC     print(f"Error: {str(e)[:300]}")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 8 — All Three Modes Compared on Same File
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # ALL THREE MODES — FINAL COMPARISON
# MAGIC # ============================================================
# MAGIC
# MAGIC path = "/FileStore/test/one_bad_row.csv"
# MAGIC
# MAGIC print("=" * 60)
# MAGIC print("    THREE MODES — SAME FILE — DIFFERENT OUTCOMES")
# MAGIC print("=" * 60)
# MAGIC
# MAGIC # PERMISSIVE
# MAGIC df_p = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode", "PERMISSIVE") \
# MAGIC     .load(path)
# MAGIC permissive_count = df_p.count()
# MAGIC permissive_nulls = df_p.filter(isnull(col("salary"))).count()
# MAGIC
# MAGIC print(f"\n  PERMISSIVE")
# MAGIC print(f"  Rows returned      : {permissive_count}")
# MAGIC print(f"  Rows with nulls    : {permissive_nulls}")
# MAGIC print(f"  Job status         : Completed")
# MAGIC print(f"  Bad data handling  : Kept as null")
# MAGIC
# MAGIC # DROPMALFORMED
# MAGIC df_d = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode", "DROPMALFORMED") \
# MAGIC     .load(path)
# MAGIC drop_count = df_d.count()
# MAGIC
# MAGIC print(f"\n  DROPMALFORMED")
# MAGIC print(f"  Rows returned      : {drop_count}")
# MAGIC print(f"  Rows dropped       : {permissive_count - drop_count}")
# MAGIC print(f"  Job status         : Completed")
# MAGIC print(f"  Bad data handling  : Silently removed")
# MAGIC
# MAGIC # FAILFAST
# MAGIC print(f"\n  FAILFAST")
# MAGIC try:
# MAGIC     df_f = spark.read \
# MAGIC         .format("csv") \
# MAGIC         .option("header", "true") \
# MAGIC         .schema(schema) \
# MAGIC         .option("mode", "FAILFAST") \
# MAGIC         .load(path)
# MAGIC     df_f.count()
# MAGIC     print(f"  Rows returned      : {df_f.count()}")
# MAGIC     print(f"  Job status         : Completed")
# MAGIC
# MAGIC except Exception as e:
# MAGIC     print(f"  Rows returned      : 0")
# MAGIC     print(f"  Job status         : FAILED IMMEDIATELY")
# MAGIC     print(f"  Bad data handling  : Exception thrown")
# MAGIC     print(f"  Error              : Malformed record detected")
# MAGIC
# MAGIC print(f"\n{'='*60}")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## When to Use FAILFAST — Detailed Guide
# MAGIC
# MAGIC ```
# MAGIC USE FAILFAST WHEN:
# MAGIC
# MAGIC 1. FINANCIAL AND MONETARY PIPELINES
# MAGIC    Revenue calculations, payroll, billing.
# MAGIC    A wrong value silently becoming null
# MAGIC    means wrong financial reports.
# MAGIC    FAILFAST ensures wrong data never enters.
# MAGIC
# MAGIC 2. REGULATORY AND COMPLIANCE DATA
# MAGIC    Data submitted to government bodies,
# MAGIC    auditors, or compliance systems.
# MAGIC    Must be 100% correct — no exceptions.
# MAGIC
# MAGIC 3. UPSTREAM TEAM HAS A DATA CONTRACT
# MAGIC    Your team and the upstream team agreed
# MAGIC    that data will arrive in a specific format.
# MAGIC    FAILFAST enforces that contract.
# MAGIC    If upstream breaks it, FAILFAST alerts immediately.
# MAGIC
# MAGIC 4. PRE-VALIDATED DATA
# MAGIC    Data already passed through a validation layer
# MAGIC    before reaching Spark.
# MAGIC    FAILFAST is the final safety net.
# MAGIC    If it triggers, something serious went wrong.
# MAGIC
# MAGIC 5. YOU NEED IMMEDIATE ALERTING
# MAGIC    A downstream dashboard must be accurate.
# MAGIC    Silent nulls from PERMISSIVE would show
# MAGIC    wrong numbers and nobody would know.
# MAGIC    FAILFAST fails the job, triggers monitoring alert,
# MAGIC    engineer investigates immediately.
# MAGIC
# MAGIC 6. SMALL DATASETS WHERE EVERY ROW MATTERS
# MAGIC    If you are processing 100 rows of
# MAGIC    critical configuration data,
# MAGIC    missing even one row is unacceptable.
# MAGIC
# MAGIC DO NOT USE FAILFAST WHEN:
# MAGIC
# MAGIC 1. DATA IS EXPLORATORY
# MAGIC    You are investigating a new data source.
# MAGIC    You do not yet know its quality.
# MAGIC    Use PERMISSIVE first to understand the data.
# MAGIC
# MAGIC 2. SOME BAD ROWS ARE EXPECTED AND ACCEPTABLE
# MAGIC    High volume data where 0.01% bad rows
# MAGIC    have no meaningful impact on results.
# MAGIC    Use DROPMALFORMED or PERMISSIVE.
# MAGIC
# MAGIC 3. YOU DO NOT HAVE A DATA RECOVERY PLAN
# MAGIC    If FAILFAST triggers and you have no way
# MAGIC    to quickly fix the source data and rerun,
# MAGIC    the business impact of a stuck pipeline
# MAGIC    may be worse than some null values.
# MAGIC
# MAGIC 4. UPSTREAM DATA QUALITY IS UNPREDICTABLE
# MAGIC    Third party APIs, vendor feeds, manual uploads.
# MAGIC    Use PERMISSIVE with quarantine pattern instead.
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## FAILFAST Quick Reference
# MAGIC
# MAGIC | Aspect | Detail |
# MAGIC |---|---|
# MAGIC | Behavior on bad row | Throws exception immediately |
# MAGIC | Rows returned | Zero — job stops |
# MAGIC | Job continues | No |
# MAGIC | Error visibility | High — explicit exception |
# MAGIC | When triggered | At action — lazy evaluation still applies |
# MAGIC | Best for | Financial data, strict contracts, compliance |
# MAGIC | Avoid for | Exploration, unpredictable upstream data |
# MAGIC | Combine with | try-except, alerting, PERMISSIVE diagnosis |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## One Line to Remember
# MAGIC
# MAGIC FAILFAST is Spark's **"one strike and you're out"** read mode — the moment it finds even a single malformed record it throws an exception and stops the entire job immediately, making it the right choice when bad data silently passing through your pipeline would cause more damage than a failed pipeline would, because a loud failure you can fix is always better than a silent failure you cannot see.

# COMMAND ----------

# ============================================================
# CREATE TEST FILES
# ============================================================

# Perfectly clean data
clean_data = """emp_id,name,department,salary,age,join_date,is_active
1,Alice,Engineering,95000.0,32,2019-03-15,true
2,Bob,Marketing,52000.0,26,2021-07-22,true
3,Charlie,Engineering,120000.0,38,2017-01-10,true
4,Diana,HR,75000.0,35,2018-11-05,true
5,Edward,Finance,88000.0,33,2019-09-30,true"""

# Data with one bad row in the middle
one_bad_row = """emp_id,name,department,salary,age,join_date,is_active
1,Alice,Engineering,95000.0,32,2019-03-15,true
2,Bob,Marketing,52000.0,26,2021-07-22,true
3,Charlie,Engineering,INVALID_SALARY,38,2017-01-10,true
4,Diana,HR,75000.0,35,2018-11-05,true
5,Edward,Finance,88000.0,33,2019-09-30,true"""

# Data with bad row at the very end
bad_at_end = """emp_id,name,department,salary,age,join_date,is_active
1,Alice,Engineering,95000.0,32,2019-03-15,true
2,Bob,Marketing,52000.0,26,2021-07-22,true
3,Charlie,Engineering,120000.0,38,2017-01-10,true
4,Diana,HR,75000.0,35,2018-11-05,true
BROKEN_ROW,Edward,Finance,88000.0,33,2019-09-30,true"""

# Data with bad row at the very start
bad_at_start = """emp_id,name,department,salary,age,join_date,is_active
NOT_A_NUMBER,Alice,Engineering,95000.0,32,2019-03-15,true
2,Bob,Marketing,52000.0,26,2021-07-22,true
3,Charlie,Engineering,120000.0,38,2017-01-10,true"""

# Write all files
dbutils.fs.put("/Volumes/workspace/default/data_engineering/test/clean_employees.csv",
               clean_data, overwrite=True)

dbutils.fs.put("/Volumes/workspace/default/data_engineering/test/one_bad_row.csv",
               one_bad_row, overwrite=True)

dbutils.fs.put("/Volumes/workspace/default/data_engineering/test/bad_at_end.csv",
               bad_at_end, overwrite=True)

dbutils.fs.put("/Volumes/workspace/default/data_engineering/test/bad_at_start.csv",
               bad_at_start, overwrite=True)

print("All test files created")
print("  clean_employees.csv  — all rows perfect")
print("  one_bad_row.csv      — bad row in middle")
print("  bad_at_end.csv       — bad row at end")
print("  bad_at_start.csv     — bad row at start")

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType,
    DoubleType, BooleanType, DateType
)
from pyspark.sql.functions import col, count, when, isnull

schema = StructType([
    StructField("emp_id",     IntegerType(), True),
    StructField("name",       StringType(),  True),
    StructField("department", StringType(),  True),
    StructField("salary",     DoubleType(),  True),
    StructField("age",        IntegerType(), True),
    StructField("join_date",  DateType(),    True),
    StructField("is_active",  BooleanType(), True)
])

# ============================================================
# FAILFAST ON PERFECTLY CLEAN DATA
# ============================================================

print("=== FAILFAST ON CLEAN DATA ===")

try:
    df_clean = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .option("mode", "FAILFAST") \
        .load("/Volumes/workspace/default/data_engineering/test/clean_employees.csv")

    # Action triggers actual read and validation
    df_clean.show(truncate=False)
    print(f"Total rows: {df_clean.count()}")
    print("SUCCESS — all rows are clean")

except Exception as e:
    print(f"FAILED: {e}")

# ============================================================
# FAILFAST ON DATA WITH ONE BAD ROW
# ============================================================

print("=== FAILFAST ON DATA WITH BAD ROW ===")

try:
    df_bad = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .option("mode", "FAILFAST") \
        .load("/Volumes/workspace/default/data_engineering/test/one_bad_row.csv")

    # # Transformation — lazy, no execution yet
    # df_filtered = df_bad.filter(col("salary") > 60000)

    # ACTION — this triggers actual reading and FAILFAST check
    df_bad.show()

    print("SUCCESS")   # This line will NOT be reached

except Exception as e:
    print(f"\nFAILFAST TRIGGERED")
    print(f"Error type : {type(e).__name__}")
    print(f"Error msg  : {str(e)[:400]}")
    print(f"\nJob stopped immediately")
    print(f"Zero rows processed or returned")


