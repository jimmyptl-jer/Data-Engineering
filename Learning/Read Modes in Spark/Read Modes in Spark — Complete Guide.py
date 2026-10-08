# Databricks notebook source
# MAGIC %md
# MAGIC # Read Modes in Spark — Complete Guide
# MAGIC
# MAGIC ## What are Read Modes?
# MAGIC
# MAGIC When Spark reads data from a source — CSV, JSON, or any other format — the incoming data is not always perfect. Values may have wrong types, rows may be malformed, columns may be missing, or the structure may be completely broken.
# MAGIC
# MAGIC **Read Modes tell Spark what to do when it encounters bad or corrupt data during reading.**
# MAGIC
# MAGIC It is essentially your corruption handling strategy at the point of ingestion.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The Three Read Modes
# MAGIC
# MAGIC Spark provides exactly three read modes:
# MAGIC
# MAGIC ```
# MAGIC PERMISSIVE    → Keep bad rows, fill bad fields with null
# MAGIC DROPMALFORMED → Silently drop bad rows entirely
# MAGIC FAILFAST      → Immediately throw an error and stop
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Setup — Creating Test Data
# MAGIC
# MAGIC Let us create files with intentionally bad data so we can see each mode in action clearly.
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # CREATE TEST FILES WITH BAD DATA
# MAGIC # ============================================================
# MAGIC
# MAGIC # Good data
# MAGIC good_data = """emp_id,name,department,salary,is_active
# MAGIC 1,Alice,Engineering,95000.0,true
# MAGIC 2,Bob,Marketing,52000.0,true
# MAGIC 3,Charlie,Engineering,120000.0,true"""
# MAGIC
# MAGIC # Bad data — mixed with good rows
# MAGIC bad_data = """emp_id,name,department,salary,is_active
# MAGIC 1,Alice,Engineering,95000.0,true
# MAGIC 2,Bob,Marketing,INVALID_SALARY,true
# MAGIC 3,Charlie,Engineering,120000.0,true
# MAGIC NOT_A_NUMBER,Diana,HR,75000.0,true
# MAGIC 5,Edward,Finance,88000.0,WRONG_BOOLEAN
# MAGIC 6,Fiona,Engineering,92000.0,false"""
# MAGIC
# MAGIC # Write to DBFS
# MAGIC dbutils.fs.put(
# MAGIC     "/FileStore/test/good_employees.csv",
# MAGIC     good_data,
# MAGIC     overwrite=True
# MAGIC )
# MAGIC
# MAGIC dbutils.fs.put(
# MAGIC     "/FileStore/test/bad_employees.csv",
# MAGIC     bad_data,
# MAGIC     overwrite=True
# MAGIC )
# MAGIC
# MAGIC print("Test files created")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Schema for Reading
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.types import (
# MAGIC     StructType, StructField,
# MAGIC     StringType, IntegerType,
# MAGIC     DoubleType, BooleanType
# MAGIC )
# MAGIC from pyspark.sql.functions import col, count, when, isnull
# MAGIC
# MAGIC schema = StructType([
# MAGIC     StructField("emp_id",     IntegerType(), True),
# MAGIC     StructField("name",       StringType(),  True),
# MAGIC     StructField("department", StringType(),  True),
# MAGIC     StructField("salary",     DoubleType(),  True),
# MAGIC     StructField("is_active",  BooleanType(), True)
# MAGIC ])
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Mode 1 — PERMISSIVE (Default)
# MAGIC
# MAGIC ### What it Does
# MAGIC
# MAGIC PERMISSIVE is the **default mode** in Spark. When a bad row is encountered:
# MAGIC - The row is kept in the DataFrame
# MAGIC - Fields that cannot be parsed are set to **null**
# MAGIC - Optionally a special column captures the raw corrupt row
# MAGIC
# MAGIC This is the most lenient mode. Bad data does not stop your job. It silently converts problems to nulls.
# MAGIC
# MAGIC ### Basic PERMISSIVE
# MAGIC
# MAGIC ```python
# MAGIC # PERMISSIVE is default — you do not even need to specify it
# MAGIC df_permissive = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode", "PERMISSIVE") \
# MAGIC     .load("/FileStore/test/bad_employees.csv")
# MAGIC
# MAGIC print("=== PERMISSIVE MODE — ALL ROWS ===")
# MAGIC df_permissive.show(truncate=False)
# MAGIC ```
# MAGIC
# MAGIC Output:
# MAGIC ```
# MAGIC +------+-------+-----------+--------+---------+
# MAGIC |emp_id|name   |department |salary  |is_active|
# MAGIC +------+-------+-----------+--------+---------+
# MAGIC |1     |Alice  |Engineering|95000.0 |true     |
# MAGIC |2     |Bob    |Marketing  |null    |true     |  ← salary was INVALID_SALARY → null
# MAGIC |3     |Charlie|Engineering|120000.0|true     |
# MAGIC |null  |Diana  |HR         |75000.0 |true     |  ← emp_id was NOT_A_NUMBER → null
# MAGIC |5     |Edward |Finance    |88000.0 |null     |  ← is_active was WRONG_BOOLEAN → null
# MAGIC |6     |Fiona  |Engineering|92000.0 |false    |
# MAGIC +------+-------+-----------+--------+---------+
# MAGIC ```
# MAGIC
# MAGIC Notice — all 6 rows are kept. Bad values became null. No errors thrown.
# MAGIC
# MAGIC ### PERMISSIVE with Corrupt Record Column
# MAGIC
# MAGIC The most powerful use of PERMISSIVE — capture the original raw bad row:
# MAGIC
# MAGIC ```python
# MAGIC # Add _corrupt_record column to capture bad rows
# MAGIC schema_with_corrupt = StructType([
# MAGIC     StructField("emp_id",          IntegerType(), True),
# MAGIC     StructField("name",            StringType(),  True),
# MAGIC     StructField("department",      StringType(),  True),
# MAGIC     StructField("salary",          DoubleType(),  True),
# MAGIC     StructField("is_active",       BooleanType(), True),
# MAGIC     StructField("_corrupt_record", StringType(),  True)  # Add this
# MAGIC ])
# MAGIC
# MAGIC df_with_corrupt = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema_with_corrupt) \
# MAGIC     .option("mode", "PERMISSIVE") \
# MAGIC     .option("columnNameOfCorruptRecord", "_corrupt_record") \
# MAGIC     .load("/FileStore/test/bad_employees.csv")
# MAGIC
# MAGIC print("=== ALL ROWS INCLUDING CORRUPT ===")
# MAGIC df_with_corrupt.show(truncate=False)
# MAGIC
# MAGIC # Separate good and bad rows
# MAGIC df_good = df_with_corrupt.filter(
# MAGIC     col("_corrupt_record").isNull()
# MAGIC )
# MAGIC
# MAGIC df_bad = df_with_corrupt.filter(
# MAGIC     col("_corrupt_record").isNotNull()
# MAGIC )
# MAGIC
# MAGIC print("\n=== GOOD ROWS ONLY ===")
# MAGIC df_good.show(truncate=False)
# MAGIC
# MAGIC print("\n=== CORRUPT ROWS ONLY ===")
# MAGIC df_bad.select("_corrupt_record").show(truncate=False)
# MAGIC
# MAGIC print(f"\nTotal rows    : {df_with_corrupt.count()}")
# MAGIC print(f"Good rows     : {df_good.count()}")
# MAGIC print(f"Corrupt rows  : {df_bad.count()}")
# MAGIC ```
# MAGIC
# MAGIC ### When to Use PERMISSIVE
# MAGIC
# MAGIC ```
# MAGIC ✓ Exploring a new dataset for the first time
# MAGIC ✓ You want to understand what corruption exists
# MAGIC ✓ You want to separate good and bad records
# MAGIC ✓ Bad rows are expected but should be logged not stopped
# MAGIC ✓ You are building a data quality report
# MAGIC ✓ Downstream logic can handle nulls
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Mode 2 — DROPMALFORMED
# MAGIC
# MAGIC ### What it Does
# MAGIC
# MAGIC DROPMALFORMED is the **silent dropper**. When a bad row is encountered:
# MAGIC - The row is completely removed from the DataFrame
# MAGIC - No error is thrown
# MAGIC - No null values — the row simply does not exist
# MAGIC - Good rows continue processing normally
# MAGIC
# MAGIC ```python
# MAGIC df_drop = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode", "DROPMALFORMED") \
# MAGIC     .load("/FileStore/test/bad_employees.csv")
# MAGIC
# MAGIC print("=== DROPMALFORMED MODE ===")
# MAGIC df_drop.show(truncate=False)
# MAGIC print(f"Total rows after drop: {df_drop.count()}")
# MAGIC ```
# MAGIC
# MAGIC Output:
# MAGIC ```
# MAGIC +------+-------+-----------+--------+---------+
# MAGIC |emp_id|name   |department |salary  |is_active|
# MAGIC +------+-------+-----------+--------+---------+
# MAGIC |1     |Alice  |Engineering|95000.0 |true     |
# MAGIC |3     |Charlie|Engineering|120000.0|true     |
# MAGIC |6     |Fiona  |Engineering|92000.0 |false    |
# MAGIC +------+-------+-----------+--------+---------+
# MAGIC Total rows after drop: 3
# MAGIC ```
# MAGIC
# MAGIC Rows 2, 4, and 5 are completely gone. No trace of them. No errors.
# MAGIC
# MAGIC ### Tracking Dropped Rows
# MAGIC
# MAGIC Since DROPMALFORMED does not tell you what was dropped, you need to track it yourself:
# MAGIC
# MAGIC ```python
# MAGIC # Read with PERMISSIVE first to count
# MAGIC df_permissive = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode", "PERMISSIVE") \
# MAGIC     .load("/FileStore/test/bad_employees.csv")
# MAGIC
# MAGIC # Read with DROPMALFORMED
# MAGIC df_drop = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode", "DROPMALFORMED") \
# MAGIC     .load("/FileStore/test/bad_employees.csv")
# MAGIC
# MAGIC total_rows   = df_permissive.count()
# MAGIC clean_rows   = df_drop.count()
# MAGIC dropped_rows = total_rows - clean_rows
# MAGIC
# MAGIC print(f"Total rows read  : {total_rows}")
# MAGIC print(f"Clean rows kept  : {clean_rows}")
# MAGIC print(f"Rows dropped     : {dropped_rows}")
# MAGIC print(f"Drop percentage  : {round(dropped_rows/total_rows*100, 2)}%")
# MAGIC ```
# MAGIC
# MAGIC ### When to Use DROPMALFORMED
# MAGIC
# MAGIC ```
# MAGIC ✓ Bad rows are completely useless — no value in keeping them
# MAGIC ✓ You have high volume data — a few bad rows do not matter
# MAGIC ✓ Pipeline must continue without errors regardless of bad data
# MAGIC ✓ You are doing aggregate analysis where a few missing rows are acceptable
# MAGIC ✓ Upstream data quality is mostly good with rare exceptions
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Mode 3 — FAILFAST
# MAGIC
# MAGIC ### What it Does
# MAGIC
# MAGIC FAILFAST is the **zero tolerance** mode. When a bad row is encountered:
# MAGIC - Spark immediately throws an exception
# MAGIC - The entire job stops
# MAGIC - No data is processed or returned
# MAGIC - The error message tells you what went wrong
# MAGIC
# MAGIC ```python
# MAGIC try:
# MAGIC     df_fail = spark.read \
# MAGIC         .format("csv") \
# MAGIC         .option("header", "true") \
# MAGIC         .schema(schema) \
# MAGIC         .option("mode", "FAILFAST") \
# MAGIC         .load("/FileStore/test/bad_employees.csv")
# MAGIC
# MAGIC     # This action triggers the actual read
# MAGIC     df_fail.show()
# MAGIC
# MAGIC except Exception as e:
# MAGIC     print(f"FAILFAST caught an error:")
# MAGIC     print(f"{str(e)[:300]}")
# MAGIC ```
# MAGIC
# MAGIC Output:
# MAGIC ```
# MAGIC FAILFAST caught an error:
# MAGIC SparkException: Malformed records are detected in record parsing.
# MAGIC Parse Mode: FAILFAST. To process malformed records as null result,
# MAGIC try setting the option 'mode' as 'PERMISSIVE'.
# MAGIC ```
# MAGIC
# MAGIC ### FAILFAST on Good Data
# MAGIC
# MAGIC ```python
# MAGIC # FAILFAST works fine when data is clean
# MAGIC df_good = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode", "FAILFAST") \
# MAGIC     .load("/FileStore/test/good_employees.csv")
# MAGIC
# MAGIC print("=== FAILFAST ON GOOD DATA — WORKS PERFECTLY ===")
# MAGIC df_good.show()
# MAGIC ```
# MAGIC
# MAGIC ### When to Use FAILFAST
# MAGIC
# MAGIC ```
# MAGIC ✓ Data contract with upstream team guarantees clean data
# MAGIC ✓ Any corruption means something is seriously wrong
# MAGIC ✓ You need to alert immediately when data quality breaks
# MAGIC ✓ Financial or regulatory data where errors cannot be silent
# MAGIC ✓ Data is pre-validated before reaching Spark
# MAGIC ✓ You want the pipeline to fail loud rather than fail silent
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Read Modes with JSON
# MAGIC
# MAGIC Modes work exactly the same with JSON:
# MAGIC
# MAGIC ```python
# MAGIC # Create bad JSON
# MAGIC bad_json = """{"emp_id": 1, "name": "Alice", "salary": 95000.0}
# MAGIC {"emp_id": 2, "name": "Bob", "salary": "INVALID"}
# MAGIC {"emp_id": 3, "name": "Charlie"}
# MAGIC COMPLETELY BROKEN JSON LINE
# MAGIC {"emp_id": 5, "name": "Edward", "salary": 88000.0}"""
# MAGIC
# MAGIC dbutils.fs.put(
# MAGIC     "/FileStore/test/bad_employees.json",
# MAGIC     bad_json,
# MAGIC     overwrite=True
# MAGIC )
# MAGIC
# MAGIC json_schema = StructType([
# MAGIC     StructField("emp_id", IntegerType(), True),
# MAGIC     StructField("name",   StringType(),  True),
# MAGIC     StructField("salary", DoubleType(),  True)
# MAGIC ])
# MAGIC
# MAGIC # PERMISSIVE with JSON
# MAGIC df_json_p = spark.read \
# MAGIC     .format("json") \
# MAGIC     .schema(json_schema) \
# MAGIC     .option("mode", "PERMISSIVE") \
# MAGIC     .load("/FileStore/test/bad_employees.json")
# MAGIC
# MAGIC print("=== JSON PERMISSIVE ===")
# MAGIC df_json_p.show(truncate=False)
# MAGIC
# MAGIC # DROPMALFORMED with JSON
# MAGIC df_json_d = spark.read \
# MAGIC     .format("json") \
# MAGIC     .schema(json_schema) \
# MAGIC     .option("mode", "DROPMALFORMED") \
# MAGIC     .load("/FileStore/test/bad_employees.json")
# MAGIC
# MAGIC print("\n=== JSON DROPMALFORMED ===")
# MAGIC df_json_d.show(truncate=False)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Read Modes with Additional Options
# MAGIC
# MAGIC ### Combining Mode with Other Options
# MAGIC
# MAGIC ```python
# MAGIC # Complete production style read with mode
# MAGIC df = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header",       "true") \
# MAGIC     .option("mode",         "PERMISSIVE") \
# MAGIC     .option("columnNameOfCorruptRecord", "_corrupt_record") \
# MAGIC     .option("nullValue",    "N/A") \
# MAGIC     .option("emptyValue",   "") \
# MAGIC     .option("dateFormat",   "yyyy-MM-dd") \
# MAGIC     .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
# MAGIC     .option("encoding",     "UTF-8") \
# MAGIC     .schema(schema_with_corrupt) \
# MAGIC     .load("/FileStore/tables/BigMart.csv")
# MAGIC
# MAGIC df.show(5)
# MAGIC ```
# MAGIC
# MAGIC ### badRecordsPath — Save Bad Records to a Location
# MAGIC
# MAGIC ```python
# MAGIC # Save bad records to a separate path for investigation
# MAGIC df = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("badRecordsPath", "/FileStore/bad_records/") \
# MAGIC     .load("/FileStore/test/bad_employees.csv")
# MAGIC
# MAGIC df.show()
# MAGIC
# MAGIC # Read the bad records that were saved
# MAGIC print("\n=== SAVED BAD RECORDS ===")
# MAGIC df_bad_records = spark.read.json("/FileStore/bad_records/")
# MAGIC df_bad_records.show(truncate=False)
# MAGIC ```
# MAGIC
# MAGIC This is very useful in production — good records are processed normally and bad records are automatically saved to a separate location for investigation without stopping the pipeline.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Real World Strategy — BigMart Dataset
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql.functions import *
# MAGIC
# MAGIC bigmart_schema_with_corrupt = StructType([
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
# MAGIC     StructField("Item_Outlet_Sales",         DoubleType(),  True),
# MAGIC     StructField("_corrupt_record",           StringType(),  True)
# MAGIC ])
# MAGIC
# MAGIC # Step 1 — Read with PERMISSIVE to capture everything
# MAGIC print("=== STEP 1: READ WITH PERMISSIVE ===")
# MAGIC df_raw = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .option("mode", "PERMISSIVE") \
# MAGIC     .option("columnNameOfCorruptRecord", "_corrupt_record") \
# MAGIC     .schema(bigmart_schema_with_corrupt) \
# MAGIC     .load("/FileStore/tables/BigMart.csv")
# MAGIC
# MAGIC total = df_raw.count()
# MAGIC print(f"Total records read: {total}")
# MAGIC
# MAGIC # Step 2 — Separate good and corrupt
# MAGIC print("\n=== STEP 2: SEPARATE GOOD AND CORRUPT ===")
# MAGIC df_good    = df_raw.filter(col("_corrupt_record").isNull()) \
# MAGIC                    .drop("_corrupt_record")
# MAGIC
# MAGIC df_corrupt = df_raw.filter(col("_corrupt_record").isNotNull()) \
# MAGIC                    .select("_corrupt_record")
# MAGIC
# MAGIC good_count    = df_good.count()
# MAGIC corrupt_count = df_corrupt.count()
# MAGIC
# MAGIC print(f"Good records    : {good_count}")
# MAGIC print(f"Corrupt records : {corrupt_count}")
# MAGIC print(f"Corrupt rate    : {round(corrupt_count/total*100,2)}%")
# MAGIC
# MAGIC # Step 3 — Save corrupt for investigation
# MAGIC if corrupt_count > 0:
# MAGIC     print("\n=== STEP 3: SAVING CORRUPT RECORDS ===")
# MAGIC     df_corrupt.write \
# MAGIC         .mode("overwrite") \
# MAGIC         .format("csv") \
# MAGIC         .option("header", "true") \
# MAGIC         .save("/FileStore/bigmart_corrupt/")
# MAGIC     print(f"Corrupt records saved to /FileStore/bigmart_corrupt/")
# MAGIC
# MAGIC     print("\nCorrupt records:")
# MAGIC     df_corrupt.show(truncate=False)
# MAGIC
# MAGIC # Step 4 — Process good data
# MAGIC print("\n=== STEP 4: PROCESSING GOOD DATA ===")
# MAGIC df_final = df_good \
# MAGIC     .withColumn("Item_Fat_Content",
# MAGIC         when(col("Item_Fat_Content").isin("LF","low fat"), "Low Fat")
# MAGIC         .when(col("Item_Fat_Content") == "reg", "Regular")
# MAGIC         .otherwise(col("Item_Fat_Content"))
# MAGIC     ) \
# MAGIC     .withColumn("Outlet_Age",
# MAGIC         lit(2024) - col("Outlet_Establishment_Year")
# MAGIC     ) \
# MAGIC     .withColumn("MRP_Category",
# MAGIC         when(col("Item_MRP") > 200, "Premium")
# MAGIC         .when(col("Item_MRP") > 100, "Mid Range")
# MAGIC         .otherwise("Budget")
# MAGIC     )
# MAGIC
# MAGIC df_final.show(5, truncate=False)
# MAGIC
# MAGIC # Step 5 — Write clean data
# MAGIC print("\n=== STEP 5: WRITING CLEAN DATA ===")
# MAGIC df_final.write \
# MAGIC     .mode("overwrite") \
# MAGIC     .format("delta") \
# MAGIC     .save("/FileStore/bigmart_clean/")
# MAGIC
# MAGIC print("Clean data written to Delta")
# MAGIC print(f"Final clean record count: {df_final.count()}")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Choosing the Right Mode — Decision Flow
# MAGIC
# MAGIC ```
# MAGIC New dataset arriving
# MAGIC         │
# MAGIC         ▼
# MAGIC Is this for exploration or production?
# MAGIC         │
# MAGIC    ┌────┴────┐
# MAGIC    │         │
# MAGIC Explore   Production
# MAGIC    │         │
# MAGIC    ▼         ▼
# MAGIC PERMISSIVE  Is data guaranteed clean?
# MAGIC             │
# MAGIC        ┌────┴────┐
# MAGIC        │         │
# MAGIC       Yes        No
# MAGIC        │         │
# MAGIC        ▼         ▼
# MAGIC    FAILFAST   Do bad rows have any value?
# MAGIC                    │
# MAGIC               ┌────┴────┐
# MAGIC               │         │
# MAGIC              Yes        No
# MAGIC               │         │
# MAGIC               ▼         ▼
# MAGIC           PERMISSIVE  DROPMALFORMED
# MAGIC          + corrupt    (or PERMISSIVE
# MAGIC            column      + badRecordsPath)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## All Three Modes — Side by Side
# MAGIC
# MAGIC ```python
# MAGIC path = "/FileStore/test/bad_employees.csv"
# MAGIC
# MAGIC # PERMISSIVE
# MAGIC df_p = spark.read.format("csv") \
# MAGIC     .option("header","true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode","PERMISSIVE") \
# MAGIC     .load(path)
# MAGIC
# MAGIC # DROPMALFORMED
# MAGIC df_d = spark.read.format("csv") \
# MAGIC     .option("header","true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode","DROPMALFORMED") \
# MAGIC     .load(path)
# MAGIC
# MAGIC # FAILFAST — wrapped in try
# MAGIC try:
# MAGIC     df_f = spark.read.format("csv") \
# MAGIC         .option("header","true") \
# MAGIC         .schema(schema) \
# MAGIC         .option("mode","FAILFAST") \
# MAGIC         .load(path)
# MAGIC     df_f.count()
# MAGIC     failfast_count = df_f.count()
# MAGIC except Exception as e:
# MAGIC     failfast_count = "FAILED"
# MAGIC
# MAGIC print("=" * 50)
# MAGIC print("       READ MODES COMPARISON")
# MAGIC print("=" * 50)
# MAGIC print(f"PERMISSIVE    : {df_p.count()} rows (bad fields = null)")
# MAGIC print(f"DROPMALFORMED : {df_d.count()} rows (bad rows removed)")
# MAGIC print(f"FAILFAST      : {failfast_count}")
# MAGIC print("=" * 50)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Quick Reference
# MAGIC
# MAGIC | Mode | Bad Row Handling | Job Continues | Bad Data Visible | Use Case |
# MAGIC |---|---|---|---|---|
# MAGIC | PERMISSIVE | Keep row, null bad fields | Yes | Yes — as nulls | Exploration, data quality analysis |
# MAGIC | DROPMALFORMED | Delete row entirely | Yes | No — silently gone | High volume, few bad rows acceptable |
# MAGIC | FAILFAST | Throw exception immediately | No | Yes — as error | Strict data contracts, financial data |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## One Line to Remember
# MAGIC
# MAGIC Read modes are your **corruption handling strategy at the point of ingestion** — use **PERMISSIVE** when you want to see and handle bad data yourself, use **DROPMALFORMED** when bad rows are worthless and should be silently ignored, use **FAILFAST** when any corruption means something is seriously wrong and the pipeline must stop immediately — and in production always use PERMISSIVE with a corrupt record column so you have full visibility into what bad data arrived and can act on it.