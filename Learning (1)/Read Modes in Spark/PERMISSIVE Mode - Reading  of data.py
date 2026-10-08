# Databricks notebook source
# MAGIC %md
# MAGIC # PERMISSIVE Mode — Complete Deep Dive
# MAGIC
# MAGIC ## What is PERMISSIVE Mode?
# MAGIC
# MAGIC PERMISSIVE is Spark's **default and most forgiving read mode**. The philosophy behind it is simple — never throw away data, never crash the job, bring everything in and let the engineer decide what to do with bad records.
# MAGIC
# MAGIC When Spark encounters a row that does not match the defined schema in PERMISSIVE mode, it does not panic. It does not stop. It does not silently delete the row. Instead it keeps the row, sets the problematic field to **null**, and moves on. The job continues processing all remaining rows normally.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Why PERMISSIVE Exists
# MAGIC
# MAGIC In real world data engineering, data comes from sources you do not fully control — vendors, third party APIs, legacy systems, manual uploads. This data is almost never perfectly clean. If Spark crashed every time it saw a bad value, most production pipelines would never complete.
# MAGIC
# MAGIC PERMISSIVE was designed with the philosophy that **some data is better than no data**. Even a partially parsed row with some nulls can be useful. You can investigate it, fix it, or route it to a quarantine table — but you cannot do anything if the entire job crashed before you even saw the problem.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exactly What Happens Internally
# MAGIC
# MAGIC When Spark reads a row in PERMISSIVE mode, it processes each field one by one:
# MAGIC
# MAGIC ```
# MAGIC Row arrives from source
# MAGIC         │
# MAGIC         ▼
# MAGIC Field 1 — can it be parsed to the schema type?
# MAGIC    ├── YES → write the parsed value
# MAGIC    └── NO  → write null for this field, continue
# MAGIC         │
# MAGIC         ▼
# MAGIC Field 2 — can it be parsed?
# MAGIC    ├── YES → write the parsed value
# MAGIC    └── NO  → write null, continue
# MAGIC         │
# MAGIC         ▼
# MAGIC ... repeat for every field ...
# MAGIC         │
# MAGIC         ▼
# MAGIC Row is added to DataFrame regardless
# MAGIC (even if every single field is null)
# MAGIC ```
# MAGIC
# MAGIC The row is never rejected. Only individual fields that fail parsing become null. Fields that parse correctly keep their values.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Setup — Creating Realistic Bad Data
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # CREATE REALISTIC BAD DATA FILE
# MAGIC # ============================================================
# MAGIC
# MAGIC bad_data = """emp_id,name,department,salary,age,join_date,is_active
# MAGIC 1,Alice,Engineering,95000.0,32,2019-03-15,true
# MAGIC 2,Bob,Marketing,INVALID_SALARY,26,2021-07-22,true
# MAGIC 3,Charlie,Engineering,120000.0,38,2017-01-10,true
# MAGIC NOT_A_NUMBER,Diana,HR,75000.0,35,2018-11-05,true
# MAGIC 5,Edward,Finance,88000.0,WRONG_AGE,2019-09-30,true
# MAGIC 6,Fiona,Engineering,92000.0,31,BAD_DATE,false
# MAGIC 7,George,Marketing,105000.0,42,2016-06-14,WRONG_BOOLEAN
# MAGIC 8,Hannah,HR,INVALID,ALSO_INVALID,ALSO_BAD,ALSO_WRONG
# MAGIC 9,Ivan,Finance,115000.0,45,2015-12-01,false"""
# MAGIC
# MAGIC dbutils.fs.put(
# MAGIC     "/FileStore/test/bad_employees.csv",
# MAGIC     bad_data,
# MAGIC     overwrite=True
# MAGIC )
# MAGIC
# MAGIC print("Bad data file created with:")
# MAGIC print("  Row 2  — invalid salary (string instead of double)")
# MAGIC print("  Row 4  — invalid emp_id (string instead of integer)")
# MAGIC print("  Row 5  — invalid age (string instead of integer)")
# MAGIC print("  Row 6  — invalid date format")
# MAGIC print("  Row 7  — invalid boolean")
# MAGIC print("  Row 8  — almost everything invalid")
# MAGIC print("  Row 9  — all good")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Schema Definition
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.types import (
# MAGIC     StructType, StructField,
# MAGIC     IntegerType, StringType,
# MAGIC     DoubleType, BooleanType, DateType
# MAGIC )
# MAGIC from pyspark.sql.functions import (
# MAGIC     col, count, when, isnull,
# MAGIC     isnotnull, sum as spark_sum
# MAGIC )
# MAGIC
# MAGIC # Clean schema — no corrupt column yet
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
# MAGIC ## Part 1 — Basic PERMISSIVE
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # BASIC PERMISSIVE READ
# MAGIC # ============================================================
# MAGIC
# MAGIC df_permissive = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode", "PERMISSIVE") \
# MAGIC     .load("/FileStore/test/bad_employees.csv")
# MAGIC
# MAGIC print("=== PERMISSIVE MODE — ALL ROWS ===")
# MAGIC df_permissive.show(truncate=False)
# MAGIC print(f"Total rows: {df_permissive.count()}")
# MAGIC ```
# MAGIC
# MAGIC Output:
# MAGIC ```
# MAGIC +------+-------+-----------+--------+----+----------+---------+
# MAGIC |emp_id|name   |department |salary  |age |join_date |is_active|
# MAGIC +------+-------+-----------+--------+----+----------+---------+
# MAGIC |1     |Alice  |Engineering|95000.0 |32  |2019-03-15|true     |
# MAGIC |2     |Bob    |Marketing  |null    |26  |2021-07-22|true     | ← salary null
# MAGIC |3     |Charlie|Engineering|120000.0|38  |2017-01-10|true     |
# MAGIC |null  |Diana  |HR         |75000.0 |35  |2018-11-05|true     | ← emp_id null
# MAGIC |5     |Edward |Finance    |88000.0 |null|2019-09-30|true     | ← age null
# MAGIC |6     |Fiona  |Engineering|92000.0 |31  |null      |false    | ← date null
# MAGIC |7     |George |Marketing  |105000.0|42  |2016-06-14|null     | ← boolean null
# MAGIC |8     |Hannah |HR         |null    |null|null      |null     | ← all null
# MAGIC |9     |Ivan   |Finance    |115000.0|45  |2015-12-01|false    |
# MAGIC +------+-------+-----------+--------+----+----------+---------+
# MAGIC Total rows: 9
# MAGIC ```
# MAGIC
# MAGIC All 9 rows kept. Bad fields became null. Job never stopped.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 2 — Understanding What Became Null and Why
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # ANALYSE WHAT IS NULL AND WHY
# MAGIC # ============================================================
# MAGIC
# MAGIC print("=== NULL COUNT PER COLUMN ===")
# MAGIC df_permissive.select([
# MAGIC     count(when(isnull(col(c)), c)).alias(c)
# MAGIC     for c in df_permissive.columns
# MAGIC ]).show()
# MAGIC
# MAGIC print("=== NULL PERCENTAGE PER COLUMN ===")
# MAGIC total = df_permissive.count()
# MAGIC df_permissive.select([
# MAGIC     (
# MAGIC         count(when(isnull(col(c)), c)) / total * 100
# MAGIC     ).alias(c)
# MAGIC     for c in df_permissive.columns
# MAGIC ]).show()
# MAGIC
# MAGIC print("\n=== ROWS WITH AT LEAST ONE NULL ===")
# MAGIC df_with_nulls = df_permissive.filter(
# MAGIC     isnull(col("emp_id"))    |
# MAGIC     isnull(col("salary"))    |
# MAGIC     isnull(col("age"))       |
# MAGIC     isnull(col("join_date")) |
# MAGIC     isnull(col("is_active"))
# MAGIC )
# MAGIC df_with_nulls.show(truncate=False)
# MAGIC print(f"Rows with nulls: {df_with_nulls.count()}")
# MAGIC
# MAGIC print("\n=== PERFECTLY CLEAN ROWS ===")
# MAGIC df_clean = df_permissive.filter(
# MAGIC     isnotnull(col("emp_id"))    &
# MAGIC     isnotnull(col("name"))      &
# MAGIC     isnotnull(col("salary"))    &
# MAGIC     isnotnull(col("age"))       &
# MAGIC     isnotnull(col("join_date")) &
# MAGIC     isnotnull(col("is_active"))
# MAGIC )
# MAGIC df_clean.show(truncate=False)
# MAGIC print(f"Clean rows: {df_clean.count()}")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 3 — PERMISSIVE with Corrupt Record Column
# MAGIC
# MAGIC This is the most powerful feature of PERMISSIVE mode. The `_corrupt_record` column captures the **entire original raw row** as a string exactly as it came from the source — before any parsing was attempted.
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # SCHEMA WITH CORRUPT RECORD COLUMN
# MAGIC # ============================================================
# MAGIC
# MAGIC schema_with_corrupt = StructType([
# MAGIC     StructField("emp_id",          IntegerType(), True),
# MAGIC     StructField("name",            StringType(),  True),
# MAGIC     StructField("department",      StringType(),  True),
# MAGIC     StructField("salary",          DoubleType(),  True),
# MAGIC     StructField("age",             IntegerType(), True),
# MAGIC     StructField("join_date",       DateType(),    True),
# MAGIC     StructField("is_active",       BooleanType(), True),
# MAGIC     StructField("_corrupt_record", StringType(),  True)
# MAGIC     # Must be last
# MAGIC     # Must be StringType
# MAGIC     # Name must match columnNameOfCorruptRecord option
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
# MAGIC print("=== ALL ROWS WITH CORRUPT COLUMN ===")
# MAGIC df_with_corrupt.show(truncate=False)
# MAGIC ```
# MAGIC
# MAGIC Output:
# MAGIC ```
# MAGIC +------+-------+-----------+--------+----+----------+---------+------------------------------------------------+
# MAGIC |emp_id|name   |department |salary  |age |join_date |is_active|_corrupt_record                                 |
# MAGIC +------+-------+-----------+--------+----+----------+---------+------------------------------------------------+
# MAGIC |1     |Alice  |Engineering|95000.0 |32  |2019-03-15|true     |null                                            |
# MAGIC |2     |Bob    |Marketing  |null    |26  |2021-07-22|true     |2,Bob,Marketing,INVALID_SALARY,26,2021-07-22,true|
# MAGIC |3     |Charlie|Engineering|120000.0|38  |2017-01-10|true     |null                                            |
# MAGIC |null  |Diana  |HR         |75000.0 |35  |2018-11-05|true     |NOT_A_NUMBER,Diana,HR,75000.0,35,2018-11-05,true|
# MAGIC |5     |Edward |Finance    |88000.0 |null|2019-09-30|true     |5,Edward,Finance,88000.0,WRONG_AGE,2019-09-30...|
# MAGIC |6     |Fiona  |Engineering|92000.0 |31  |null      |false    |6,Fiona,Engineering,92000.0,31,BAD_DATE,false   |
# MAGIC |7     |George |Marketing  |105000.0|42  |2016-06-14|null     |7,George,Marketing,105000.0,42,2016-06-14,WRO...|
# MAGIC |8     |Hannah |HR         |null    |null|null      |null     |8,Hannah,HR,INVALID,ALSO_INVALID,ALSO_BAD,ALS...|
# MAGIC |9     |Ivan   |Finance    |115000.0|45  |2015-12-01|false    |null                                            |
# MAGIC +------+-------+-----------+--------+----+----------+---------+------------------------------------------------+
# MAGIC ```
# MAGIC
# MAGIC Notice — when `_corrupt_record` is null it means the row was clean. When it has a value it means that row had at least one parsing problem and the raw original row is captured there.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 4 — Separating Good and Bad Rows
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # SEPARATE GOOD ROWS FROM BAD ROWS
# MAGIC # ============================================================
# MAGIC
# MAGIC # GOOD ROWS — no corruption
# MAGIC df_good = df_with_corrupt \
# MAGIC     .filter(col("_corrupt_record").isNull()) \
# MAGIC     .drop("_corrupt_record")   # Remove the corrupt column from good data
# MAGIC
# MAGIC # BAD ROWS — at least one field was corrupt
# MAGIC df_bad = df_with_corrupt \
# MAGIC     .filter(col("_corrupt_record").isNotNull())
# MAGIC
# MAGIC print("=== GOOD ROWS — READY FOR PROCESSING ===")
# MAGIC df_good.show(truncate=False)
# MAGIC
# MAGIC print("\n=== BAD ROWS — NEED INVESTIGATION ===")
# MAGIC df_bad.show(truncate=False)
# MAGIC
# MAGIC # Summary
# MAGIC total         = df_with_corrupt.count()
# MAGIC good_count    = df_good.count()
# MAGIC bad_count     = df_bad.count()
# MAGIC good_pct      = round(good_count / total * 100, 1)
# MAGIC bad_pct       = round(bad_count  / total * 100, 1)
# MAGIC
# MAGIC print("\n=== SUMMARY ===")
# MAGIC print(f"Total rows read    : {total}")
# MAGIC print(f"Good rows          : {good_count} ({good_pct}%)")
# MAGIC print(f"Corrupt rows       : {bad_count}  ({bad_pct}%)")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 5 — Investigating Corrupt Rows in Detail
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # DEEP INVESTIGATE CORRUPT ROWS
# MAGIC # ============================================================
# MAGIC
# MAGIC print("=== RAW CORRUPT RECORDS ===")
# MAGIC df_bad.select("_corrupt_record").show(truncate=False)
# MAGIC
# MAGIC print("\n=== CORRUPT ROWS — WHAT WAS SALVAGED ===")
# MAGIC # Even corrupt rows may have some valid fields
# MAGIC df_bad.select(
# MAGIC     "emp_id",
# MAGIC     "name",
# MAGIC     "department",
# MAGIC     "salary",
# MAGIC     "age",
# MAGIC     "join_date",
# MAGIC     "is_active",
# MAGIC     "_corrupt_record"
# MAGIC ).show(truncate=False)
# MAGIC
# MAGIC # Which fields are null in corrupt rows
# MAGIC print("\n=== NULL ANALYSIS ON CORRUPT ROWS ONLY ===")
# MAGIC df_bad.select([
# MAGIC     count(when(isnull(col(c)), c)).alias(c)
# MAGIC     for c in df_bad.columns
# MAGIC     if c != "_corrupt_record"
# MAGIC ]).show()
# MAGIC
# MAGIC # For each corrupt row — identify which field caused the issue
# MAGIC print("\n=== FIELD BY FIELD CORRUPTION ANALYSIS ===")
# MAGIC df_bad.select(
# MAGIC     col("_corrupt_record").alias("raw_row"),
# MAGIC     when(isnull(col("emp_id")),     "emp_id")
# MAGIC     .when(isnull(col("salary")),    "salary")
# MAGIC     .when(isnull(col("age")),       "age")
# MAGIC     .when(isnull(col("join_date")), "join_date")
# MAGIC     .when(isnull(col("is_active")), "is_active")
# MAGIC     .otherwise("Multiple fields")
# MAGIC     .alias("corrupted_field")
# MAGIC ).show(truncate=False)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 6 — Save and Quarantine Bad Records
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # QUARANTINE BAD RECORDS
# MAGIC # ============================================================
# MAGIC
# MAGIC from pyspark.sql.functions import current_timestamp, lit
# MAGIC
# MAGIC # Add metadata to bad records before saving
# MAGIC df_bad_to_save = df_bad.select(
# MAGIC     col("_corrupt_record").alias("raw_record"),
# MAGIC     lit("bad_employees.csv").alias("source_file"),
# MAGIC     current_timestamp().alias("detected_at"),
# MAGIC     lit("SCHEMA_MISMATCH").alias("error_type")
# MAGIC )
# MAGIC
# MAGIC print("=== BAD RECORDS TO BE QUARANTINED ===")
# MAGIC df_bad_to_save.show(truncate=False)
# MAGIC
# MAGIC # Save to quarantine location
# MAGIC df_bad_to_save.write \
# MAGIC     .mode("overwrite") \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .save("/FileStore/quarantine/bad_employees/")
# MAGIC
# MAGIC print("Bad records saved to quarantine")
# MAGIC
# MAGIC # Save good records to processing pipeline
# MAGIC df_good.write \
# MAGIC     .mode("overwrite") \
# MAGIC     .format("delta") \
# MAGIC     .save("/FileStore/clean/employees/")
# MAGIC
# MAGIC print("Good records saved to clean zone")
# MAGIC
# MAGIC # Verify
# MAGIC df_quarantine = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .load("/FileStore/quarantine/bad_employees/")
# MAGIC
# MAGIC df_clean_zone = spark.read \
# MAGIC     .format("delta") \
# MAGIC     .load("/FileStore/clean/employees/")
# MAGIC
# MAGIC print(f"\nQuarantine records : {df_quarantine.count()}")
# MAGIC print(f"Clean records      : {df_clean_zone.count()}")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 7 — PERMISSIVE on BigMart Dataset
# MAGIC
# MAGIC Applying everything on our real BigMart dataset:
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # BIGMART — PERMISSIVE WITH FULL PIPELINE
# MAGIC # ============================================================
# MAGIC
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql.functions import *
# MAGIC
# MAGIC bigmart_schema_corrupt = StructType([
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
# MAGIC # Step 1 — Read
# MAGIC df_raw = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .option("mode", "PERMISSIVE") \
# MAGIC     .option("columnNameOfCorruptRecord", "_corrupt_record") \
# MAGIC     .schema(bigmart_schema_corrupt) \
# MAGIC     .load("/FileStore/tables/BigMart.csv")
# MAGIC
# MAGIC # Step 2 — Split
# MAGIC df_good    = df_raw.filter(col("_corrupt_record").isNull()) \
# MAGIC                    .drop("_corrupt_record")
# MAGIC df_corrupt = df_raw.filter(col("_corrupt_record").isNotNull())
# MAGIC
# MAGIC # Step 3 — Report
# MAGIC total   = df_raw.count()
# MAGIC good    = df_good.count()
# MAGIC corrupt = df_corrupt.count()
# MAGIC
# MAGIC print("=" * 55)
# MAGIC print("      BIGMART PERMISSIVE READ REPORT")
# MAGIC print("=" * 55)
# MAGIC print(f"  Total records read   : {total:,}")
# MAGIC print(f"  Good records         : {good:,}")
# MAGIC print(f"  Corrupt records      : {corrupt:,}")
# MAGIC print(f"  Data quality rate    : {round(good/total*100,2)}%")
# MAGIC print("=" * 55)
# MAGIC
# MAGIC # Step 4 — Null analysis on good data
# MAGIC print("\n=== NULL COUNT IN GOOD DATA ===")
# MAGIC df_good.select([
# MAGIC     count(when(isnull(col(c)), c)).alias(c)
# MAGIC     for c in df_good.columns
# MAGIC ]).show()
# MAGIC
# MAGIC # Step 5 — Show corrupt if any
# MAGIC if corrupt > 0:
# MAGIC     print("\n=== CORRUPT RECORDS ===")
# MAGIC     df_corrupt.select("_corrupt_record").show(truncate=False)
# MAGIC else:
# MAGIC     print("\nBigMart data is fully clean — no corrupt records")
# MAGIC
# MAGIC # Step 6 — Proceed with good data
# MAGIC df_good.show(5, truncate=False)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 8 — When PERMISSIVE Silently Creates Problems
# MAGIC
# MAGIC This is the most important thing to understand about PERMISSIVE — it is lenient but that leniency can mask real problems.
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # THE HIDDEN DANGER OF PERMISSIVE
# MAGIC # ============================================================
# MAGIC
# MAGIC # Scenario — someone sends wrong file
# MAGIC # All numeric columns are strings in wrong file
# MAGIC wrong_data = """emp_id,name,department,salary,age,join_date,is_active
# MAGIC one,Alice,Engineering,ninety-five thousand,thirty-two,sometime in March,yes
# MAGIC two,Bob,Marketing,fifty-two thousand,twenty-six,sometime in July,yes"""
# MAGIC
# MAGIC dbutils.fs.put(
# MAGIC     "/FileStore/test/completely_wrong.csv",
# MAGIC     wrong_data,
# MAGIC     overwrite=True
# MAGIC )
# MAGIC
# MAGIC # PERMISSIVE reads it without any error
# MAGIC df_wrong = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode", "PERMISSIVE") \
# MAGIC     .load("/FileStore/test/completely_wrong.csv")
# MAGIC
# MAGIC print("=== PERMISSIVE ON COMPLETELY WRONG FILE ===")
# MAGIC df_wrong.show(truncate=False)
# MAGIC print(f"Row count: {df_wrong.count()}")
# MAGIC # Output shows 2 rows — but almost everything is null
# MAGIC # Job did NOT fail — it silently produced useless data
# MAGIC ```
# MAGIC
# MAGIC Output:
# MAGIC ```
# MAGIC +------+-----+-----------+------+----+---------+---------+
# MAGIC |emp_id|name |department |salary|age |join_date|is_active|
# MAGIC +------+-----+-----------+------+----+---------+---------+
# MAGIC |null  |Alice|Engineering|null  |null|null     |null     |
# MAGIC |null  |Bob  |Marketing  |null  |null|null     |null     |
# MAGIC +------+-----+-----------+------+----+---------+---------+
# MAGIC ```
# MAGIC
# MAGIC The job succeeded. No errors. But the data is almost entirely null and useless. This is why PERMISSIVE alone is not enough — you must always check null counts after reading.
# MAGIC
# MAGIC ```python
# MAGIC # ALWAYS DO THIS AFTER PERMISSIVE READ
# MAGIC def check_data_quality(df, threshold_pct=50):
# MAGIC     """
# MAGIC     After PERMISSIVE read, check if null rate is acceptable
# MAGIC     Raise alert if any column exceeds threshold
# MAGIC     """
# MAGIC     total = df.count()
# MAGIC     print("\n=== DATA QUALITY CHECK AFTER PERMISSIVE READ ===")
# MAGIC
# MAGIC     alerts = []
# MAGIC
# MAGIC     for c in df.columns:
# MAGIC         null_count = df.filter(isnull(col(c))).count()
# MAGIC         null_pct   = round(null_count / total * 100, 1)
# MAGIC         status     = "OK" if null_pct < threshold_pct else "ALERT"
# MAGIC
# MAGIC         print(f"  {c:<30} nulls: {null_count:>5} ({null_pct:>5}%)  {status}")
# MAGIC
# MAGIC         if null_pct >= threshold_pct:
# MAGIC             alerts.append(f"{c} has {null_pct}% nulls")
# MAGIC
# MAGIC     if alerts:
# MAGIC         print(f"\nDATA QUALITY ALERTS:")
# MAGIC         for a in alerts:
# MAGIC             print(f"  WARNING: {a}")
# MAGIC     else:
# MAGIC         print("\nAll columns within acceptable null threshold")
# MAGIC
# MAGIC # Run on PERMISSIVE result
# MAGIC check_data_quality(df_permissive, threshold_pct=30)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 9 — PERMISSIVE vs No Schema — Important Difference
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # PERMISSIVE WITH SCHEMA vs WITHOUT SCHEMA
# MAGIC # ============================================================
# MAGIC
# MAGIC # WITHOUT schema — inferSchema guesses types
# MAGIC # Bad values are read as strings — no nulls created
# MAGIC df_no_schema = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .option("inferSchema", "true") \
# MAGIC     .load("/FileStore/test/bad_employees.csv")
# MAGIC
# MAGIC print("=== NO SCHEMA — inferSchema ===")
# MAGIC df_no_schema.show(truncate=False)
# MAGIC df_no_schema.printSchema()
# MAGIC # INVALID_SALARY column becomes StringType
# MAGIC # NOT_A_NUMBER emp_id column infers as StringType
# MAGIC # Everything is read — no nulls — but types are wrong
# MAGIC
# MAGIC # WITH schema and PERMISSIVE
# MAGIC # Spark tries to cast to defined types
# MAGIC # Failed casts become null
# MAGIC df_with_schema = spark.read \
# MAGIC     .format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .schema(schema) \
# MAGIC     .option("mode", "PERMISSIVE") \
# MAGIC     .load("/FileStore/test/bad_employees.csv")
# MAGIC
# MAGIC print("\n=== WITH SCHEMA + PERMISSIVE ===")
# MAGIC df_with_schema.show(truncate=False)
# MAGIC df_with_schema.printSchema()
# MAGIC # INVALID_SALARY → null (correct type enforced, bad value becomes null)
# MAGIC # NOT_A_NUMBER → null (correct type enforced, bad value becomes null)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## When to Use PERMISSIVE — Detailed Guide
# MAGIC
# MAGIC ```
# MAGIC USE PERMISSIVE WHEN:
# MAGIC
# MAGIC 1. FIRST TIME EXPLORING A DATASET
# MAGIC    You do not know what corruption exists.
# MAGIC    PERMISSIVE lets you read everything first,
# MAGIC    then investigate what went wrong.
# MAGIC
# MAGIC 2. BUILDING DATA QUALITY REPORTS
# MAGIC    You want to measure how dirty the data is.
# MAGIC    PERMISSIVE gives you all rows including bad ones.
# MAGIC    You can then count nulls and report quality metrics.
# MAGIC
# MAGIC 3. SEPARATING GOOD AND BAD RECORDS
# MAGIC    With _corrupt_record column you can split the data.
# MAGIC    Good rows go to the main pipeline.
# MAGIC    Bad rows go to a quarantine table for review.
# MAGIC
# MAGIC 4. DATA IS EXPECTED TO HAVE SOME BAD ROWS
# MAGIC    Upstream sends 99% good data with occasional errors.
# MAGIC    You want to process the 99% without stopping for 1%.
# MAGIC    PERMISSIVE keeps the pipeline running.
# MAGIC
# MAGIC 5. DOWNSTREAM LOGIC CAN HANDLE NULLS
# MAGIC    Your transformations use coalesce, fillna, when isNull.
# MAGIC    Nulls are expected and handled gracefully downstream.
# MAGIC
# MAGIC 6. INVESTIGATION AND DEBUGGING
# MAGIC    Something is wrong with the data.
# MAGIC    You want to bring it all in and look at it.
# MAGIC    PERMISSIVE is perfect for this.
# MAGIC
# MAGIC DO NOT USE PERMISSIVE ALONE WHEN:
# MAGIC
# MAGIC 1. NULLS ARE UNACCEPTABLE IN RESULTS
# MAGIC    Financial reporting, regulatory submissions.
# MAGIC    Use FAILFAST or add manual null validation after read.
# MAGIC
# MAGIC 2. WRONG FILE WAS SENT
# MAGIC    PERMISSIVE will silently create a DataFrame full of nulls.
# MAGIC    Always add null percentage check after PERMISSIVE read.
# MAGIC
# MAGIC 3. YOU NEED GUARANTEED DATA QUALITY
# MAGIC    Combine PERMISSIVE with validation functions
# MAGIC    to catch when null rate exceeds acceptable threshold.
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Complete PERMISSIVE Template for Production
# MAGIC
# MAGIC ```python
# MAGIC # ============================================================
# MAGIC # PRODUCTION PERMISSIVE READ TEMPLATE
# MAGIC # Copy and use for any dataset
# MAGIC # ============================================================
# MAGIC
# MAGIC def read_with_permissive(
# MAGIC         path,
# MAGIC         schema_with_corrupt,
# MAGIC         source_name,
# MAGIC         max_corrupt_pct=5.0
# MAGIC ):
# MAGIC     """
# MAGIC     Production grade PERMISSIVE read
# MAGIC     Reads data, separates good and bad,
# MAGIC     raises alert if corruption exceeds threshold
# MAGIC     """
# MAGIC
# MAGIC     print(f"\n{'='*55}")
# MAGIC     print(f"  READING: {source_name}")
# MAGIC     print(f"{'='*55}")
# MAGIC
# MAGIC     # Read with PERMISSIVE
# MAGIC     df_raw = spark.read \
# MAGIC         .format("csv") \
# MAGIC         .option("header", "true") \
# MAGIC         .option("mode", "PERMISSIVE") \
# MAGIC         .option("columnNameOfCorruptRecord", "_corrupt_record") \
# MAGIC         .schema(schema_with_corrupt) \
# MAGIC         .load(path)
# MAGIC
# MAGIC     total   = df_raw.count()
# MAGIC     df_good = df_raw.filter(col("_corrupt_record").isNull()) \
# MAGIC                     .drop("_corrupt_record")
# MAGIC     df_bad  = df_raw.filter(col("_corrupt_record").isNotNull())
# MAGIC
# MAGIC     good_count    = df_good.count()
# MAGIC     corrupt_count = df_bad.count()
# MAGIC     corrupt_pct   = round(corrupt_count / total * 100, 2)
# MAGIC
# MAGIC     # Report
# MAGIC     print(f"  Total records    : {total:,}")
# MAGIC     print(f"  Good records     : {good_count:,}")
# MAGIC     print(f"  Corrupt records  : {corrupt_count:,}")
# MAGIC     print(f"  Corruption rate  : {corrupt_pct}%")
# MAGIC     print(f"  Threshold        : {max_corrupt_pct}%")
# MAGIC
# MAGIC     # Save corrupt records to quarantine
# MAGIC     if corrupt_count > 0:
# MAGIC         quarantine_path = f"/FileStore/quarantine/{source_name}"
# MAGIC         df_bad.select(
# MAGIC             col("_corrupt_record").alias("raw_record"),
# MAGIC             lit(source_name).alias("source"),
# MAGIC             current_timestamp().alias("detected_at")
# MAGIC         ).write \
# MAGIC          .mode("overwrite") \
# MAGIC          .format("delta") \
# MAGIC          .save(quarantine_path)
# MAGIC         print(f"  Corrupt records saved to: {quarantine_path}")
# MAGIC
# MAGIC     # Alert if corruption too high
# MAGIC     if corrupt_pct > max_corrupt_pct:
# MAGIC         raise ValueError(
# MAGIC             f"Corruption rate {corrupt_pct}% exceeds "
# MAGIC             f"threshold {max_corrupt_pct}% for {source_name}. "
# MAGIC             f"Pipeline stopped."
# MAGIC         )
# MAGIC
# MAGIC     print(f"  Status: PASSED — proceeding with {good_count:,} good records")
# MAGIC     print(f"{'='*55}\n")
# MAGIC
# MAGIC     return df_good
# MAGIC
# MAGIC
# MAGIC # ── Use the template ─────────────────────────────────────────
# MAGIC
# MAGIC schema_with_corrupt = StructType([
# MAGIC     StructField("emp_id",          IntegerType(), True),
# MAGIC     StructField("name",            StringType(),  True),
# MAGIC     StructField("department",      StringType(),  True),
# MAGIC     StructField("salary",          DoubleType(),  True),
# MAGIC     StructField("age",             IntegerType(), True),
# MAGIC     StructField("join_date",       DateType(),    True),
# MAGIC     StructField("is_active",       BooleanType(), True),
# MAGIC     StructField("_corrupt_record", StringType(),  True)
# MAGIC ])
# MAGIC
# MAGIC df_clean = read_with_permissive(
# MAGIC     path            = "/FileStore/test/bad_employees.csv",
# MAGIC     schema_with_corrupt = schema_with_corrupt,
# MAGIC     source_name     = "employees",
# MAGIC     max_corrupt_pct = 20.0
# MAGIC )
# MAGIC
# MAGIC # Continue pipeline with clean data
# MAGIC df_clean.show(truncate=False)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## PERMISSIVE — Key Rules to Always Remember
# MAGIC
# MAGIC ```
# MAGIC Rule 1 — PERMISSIVE is the DEFAULT
# MAGIC You do not have to specify it.
# MAGIC spark.read.format("csv").schema(s).load(p)
# MAGIC is the same as adding .option("mode","PERMISSIVE")
# MAGIC
# MAGIC Rule 2 — Bad FIELDS become null, not bad ROWS
# MAGIC The row stays. Only the unparseable field becomes null.
# MAGIC
# MAGIC Rule 3 — _corrupt_record must be in schema
# MAGIC If you want to use columnNameOfCorruptRecord,
# MAGIC the column must be added to the StructType schema.
# MAGIC It must be StringType and should be last.
# MAGIC
# MAGIC Rule 4 — Always check null counts after PERMISSIVE
# MAGIC PERMISSIVE will silently produce null-filled DataFrames
# MAGIC if the wrong file is sent.
# MAGIC Always validate null rates after reading.
# MAGIC
# MAGIC Rule 5 — _corrupt_record is null for GOOD rows
# MAGIC Only rows with parsing problems have a value in _corrupt_record.
# MAGIC Filter isNull to get good rows.
# MAGIC Filter isNotNull to get bad rows.
# MAGIC
# MAGIC Rule 6 — Combine with validation for production
# MAGIC PERMISSIVE alone is not production safe.
# MAGIC Always add null percentage check or row count validation
# MAGIC to catch when corruption rate exceeds acceptable threshold.
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## One Line to Remember
# MAGIC
# MAGIC PERMISSIVE is Spark's **"bring everything in, figure it out later"** mode — it never throws away rows, converts bad fields to null, optionally captures the original corrupt row in a special column so you can investigate and quarantine it, but it must always be paired with null validation checks because its leniency means it will silently produce a DataFrame full of nulls if the wrong file is sent — making it the right choice for exploration, data quality analysis, and pipelines that need to separate good and bad records, but never a reason to skip validation.

# COMMAND ----------

# ============================================================
# CREATE REALISTIC BAD DATA FILE
# ============================================================

bad_data = """emp_id,name,department,salary,age,join_date,is_active
1,Alice,Engineering,95000.0,32,2019-03-15,true
2,Bob,Marketing,INVALID_SALARY,26,2021-07-22,true
3,Charlie,Engineering,120000.0,38,2017-01-10,true
NOT_A_NUMBER,Diana,HR,75000.0,35,2018-11-05,true
5,Edward,Finance,88000.0,WRONG_AGE,2019-09-30,true
6,Fiona,Engineering,92000.0,31,BAD_DATE,false
7,George,Marketing,105000.0,42,2016-06-14,WRONG_BOOLEAN
8,Hannah,HR,INVALID,ALSO_INVALID,ALSO_BAD,ALSO_WRONG
9,Ivan,Finance,115000.0,45,2015-12-01,false"""

dbutils.fs.put(
    "/Volumes/workspace/default/data_engineering/bad_employees.csv",
    bad_data,
    overwrite=True
)

df = (
    spark.read
        .format("csv")                          
        .option("header", "true")               
        .option("inferSchema", "true")
        .load("/Volumes/workspace/default/data_engineering/bad_employees.csv")
)



print("Bad data file created with:")
print("  Row 2  — invalid salary (string instead of double)")
print("  Row 4  — invalid emp_id (string instead of integer)")
print("  Row 5  — invalid age (string instead of integer)")
print("  Row 6  — invalid date format")
print("  Row 7  — invalid boolean")
print("  Row 8  — almost everything invalid")
print("  Row 9  — all good")


from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, BooleanType

schema = StructType([
    StructField("emp_id",     IntegerType(), True),
    StructField("name",       StringType(),  True),
    StructField("department", StringType(),  True),
    StructField("salary",     DoubleType(),  True),
    StructField("age",        IntegerType(), True),
    StructField("join_date",  DateType(),    True),
    StructField("is_active",  BooleanType(), True)
])


df_permissive = (
    spark.read
        .format("csv")                          
        .option("header", "true")               
        .schema(schema)
        .load("/Volumes/workspace/default/data_engineering/bad_employees.csv")
)

print("Schema Enforced")


# ============================================================
# ANALYSE WHAT IS NULL AND WHY
# ============================================================

from pyspark.sql.functions import count, isnull, isnotnull, col, when 

print("=== NULL COUNT PER COLUMN ===")
df_permissive.select([
    count(when(isnull(col(c)), c)).alias(c)
    for c in df_permissive.columns
]).show()

print("=== NULL PERCENTAGE PER COLUMN ===")
total = df_permissive.count()
df_permissive.select([
    (
        count(when(isnull(col(c)), c)) / total * 100
    ).alias(c)
    for c in df_permissive.columns
]).show()

print("\n=== ROWS WITH AT LEAST ONE NULL ===")
df_with_nulls = df_permissive.filter(
    isnull(col("emp_id"))    |
    isnull(col("salary"))    |
    isnull(col("age"))       |
    isnull(col("join_date")) |
    isnull(col("is_active"))
)
df_with_nulls.show(truncate=False)
print(f"Rows with nulls: {df_with_nulls.count()}")

print("\n=== PERFECTLY CLEAN ROWS ===")
df_clean = df_permissive.filter(
    isnotnull(col("emp_id"))    &
    isnotnull(col("name"))      &
    isnotnull(col("salary"))    &
    isnotnull(col("age"))       &
    isnotnull(col("join_date")) &
    isnotnull(col("is_active"))
)
df_clean.show(truncate=False)
print(f"Clean rows: {df_clean.count()}")



# COMMAND ----------

# ============================================================
# SCHEMA WITH CORRUPT RECORD COLUMN
# ============================================================

schema_with_corrupt = StructType([
    StructField("emp_id",          IntegerType(), True),
    StructField("name",            StringType(),  True),
    StructField("department",      StringType(),  True),
    StructField("salary",          DoubleType(),  True),
    StructField("age",             IntegerType(), True),
    StructField("join_date",       DateType(),    True),
    StructField("is_active",       BooleanType(), True),
    StructField("_corrupt_record", StringType(),  True)
    # Must be last
    # Must be StringType
    # Name must match columnNameOfCorruptRecord option
])

df_with_corrupt = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(schema_with_corrupt) \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .load("/Volumes/workspace/default/data_engineering/bad_employees.csv")

print("=== ALL ROWS WITH CORRUPT COLUMN ===")
df_with_corrupt.show(truncate=False)