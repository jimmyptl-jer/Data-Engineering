# Databricks notebook source
# ============================================================
# STEP 1: Read CSV file with schema inference
# Purpose: Load CSV data and automatically detect column types
# ============================================================

print("📌 STEP 1: Reading CSV with inferred schema...")

df = (
    spark.read
        .format("csv")                          # Specify file format as CSV
        .option("header", "true")               # First row contains column names
        .option("inferSchema", "true")          # Automatically detect data types
        .load("/Volumes/workspace/default/data_engineering/BigMart Sales.csv")  # File path
)

# Print schema to understand structure and data types
print("📌 Schema of DataFrame (df):")
df.printSchema()

# Display first 10 rows for preview
print("📌 Showing first 10 records from df:")
display(df.limit(10))


# ============================================================
# STEP 2: Read CSV file without schema inference
# Purpose: Load CSV data with all columns as STRING (default)
# ============================================================

print("📌 STEP 2: Reading CSV without schema inference...")

df1 = (
    spark.read
        .format("csv")                          # Specify file format as CSV
        .option("header", "true")               # First row contains column names
        # No inferSchema → all columns will be treated as STRING
        .load("/Volumes/workspace/default/data_engineering/BigMart Sales.csv")
)

# Show top rows (default 20 rows)
print("📌 Showing records from df1:")
df1.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `workspace`.`default`.`big_mart_sales`;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Data Reading

# COMMAND ----------

# DBTITLE 1,Read data from Unity Catalog table
# Read from Unity Catalog table instead of DBFS
df = spark.table("workspace.default.big_mart_sales")
display(df.limit(10))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Data Reading from csv

# COMMAND ----------

# ------------------------------------------------------------
# STEP 1: Read CSV with inferred schema (for exploration only)
# ------------------------------------------------------------

df = (
    spark.read
        .format("csv")                         # File format
        .option("header", "true")              # First row contains column names
        .option("inferSchema", "true")         # Auto-detect schema (temporary use)
        .load("/Volumes/workspace/default/data_engineering/BigMart Sales.csv")
)

# Preview data
print("🔹 Sample Data (Inferred Schema):")
df.show(truncate=False)

# Print inferred schema
print("\n🔹 Inferred Schema:")
df.printSchema()


# ------------------------------------------------------------
# STEP 2: Define explicit schema using DDL (Production approach)
# ------------------------------------------------------------

ddl_schema = """
    Item_Identifier           STRING,
    Item_Weight               DOUBLE,
    Item_Fat_Content          STRING,
    Item_Visibility           DOUBLE,
    Item_Type                 STRING,
    Item_MRP                  DOUBLE,
    Outlet_Identifier         STRING,
    Outlet_Establishment_Year INT,
    Outlet_Size               STRING,
    Outlet_Location_Type      STRING,
    Outlet_Type               STRING,
    Item_Outlet_Sales         DOUBLE
"""


# ------------------------------------------------------------
# STEP 3: Read CSV again using defined schema
# ------------------------------------------------------------

df = (
    spark.read
        .format("csv")                         # File format
        .option("header", "true")              # Use header row
        .schema(ddl_schema)                   # ✅ Apply DDL schema
        # NOTE: Do NOT use inferSchema here
        .load("/Volumes/workspace/default/data_engineering/BigMart Sales.csv")
)

# Preview data with correct schema
print("\n🔹 Sample Data (DDL Schema Applied):")
df.show(truncate=False)

# Print final schema
print("\n🔹 Final Schema:")
df.printSchema()

# COMMAND ----------

# ============================================================
# BIGMART SALES — PySpark Data Engineering Pipeline
# 
# Dataset  : BigMart Sales.csv
# Purpose  : Ingest, explore, clean, and transform sales data
# Phases   : Exploration → Schema Definition → Transformation
# ============================================================


# ============================================================
# STEP 1: Read CSV with inferSchema (Exploration Phase)
# ============================================================
# WHY    : In the exploration phase, we let Spark auto-detect
#          column types. This is a quick way to understand the
#          structure of a new dataset before hardcoding a schema.
#
# QUERY  : spark.read reads the CSV file from the Unity Catalog
#          Volume path. `inferSchema=true` triggers a full scan
#          of the data to guess data types for each column.
#          `header=true` treats the first row as column names.
#
# WHAT HAPPENS:
#   - Spark scans the entire file (can be slow on large files)
#   - Assigns best-guess types: StringType, IntegerType, etc.
#   - Returns a DataFrame with inferred column types
#   - df.count()  → triggers a full scan → returns total rows
#   - df.show()   → displays first 20 rows
#   - df.printSchema() → prints column names + inferred types
#
# ⚠️ CAUTION: inferSchema is expensive and unreliable in prod.
#             It causes an extra pass over the entire dataset.
# ============================================================

print("📌 STEP 1: Reading CSV using inferSchema (Exploration Phase)")

df = (
    spark.read
        .format("csv")
        .option("header", "true")       # First row = column headers
        .option("inferSchema", "true")  # Auto-detect column data types (dev only)
        .load("/Volumes/workspace/default/data_engineering/BigMart Sales.csv")
)

print(f"🔢 Total Rows (Inferred DataFrame): {df.count()}")  # Action → triggers full scan

print("\n🔹 Sample Data (Inferred Schema):")
df.show(truncate=False)  # Show top 20 rows; truncate=False shows full column values

print("\n🔹 Inferred Schema:")
df.printSchema()  # Displays tree-style schema with column names and types


print("***************************************************")
print("***************************************************")
print("***************************************************")
print("***************************************************")
print("***************************************************")
print("***************************************************")
print("***************************************************")

df.show()              # show all rows (default 20)
df.show(2)             # show top 2 rows
df.show(truncate=False)# show full content without truncation

df.printSchema()       # schema with types and nullability
df.columns             # list of column names
df.dtypes              # list of (column, type) tuples
df.count()             # total row count
df.describe().show()   # count, mean, stddev, min, max

df.first()             # first Row object
df.head(3)             # list of first 3 Row objects
df.take(3)             # same as head — list of Row objects
df.limit(3).show()     # DataFrame with 3 rows (transformation)
# ============================================================
# STEP 2: Define Explicit Schema (Production Phase)
# ============================================================
# WHY    : Hardcoding the schema avoids the extra file scan
#          triggered by inferSchema. It also prevents Spark from
#          misidentifying types (e.g., reading a float as string).
#          This is the PRODUCTION-GRADE approach.
#
# WHAT HAPPENS:
#   - StructType defines the full table schema
#   - StructField(name, dataType, nullable) defines each column
#   - nullable=True means the column can have null values
#
# COLUMN BREAKDOWN:
#   - Item_Identifier         → Product code (String)
#   - Item_Weight             → Weight of the product (Double)
#   - Item_Fat_Content        → Low Fat / Regular (String)
#   - Item_Visibility         → % shelf display area (Double)
#   - Item_Type               → Product category (String)
#   - Item_MRP                → Maximum Retail Price (Double)
#   - Outlet_Identifier       → Store code (String)
#   - Outlet_Establishment_Year → Year store opened (Integer)
#   - Outlet_Size             → Small / Medium / High (String)
#   - Outlet_Location_Type    → Tier 1 / 2 / 3 city (String)
#   - Outlet_Type             → Grocery Store / Supermarket (String)
#   - Item_Outlet_Sales       → Target variable — actual sales (Double)
# ============================================================

print("\n📌 STEP 2: Defining explicit schema")

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)

schema = StructType([
    StructField("Item_Identifier",           StringType(),  True),
    StructField("Item_Weight",               DoubleType(),  True),
    StructField("Item_Fat_Content",          StringType(),  True),
    StructField("Item_Visibility",           DoubleType(),  True),
    StructField("Item_Type",                 StringType(),  True),
    StructField("Item_MRP",                  DoubleType(),  True),
    StructField("Outlet_Identifier",         StringType(),  True),
    StructField("Outlet_Establishment_Year", IntegerType(), True),
    StructField("Outlet_Size",               StringType(),  True),
    StructField("Outlet_Location_Type",      StringType(),  True),
    StructField("Outlet_Type",               StringType(),  True),
    StructField("Item_Outlet_Sales",         DoubleType(),  True)
])


# ============================================================
# STEP 3: Read CSV with Explicit Schema (Production Phase)
# ============================================================
# WHY    : Now we use the schema we defined above instead of
#          letting Spark guess. This is faster, safer, and
#          deterministic for production pipelines.
#
# QUERY  : Same as Step 1, but `.schema(schema)` replaces
#          `.option("inferSchema", "true")`
#
# WHAT HAPPENS:
#   - Spark reads the file WITHOUT an extra inference scan
#   - Columns are cast to the types we defined in Step 2
#   - Any type mismatch results in nulls (not errors) by default
#   - df.count()  → total number of records in the dataset
#   - df.show()   → preview of first 20 rows
#   - df.printSchema() → confirms our explicit schema is applied
# ============================================================

print("\n📌 STEP 3: Reading CSV using explicit schema")

df = (
    spark.read
        .format("csv")
        .option("header", "true")   # First row = column headers
        .schema(schema)             # Apply our predefined schema (no extra scan)
        .load("/Volumes/workspace/default/data_engineering/BigMart Sales.csv")
)

print(f"🔢 Total Rows (After Applying Schema): {df.count()}")

print("\n🔹 Sample Data (Explicit Schema Applied):")
df.show(truncate=False)

print("\n🔹 Final Schema:")
df.printSchema()


# ============================================================
# STEP 4: Select Specific Columns (Projection / Column Pruning)
# ============================================================
# WHY    : We don't always need all 12 columns downstream.
#          Selecting only what's needed reduces memory usage
#          and speeds up subsequent transformations.
#          This is called "column pruning" in query optimization.
#
# QUERY  : df.select(...) creates a new DataFrame with only
#          the specified columns from the original df.
#
# COLUMNS SELECTED:
#   - Item_MRP         → Price of the product
#   - Item_Type        → Product category
#   - Outlet_Size      → Size of the store
#   - Outlet_Type      → Type of the store
#
# WHAT HAPPENS:
#   - A new DataFrame df1 is created (df is unchanged)
#   - Only 4 columns are retained out of 12
#   - df1.count() → total rows (same as before, no rows dropped yet)
#   - df1.show()  → preview of the 4-column DataFrame
# ============================================================

print("\n📌 STEP 4: Selecting specific columns")

from pyspark.sql.functions import col

df1 = df.select(
    col("Item_MRP"),      # Product price
    col("Item_Type"),     # Product category (e.g., Dairy, Snack Foods)
    col("Outlet_Size"),   # Store size (Small / Medium / High)
    col("Outlet_Type")    # Store type (Grocery Store / Supermarket Type1/2/3)
)

print(f"🔢 Total Rows (After Select): {df1.count()}")

print("\n🔹 Data after selecting columns:")
df1.show()


# ============================================================
# STEP 5: Filter Null Values (Data Quality / Cleaning)
# ============================================================
# WHY    : Outlet_Size has missing values in the raw data.
#          Keeping nulls can break aggregations, ML models,
#          or downstream joins. We remove them here.
#
# QUERY  : df1.filter(col("Outlet_Size").isNotNull())
#          This is equivalent to SQL: WHERE Outlet_Size IS NOT NULL
#
# WHAT HAPPENS:
#   - Spark scans df1 and drops rows where Outlet_Size is null
#   - A new filtered DataFrame df1 is returned
#   - df1.count() → should be LESS than Step 4 count
#                   (difference = number of null rows removed)
#   - df1.show()  → all rows will have a non-null Outlet_Size
#
# 💡 TIP: Compare count before/after to measure data loss:
#         null_rows = step4_count - step5_count
# ============================================================

print("\n📌 STEP 5: Filtering rows where Outlet_Size is NOT NULL")

df1 = df1.filter(col("Outlet_Size").isNotNull())  # SQL equiv: WHERE Outlet_Size IS NOT NULL

filtered_count = df1.count()
print(f"🔢 Total Rows (After Filter - Outlet_Size NOT NULL): {filtered_count}")

print("\n🔹 Data after filtering null Outlet_Size:")
df1.show()


# ============================================================
# STEP 6: Final Schema Check (Validation Step)
# ============================================================
# WHY    : After applying select + filter, we verify the schema
#          is exactly what we expect before writing or continuing.
#          Good practice in any production ETL pipeline.
#
# WHAT HAPPENS:
#   - Prints the column names and their data types for df1
#   - Confirms no accidental type changes happened
#   - Should show: Item_MRP (double), Item_Type (string),
#                  Outlet_Size (string), Outlet_Type (string)
# ============================================================

print("\n📌 STEP 6: Final schema of transformed DataFrame")
df1.printSchema()


# ============================================================
# STEP 7: Apply Aliases — Rename Columns (Standardization)
# ============================================================
# WHY    : Column names from the raw CSV are verbose (e.g.,
#          "Item_Identifier", "Item_Outlet_Sales"). Aliasing
#          them to clean, short names makes downstream queries,
#          joins, and API responses easier to work with.
#
# QUERY  : col("original_name").alias("new_name")
#          This is equivalent to SQL: SELECT Item_Identifier AS id
#
# COLUMNS AND ALIASES:
#   - Item_Identifier    → id          (short product code)
#   - Item_MRP           → mrp         (price)
#   - Item_Type          → item_type   (category)
#   - Outlet_Size        → outlet_size (store size)
#   - Outlet_Type        → outlet_type (store type)
#   - Item_Outlet_Sales  → sales       (target variable / revenue)
#
# NOTE: df2 is built from the original `df` (not df1) so that
#       we retain the `id` and `sales` columns which were not
#       part of df1's column selection.
#
# WHAT HAPPENS:
#   - df.display() → Databricks-native method, shows df in a
#                    rich tabular UI (equivalent to df.show()
#                    but with sorting/filtering in notebooks)
#   - df2.count()  → total rows (from original df, no null filter)
#   - df2.show()   → preview with renamed columns
#   - df2.printSchema() → confirms alias names are applied
# ============================================================

print("\n📌 STEP 7: Applying aliases (renaming columns)")

df2 = df.select(
    col("Item_Identifier").alias("id"),           # Product unique code
    col("Item_MRP").alias("mrp"),                 # Maximum Retail Price
    col("Item_Type").alias("item_type"),          # Product category
    col("Outlet_Size").alias("outlet_size"),      # Store size classification
    col("Outlet_Type").alias("outlet_type"),      # Store type classification
    col("Item_Outlet_Sales").alias("sales")       # Total sales amount (target variable)
)

df2.display() 

print(f"🔢 Total Rows (After Alias Applied): {df2.count()}")

print("\n🔹 Data after applying aliases:")
df2.show()

print("\n🔹 Schema after aliasing:")
df2.printSchema()


# ============================================================
# STEP 8: Filtering Examples (filter vs where)
# Purpose: Apply business rules and data cleaning conditions
# ============================================================

print("\n📌 STEP 8: Applying filter conditions")

from pyspark.sql.functions import col


# ------------------------------------------------------------
# 8.1 Filter: High MRP + Non-null Outlet_Size
# ------------------------------------------------------------

print("\n🔹 Filter 1: Item_MRP > 100 AND Outlet_Size IS NOT NULL")

df_filtered = df.filter(
    (col("Item_MRP") > 100) &
    (col("Outlet_Size").isNotNull())
)

print(f"🔢 Rows after Filter 1: {df_filtered.count()}")

df_filtered.show(truncate=False)


# ------------------------------------------------------------
# 8.2 Filter: Valid Sales Data
# ------------------------------------------------------------

print("\n🔹 Filter 2: Item_MRP > 0 AND Sales NOT NULL")

df_clean = df.filter(
    (col("Item_MRP") > 0) &
    (col("Item_Outlet_Sales").isNotNull())
)

print(f"🔢 Rows after Filter 2: {df_clean.count()}")

df_clean.show(truncate=False)


# ------------------------------------------------------------
# 8.3 Using WHERE (same as filter)
# ------------------------------------------------------------

print("\n🔹 Filter 3 (WHERE): Sales > 5000")

df_where = df.where(
    col("Item_Outlet_Sales") > 5000
)

print(f"🔢 Rows after WHERE filter: {df_where.count()}")

df_where.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC Data Reading From JSON

# COMMAND ----------

# ============================================================
# STEP 1: Read JSON file (Exploration Phase)
# Purpose: Understand structure, nested fields, and data types
# ============================================================

print("📌 STEP 1: Reading JSON file (Exploration Phase)")

df = (
    spark.read
        .format("json")                         # JSON format
        .option("multiline", False)             # Change to True if JSON is array-based
        .load("/Volumes/workspace/default/data_engineering/drivers.json")
)

# Count rows
total_rows = df.count()
print(f"🔢 Total Rows (Raw JSON Data): {total_rows}")

# Preview data
print("\n🔹 Sample Data (Raw JSON):")
df.show(20, truncate=False)

# Print schema
print("\n🔹 Inferred Schema:")
df.printSchema()


# ============================================================
# STEP 2: Re-read JSON with multiline=True (ONLY if needed)
# ============================================================

print("\n📌 STEP 2: Checking multiline JSON support")

df_multi = (
    spark.read
        .format("json")
        .option("multiline", True)
        .load("/Volumes/workspace/default/data_engineering/drivers.json")
)

multi_count = df_multi.count()
print(f"🔢 Total Rows (Multiline Read): {multi_count}")

# Use correct dataframe
if multi_count > total_rows:
    print("✅ Using multiline=True DataFrame")
    df = df_multi
else:
    print("✅ Using standard JSON DataFrame")


# ============================================================
# STEP 3: Select required columns (Safe Projection)
# ============================================================

print("\n📌 STEP 3: Selecting columns")

from pyspark.sql.functions import col

# Replace these with actual columns after checking schema
columns = df.columns

df1 = df.select(*columns)

print(f"🔢 Total Rows (After Select): {df1.count()}")

print("\n🔹 Data after selecting columns:")
df1.show(20, truncate=False)


# ============================================================
# STEP 4: Filter NULL values (SAFE way)
# ============================================================

print("\n📌 STEP 4: Filtering NULL values")

# Choose a real column dynamically (example: first column)
filter_column = columns[0]

print(f"👉 Filtering NULLs on column: {filter_column}")

df1 = df1.filter(col(filter_column).isNotNull())

filtered_count = df1.count()
print(f"🔢 Total Rows (After Filter): {filtered_count}")
print(f"❌ Rows Removed: {total_rows - filtered_count}")

print("\n🔹 Data after filtering:")
df1.show(20, truncate=False)


# ============================================================
# STEP 5: Flatten nested JSON (SAFE check)
# ============================================================

print("\n📌 STEP 5: Flatten nested JSON (if exists)")

# Check if nested struct exists
from pyspark.sql.types import StructType

nested_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]

if nested_cols:
    print(f"👉 Found nested columns: {nested_cols}")

    # Example: flatten first nested column
    nested_col = nested_cols[0]

    df_flat = df.select(
        col("*"),
        col(f"{nested_col}.*")   # flatten
    )

    print("\n🔹 Flattened Data:")
    df_flat.show(20, truncate=False)

else:
    print("⚠️ No nested structure found")


# ============================================================
# STEP 6: Explode arrays (SAFE check)
# ============================================================

print("\n📌 STEP 6: Exploding array columns (if exists)")

from pyspark.sql.types import ArrayType
from pyspark.sql.functions import explode

array_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType)]

if array_cols:
    print(f"👉 Found array columns: {array_cols}")

    array_col = array_cols[0]

    df_exploded = df.select(
        col("*"),
        explode(col(array_col)).alias(f"{array_col}_exploded")
    )

    print("\n🔹 Exploded Data:")
    df_exploded.show(20, truncate=False)

else:
    print("⚠️ No array columns found")


# ============================================================
# STEP FINAL: Display FULL Data (Safe)
# ============================================================

print("\n📌 FINAL STEP: Displaying dataset")

# Best option in Databricks
display(df)

# Controlled full display
if total_rows < 10000:
    print("\n🔹 Showing FULL dataset:")
    df.show(total_rows, truncate=False)
else:
    print("\n⚠️ Dataset too large → showing sample only")
    df.show(100, truncate=False)

# COMMAND ----------

df.show()              # show all rows (default 20)
df.show(2)             # show top 2 rows
df.show(truncate=False)# show full content without truncation

df.printSchema()       # schema with types and nullability
df.columns             # list of column names
df.dtypes              # list of (column, type) tuples
df.count()             # total row count
df.describe().show()   # count, mean, stddev, min, max

df.first()             # first Row object
df.head(3)             # list of first 3 Row objects
df.take(3)             # same as head — list of Row objects
df.limit(3).show()     # DataFrame with 3 rows (transformation)