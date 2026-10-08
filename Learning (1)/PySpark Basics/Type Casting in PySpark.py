# Databricks notebook source
# MAGIC %md
# MAGIC ### Type Casting in PySpark — Theory
# MAGIC
# MAGIC **Type Casting** is the process of converting a column from one data type to another data type.
# MAGIC
# MAGIC PySpark often reads data as strings (especially from CSV files), so type casting is required before performing calculations, aggregations, filtering, or date operations.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Why Type Casting is Needed
# MAGIC
# MAGIC Consider a CSV file:
# MAGIC
# MAGIC | id | price |
# MAGIC | -- | ----- |
# MAGIC | 1  | 100   |
# MAGIC | 2  | 200   |
# MAGIC
# MAGIC Spark may read `price` as **StringType**.
# MAGIC
# MAGIC Without casting:
# MAGIC
# MAGIC ```python
# MAGIC price = "100"
# MAGIC ```
# MAGIC
# MAGIC Spark treats it as text, not a number.
# MAGIC
# MAGIC To perform mathematical operations, it must be converted to a numeric type.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Common Data Types in PySpark
# MAGIC
# MAGIC | Data Type     | Description                    |
# MAGIC | ------------- | ------------------------------ |
# MAGIC | StringType    | Text values                    |
# MAGIC | IntegerType   | Whole numbers                  |
# MAGIC | LongType      | Large integers                 |
# MAGIC | FloatType     | Decimal numbers                |
# MAGIC | DoubleType    | High-precision decimal numbers |
# MAGIC | BooleanType   | True/False values              |
# MAGIC | DateType      | Dates                          |
# MAGIC | TimestampType | Date and time                  |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Methods of Type Casting
# MAGIC
# MAGIC ### 1. Using `cast()`
# MAGIC
# MAGIC ```python
# MAGIC col("price").cast("double")
# MAGIC ```
# MAGIC
# MAGIC Converts the column to the specified data type.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. Using SQL Expression
# MAGIC
# MAGIC ```python
# MAGIC expr("CAST(price AS DOUBLE)")
# MAGIC ```
# MAGIC
# MAGIC Uses SQL-style casting.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Characteristics
# MAGIC
# MAGIC ### Explicit Conversion
# MAGIC
# MAGIC The user specifies the target data type.
# MAGIC
# MAGIC Example:
# MAGIC
# MAGIC * String → Integer
# MAGIC * Integer → Double
# MAGIC * String → Date
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Returns New DataFrame
# MAGIC
# MAGIC Like all Spark transformations, type casting does not modify the original DataFrame.
# MAGIC
# MAGIC A new DataFrame is returned with the converted column.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Lazy Evaluation
# MAGIC
# MAGIC Casting is a transformation.
# MAGIC
# MAGIC Spark performs the conversion only when an action is executed.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Handling Invalid Values
# MAGIC
# MAGIC When Spark cannot convert a value:
# MAGIC
# MAGIC Example:
# MAGIC
# MAGIC | price |
# MAGIC | ----- |
# MAGIC | 100   |
# MAGIC | abc   |
# MAGIC
# MAGIC Casting to integer:
# MAGIC
# MAGIC ```text
# MAGIC 100 → 100
# MAGIC abc → null
# MAGIC ```
# MAGIC
# MAGIC Invalid values become **null** instead of causing an error (in most cases).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Benefits of Type Casting
# MAGIC
# MAGIC * Enables arithmetic operations.
# MAGIC * Supports aggregations (`sum`, `avg`, `max`, `min`).
# MAGIC * Improves data quality.
# MAGIC * Allows date and timestamp processing.
# MAGIC * Ensures schema consistency.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Real-World Example
# MAGIC
# MAGIC A CSV file stores:
# MAGIC
# MAGIC | quantity | price   |
# MAGIC | -------- | ------- |
# MAGIC | "10"     | "25.50" |
# MAGIC
# MAGIC Both are strings.
# MAGIC
# MAGIC To calculate total amount:
# MAGIC
# MAGIC 1. Cast quantity to Integer.
# MAGIC 2. Cast price to Double.
# MAGIC 3. Perform multiplication.
# MAGIC
# MAGIC Without casting, Spark treats the values as text and calculations may fail or produce incorrect results.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Interview Definition
# MAGIC
# MAGIC > Type Casting in PySpark is the process of converting a column from one data type to another using methods such as `cast()`. It is commonly used to transform string data into numeric, date, or timestamp formats so that calculations, filtering, aggregations, and other operations can be performed correctly. Since Spark DataFrames are immutable, casting returns a new DataFrame and follows lazy evaluation.
# MAGIC