# Databricks notebook source
# MAGIC %md
# MAGIC ### `withColumn()` in PySpark — Theory
# MAGIC
# MAGIC `withColumn()` is a DataFrame transformation used to **add a new column** or **modify an existing column** in a PySpark DataFrame.
# MAGIC
# MAGIC #### Purpose
# MAGIC
# MAGIC * Create derived columns from existing data.
# MAGIC * Update values in an existing column.
# MAGIC * Change data types.
# MAGIC * Apply calculations, conditions, or functions to columns.
# MAGIC
# MAGIC #### Syntax
# MAGIC
# MAGIC ```python
# MAGIC DataFrame.withColumn(columnName, columnExpression)
# MAGIC ```
# MAGIC
# MAGIC Where:
# MAGIC
# MAGIC * **columnName**: Name of the new or existing column.
# MAGIC * **columnExpression**: Expression that defines the column values.
# MAGIC
# MAGIC #### Key Points
# MAGIC
# MAGIC 1. **Immutable DataFrames**
# MAGIC
# MAGIC    * PySpark DataFrames are immutable.
# MAGIC    * `withColumn()` does not modify the original DataFrame.
# MAGIC    * It returns a new DataFrame with the changes.
# MAGIC
# MAGIC 2. **Add New Column**
# MAGIC
# MAGIC    * If the specified column name does not exist, a new column is created.
# MAGIC
# MAGIC 3. **Replace Existing Column**
# MAGIC
# MAGIC    * If the specified column name already exists, its values are replaced.
# MAGIC
# MAGIC 4. **Transformation Operation**
# MAGIC
# MAGIC    * `withColumn()` is a transformation and follows Spark's lazy evaluation.
# MAGIC    * Execution occurs only when an action (`show()`, `count()`, `write()`, etc.) is triggered.
# MAGIC
# MAGIC 5. **Common Uses**
# MAGIC
# MAGIC    * Arithmetic calculations
# MAGIC    * Data type conversion
# MAGIC    * Conditional logic (`when`, `otherwise`)
# MAGIC    * Date formatting
# MAGIC    * String manipulation
# MAGIC
# MAGIC #### Example Concept
# MAGIC
# MAGIC Original DataFrame:
# MAGIC
# MAGIC | Product | Price |
# MAGIC | ------- | ----- |
# MAGIC | A       | 100   |
# MAGIC | B       | 200   |
# MAGIC
# MAGIC Using `withColumn("Tax", Price * 0.1)`:
# MAGIC
# MAGIC Result:
# MAGIC
# MAGIC | Product | Price | Tax |
# MAGIC | ------- | ----- | --- |
# MAGIC | A       | 100   | 10  |
# MAGIC | B       | 200   | 20  |
# MAGIC
# MAGIC #### Difference Between `withColumn()` and `select()`
# MAGIC
# MAGIC | withColumn()                                             | select()                                  |
# MAGIC | -------------------------------------------------------- | ----------------------------------------- |
# MAGIC | Adds or modifies columns while keeping existing columns. | Selects specific columns and expressions. |
# MAGIC | Good for incremental transformations.                    | Good for reshaping the DataFrame.         |
# MAGIC | Returns all existing columns plus modifications.         | Returns only the columns specified.       |
# MAGIC
# MAGIC #### Interview Definition
# MAGIC
# MAGIC > `withColumn()` is a PySpark DataFrame transformation used to create a new column or replace an existing column by applying an expression. Since DataFrames are immutable, it returns a new DataFrame without modifying the original one and follows Spark's lazy evaluation model.
# MAGIC