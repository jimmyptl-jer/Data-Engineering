# Databricks notebook source
# MAGIC %md
# MAGIC In PySpark, `alias()` is used to give a temporary name to a column, DataFrame, or aggregated result.
# MAGIC
# MAGIC ### 1. Column Alias
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC df.select(
# MAGIC     col("employee_name").alias("name")
# MAGIC ).show()
# MAGIC ```
# MAGIC
# MAGIC Output:
# MAGIC
# MAGIC | name |
# MAGIC | ---- |
# MAGIC | John |
# MAGIC | Mary |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. Aggregate Alias
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import sum
# MAGIC
# MAGIC df.groupBy("department") \
# MAGIC   .agg(sum("salary").alias("total_salary")) \
# MAGIC   .show()
# MAGIC ```
# MAGIC
# MAGIC Output:
# MAGIC
# MAGIC | department | total_salary |
# MAGIC | ---------- | ------------ |
# MAGIC | IT         | 50000        |
# MAGIC | HR         | 30000        |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. DataFrame Alias (Useful in Joins)
# MAGIC
# MAGIC ```python
# MAGIC emp = employees.alias("e")
# MAGIC dept = departments.alias("d")
# MAGIC
# MAGIC result = emp.join(
# MAGIC     dept,
# MAGIC     emp.dept_id == dept.id,
# MAGIC     "inner"
# MAGIC ).select(
# MAGIC     "e.employee_name",
# MAGIC     "d.department_name"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC Or more explicitly:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC emp = employees.alias("e")
# MAGIC dept = departments.alias("d")
# MAGIC
# MAGIC result = emp.join(
# MAGIC     dept,
# MAGIC     col("e.dept_id") == col("d.id")
# MAGIC ).select(
# MAGIC     col("e.employee_name"),
# MAGIC     col("d.department_name")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. Renaming vs Alias
# MAGIC
# MAGIC #### Alias (temporary for query)
# MAGIC
# MAGIC ```python
# MAGIC df.select(col("salary").alias("emp_salary"))
# MAGIC ```
# MAGIC
# MAGIC #### Rename column permanently
# MAGIC
# MAGIC ```python
# MAGIC df = df.withColumnRenamed("salary", "emp_salary")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example with SQL Style
# MAGIC
# MAGIC ```python
# MAGIC df.createOrReplaceTempView("employees")
# MAGIC
# MAGIC spark.sql("""
# MAGIC     SELECT
# MAGIC         employee_name AS name,
# MAGIC         salary AS emp_salary
# MAGIC     FROM employees
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC `AS` in SQL is equivalent to `.alias()` in the DataFrame API.
# MAGIC