# Databricks notebook source
# MAGIC %md
# MAGIC In PySpark, `filter()` (or `where()`) is used to select rows that match a condition.
# MAGIC
# MAGIC ### Basic Syntax
# MAGIC
# MAGIC ```python
# MAGIC df.filter(condition)
# MAGIC ```
# MAGIC
# MAGIC or
# MAGIC
# MAGIC ```python
# MAGIC df.where(condition)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Filter Single Condition
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC df.filter(col("price") > 100).show()
# MAGIC ```
# MAGIC
# MAGIC Equivalent:
# MAGIC
# MAGIC ```python
# MAGIC df.filter("price > 100").show()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Multiple Conditions
# MAGIC
# MAGIC #### AND
# MAGIC
# MAGIC ```python
# MAGIC df.filter(
# MAGIC     (col("price") > 100) &
# MAGIC     (col("quantity") > 5)
# MAGIC ).show()
# MAGIC ```
# MAGIC
# MAGIC #### OR
# MAGIC
# MAGIC ```python
# MAGIC df.filter(
# MAGIC     (col("order_status") == "Delivered") |
# MAGIC     (col("order_status") == "Shipped")
# MAGIC ).show()
# MAGIC ```
# MAGIC
# MAGIC ⚠️ Use `&` and `|`, not Python's `and` and `or`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Filter NULL Values
# MAGIC
# MAGIC ```python
# MAGIC df.filter(col("shipping_address").isNull()).show()
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC df.filter(col("shipping_address").isNotNull()).show()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Filter by Multiple Values
# MAGIC
# MAGIC ```python
# MAGIC df.filter(
# MAGIC     col("order_status").isin("Delivered", "Shipped")
# MAGIC ).show()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Filter After Alias
# MAGIC
# MAGIC If you aliased columns with spaces:
# MAGIC
# MAGIC ```python
# MAGIC df_alias.filter(col("Order Status") == "Delivered").show()
# MAGIC ```
# MAGIC
# MAGIC or
# MAGIC
# MAGIC ```python
# MAGIC df_alias.filter("`Order Status` = 'Delivered'").show()
# MAGIC ```
# MAGIC
# MAGIC Notice the backticks around column names with spaces.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC delivered_orders = df.filter(
# MAGIC     (col("order_status") == "Delivered") &
# MAGIC     (col("price") > 500)
# MAGIC )
# MAGIC
# MAGIC delivered_orders.show()
# MAGIC ```
# MAGIC
# MAGIC This returns only orders that are **Delivered** and have a **price greater than 500**.
# MAGIC