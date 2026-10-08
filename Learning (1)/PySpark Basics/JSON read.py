# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read \
    .option("multiline", "true") \
    .json("/Volumes/workspace/udemy/source/raw_orders/day3.json")

# COMMAND ----------

df.show()

# COMMAND ----------

df.show(truncate=False)