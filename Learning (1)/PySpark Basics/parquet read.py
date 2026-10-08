# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read \
    .format("parquet") \
    .load("/Volumes/workspace/udemy/source/raw_orders/part-00000-tid-orders.c000.snappy.parquet")

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()