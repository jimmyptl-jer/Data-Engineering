# Databricks notebook source
# MAGIC %md
# MAGIC #Reading Data from volumns - Created as an part of udemy course

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("csv")\
        .option("header",True)\
        .option("inferSchema", "true")\
        .load("/Volumes/workspace/udemy/source/raw_orders/orders.csv")

df.show()

df.printSchema()

display(df)