# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ShuffleDemo") \
    .getOrCreate()

data = [
    ("AAPL", 100),
    ("AAPL", 200),
    ("MSFT", 300),
    ("MSFT", 400),
    ("GOOG", 500)
]

df = spark.createDataFrame(
    data,
    ["symbol", "price"]
)

df.groupBy("symbol").sum("price").explain(True)

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.range(0, 10_000_000)

df = df.withColumn(
    "symbol",
    F.when((F.col("id") % 3) == 0, "AAPL")
     .when((F.col("id") % 3) == 1, "MSFT")
     .otherwise("GOOG")
)

result = (
    df.groupBy("symbol")
      .count()
)

result.explain(True)

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.range(0, 10_000_000)
# Repartition
df_repartition = df.repartition(20)



df_repartition.explain(True)

df_coalesce = df_repartition.coalesce(5)

df_coalesce.explain(True)