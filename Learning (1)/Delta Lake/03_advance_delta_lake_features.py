# Databricks notebook source
from pyspark.sql import functions as F

data = [
    ("AAPL", "Apple", 250.00, 1000),
    ("GOOG", "Alphabet", 280.00, 800),
    ("MSFT", "Microsoft", 500.00, 600),
]

columns = ["symbol", "company", "price", "volume"]

df = spark.createDataFrame(data, columns)

df.show()

# COMMAND ----------

silver_path = "s3://graywolf--data--lake/databricks/stock/silver/"

df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_path)

# COMMAND ----------

silver_df = (
    spark.read
    .format("delta")
    .load(silver_path)
)

silver_df.show()

# COMMAND ----------

from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(
    spark,
    silver_path
)

delta_table.history().show(truncate=False)

# COMMAND ----------

delta_table.update(
    condition="symbol = 'AAPL'",
    set={
        "price": "260.00"
    }
)

# COMMAND ----------

delta_table.toDF().show()

# COMMAND ----------

delta_table.history().show(truncate=False)

# COMMAND ----------

delta_table.delete(
    condition="symbol = 'MSFT'"
)

# COMMAND ----------

delta_table.toDF().show()

# COMMAND ----------

delta_table.history().show(truncate=False)

# COMMAND ----------

old_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 1)
    .load(silver_path)
)

old_df.show()

# COMMAND ----------

delta_table.history().show(truncate=False)