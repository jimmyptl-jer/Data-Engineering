# Databricks notebook source
# MAGIC %md
# MAGIC #lazy loading

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data = [
    (1,"lisa","lisa@random.com"),
    (2,"john","john@random.com"),
    (3,"mary","mary@random.com"),
    (4,"peter","peter@random.com"),
    (5,"jane","jane@random.com"),
    (6,"jim","jim@random.com"),
    (7,"sara","sara@random.com"),
    (8,"bob","bob@random.com"),
    (9,"alex","alex@random.com")
]

columns = ["id","name","email"]


df = spark.createDataFrame(data,columns)

display(df)


df_new = df.select("id","email")
display(df_new)

df_new_2 = df.filter(col("id") > 5)
display(df_new_2)


# COMMAND ----------

df_new_2.explain()

# COMMAND ----------

df.explain("formatted")