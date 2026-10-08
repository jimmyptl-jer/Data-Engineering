# Databricks notebook source
from pyspark.sql.types import *

SUPABASE_HOST = "db.ogzxxbhwtqbmkzpucgfb.supabase.co"
SUPABASE_DB   = "postgres"
SUPABASE_USER = "postgres"
SUPABASE_PASS = "UnQqCSJlune8whtX"

df = (spark.read
      .format("jdbc")
      .option("url", f"jdbc:postgresql://{SUPABASE_HOST}:5432/{SUPABASE_DB}")
      .option("dbtable", "bronze.test")
      .option("user", SUPABASE_USER)
      .option("password", SUPABASE_PASS)
      .option("driver", "org.postgresql.Driver")
      .option("fetchsize", "10000")
      .load())

print(df)

df.show()

# COMMAND ----------

# Supabase Database connection variables
supabase_host = "db.ogzxxbhwtqbmkzpucgfb.supabase.co"
supabase_port = "6543" # or use 6543 for the Supabase Transaction Pooler
supabase_db = "postgres"
supabase_user = "postgres"
supabase_password = "UnQqCSJlune8whtX"

# Construct JDBC URL
jdbc_url = f"jdbc:postgresql://{supabase_host}:{supabase_port}/{supabase_db}"

# Define connection properties
connection_properties = {
    "user": supabase_user,
    "password": supabase_password,
    "driver": "org.postgresql.Driver",
    "ssl": "true",  
    "sslmode": "require"
}

# Example: Read a Supabase table into a Spark DataFrame
supabase_df = spark.read.jdbc(
    url=jdbc_url,
    table="test",
    properties=connection_properties
)



display(supabase_df)


# COMMAND ----------

from pyspark.sql import SparkSession
import traceback

# -------------------------------
# Supabase Connection Details
# -------------------------------
SUPABASE_HOST = "db.ogzxxbhwtqbmkzpucgfb.supabase.co"
SUPABASE_PORT = "5432"
SUPABASE_DB = "postgres"
SUPABASE_USER = "postgres"
SUPABASE_PASS = "UnQqCSJlune8whtX"

jdbc_url = (
    f"jdbc:postgresql://{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"
    "?sslmode=require"
)

connection_properties = {
    "user": SUPABASE_USER,
    "password": SUPABASE_PASS,
    "driver": "org.postgresql.Driver",
    "ssl": "true",
    "sslmode": "require"
}

try:
    df = (
        spark.read
        .jdbc(
            url=jdbc_url,
            table="bronze.test", 
            properties=connection_properties
        )
    )

    print("Connection Successful!")

    df.printSchema()
    df.show()

except Exception as e:
    print("Connection Failed!")
    traceback.print_exc()