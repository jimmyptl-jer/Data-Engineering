# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType

from pyspark.sql.functions import when,col

schema = StructType([
    StructField("order_id",     IntegerType(), True),
    StructField("customer_id",   StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("order_status", StringType(), True),
    StructField("shipping_address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("discount", DoubleType(), True),
    StructField("category", StringType(), True),
    StructField("sales_rep", StringType(), True),
    StructField("region", StringType(), True),
    StructField("ship_date", DateType(), True),
    StructField("delivery_days", IntegerType(), True),
    StructField("returned", StringType(), True),
    StructField("gender", StringType(), True),
])

df = spark.read\
            .option("header","true")\
            .schema(schema)\
            .csv("/Volumes/workspace/udemy/source/raw_orders/orders.csv")

display(df)


df.dropDuplicates().show()

display(df)