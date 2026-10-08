# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.option("header","true").option("inferschema","true").csv("/Volumes/workspace/udemy/source/raw_orders/orders.csv")

# COMMAND ----------

display(df)

# COMMAND ----------



# COMMAND ----------

df.count()

# COMMAND ----------

df_2 = spark.read\
            .option("header","true")\
            .option("inferschema","true")\
            .option("mode", "DROPMALFORMED")\
            .csv("/Volumes/workspace/udemy/source/raw_orders/orders.csv")

# COMMAND ----------

display(df_2)

# COMMAND ----------

df_2.count()

# COMMAND ----------

df_2.printSchema()

# COMMAND ----------

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

df_3 = spark.read\
            .option("header","true")\
            .schema(schema)\
            .csv("/Volumes/workspace/udemy/source/raw_orders/orders.csv")
df_3.count()
df_3.printSchema()

df_filter = df_3.filter(col("payment_method") == "Credit Card")
display(df_filter)

df_filter_1 = df_3.filter(col("category").isin(["Electronics","Home"]))
display(df_filter_1)

df = df_3.withColumn(
    "Order Category",
    when(col("price") > 50, "Premium")
    .otherwise("Regular")
)

display(df)

# COMMAND ----------

df_select = df_3.select("order_id", "customer_id", "order_date", "product_id", "quantity", "price", "order_status", "shipping_address")

display(df_select)

# COMMAND ----------

from pyspark.sql.functions import col

df_alias = df_3.select(
    col("order_id").alias("Order Id"), 
    col("customer_id").alias("Customer Id"), 
    col("order_date").alias("Order Date"), 
    col("product_id").alias("Product Id"), 
    col("quantity").alias("Quantity"), 
    col("price").alias("Price"), 
    col("order_status").alias("Order Status"), 
    col("shipping_address").alias("Shipping Address")
)

display(df_alias)