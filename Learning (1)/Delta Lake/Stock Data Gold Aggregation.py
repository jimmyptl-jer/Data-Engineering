# Databricks notebook source
# DBTITLE 1,Stock Data Gold Aggregation
# MAGIC %md
# MAGIC # Stock Data Gold Aggregation
# MAGIC
# MAGIC Aggregate cleaned silver-layer stock data into business-ready gold summary tables with daily metrics and moving averages.

# COMMAND ----------

# DBTITLE 1,Read Silver & Aggregate to Gold
# ============================================================
# 1. READ SILVER DATA
# ============================================================
silver_df = spark.table("stock_catalog.silver.clean_stock_table")

# ============================================================
# 2. AGGREGATE — Daily summary with moving averages
# ============================================================
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, avg, max as max_, min as min_, sum as sum_,
    count, orderBy
)

gold_df = (
    silver_df
    .groupBy("Date")
    .agg(
        count("*").alias("record_count"),
        min_("Low").alias("daily_low"),
        max_("High").alias("daily_high"),
        avg("Close").alias("avg_close"),
        sum_("Volume").alias("total_volume"),
        max_("Close").alias("close_price")
    )
    .orderBy("Date")
)

# 7-day and 30-day moving averages
window_7 = Window.orderBy("Date").rowsBetween(-6, 0)
window_30 = Window.orderBy("Date").rowsBetween(-29, 0)

gold_df = (
    gold_df
    .withColumn("ma_7_day", avg("close_price").over(window_7))
    .withColumn("ma_30_day", avg("close_price").over(window_30))
)

display(gold_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Write Gold Delta Table
# ============================================================
# 3. WRITE GOLD TABLE
# ============================================================
(
    gold_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("stock_catalog.gold.stock_summary")
)

print("Gold layer written successfully")
print(f"Row count: {gold_df.count()}")