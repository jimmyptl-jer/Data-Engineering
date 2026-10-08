# Databricks notebook source
# DBTITLE 1,Load Silver Data from Unity Catalog
# ============================================================
# 1. LOAD SILVER DATA FROM UNITY CATALOG
# ============================================================
# Import Spark SQL functions and window functions for transformations
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read the silver stock table from Unity Catalog
silver_df = spark.table(
    "stock_catalog.silver.stock_data"
)

# Preview the silver DataFrame
display(silver_df)

# COMMAND ----------

# DBTITLE 1,Define Stock Window Specification
# ============================================================
# 2. DEFINE WINDOW SPECIFICATION FOR PER-STOCK ORDERING
# ============================================================
# Partition by symbol and order by trade_date to enable row-by-row
# calculations (e.g., lag, moving averages) within each stock
stock_window = Window.partitionBy("symbol").orderBy(F.col("trade_date"))

# COMMAND ----------

# DBTITLE 1,Add Previous Day Close Price
# ============================================================
# 3. ADD PREVIOUS DAY CLOSE PRICE
# ============================================================
# Use lag() to get the previous trading day's close price for each stock
# This enables daily return calculations in the next step
gold_df = silver_df.withColumn(
    "previous_close",
    F.lag("close_price").over(stock_window)
)


# COMMAND ----------

# DBTITLE 1,Preview Gold DataFrame with Previous Close
# Preview the DataFrame with the new previous_close column
display(gold_df)

# COMMAND ----------

# DBTITLE 1,Calculate Daily Return Percentage
# ============================================================
# 4. CALCULATE DAILY RETURN PERCENTAGE
# ============================================================
# Compute the percentage change from previous close to current close:
#   formula: ((close_price - previous_close) / previous_close) * 100
# Rounded to 2 decimal places
gold_df = gold_df.withColumn(
    "daily_return_pct",
    F.round(
        (
            (F.col("close_price") - F.col("previous_close"))
            / F.col("previous_close")
        ) * 100,
        2
    )
)

# COMMAND ----------

# DBTITLE 1,Define 7-Day Moving Window
# ============================================================
# 5. DEFINE 7-DAY MOVING WINDOW
# ============================================================
# Window spanning the current row and 6 preceding rows (7 days total)
# Used for 7-day moving averages and volatility calculations
moving_7_day_window = Window.partitionBy("symbol").orderBy(F.col("trade_date")).rowsBetween(-6, 0)

# COMMAND ----------

# DBTITLE 1,Calculate 7-Day Moving Average
# ============================================================
# 6. CALCULATE 7-DAY MOVING AVERAGE OF CLOSE PRICE
# ============================================================
# Average the close_price over the 7-day rolling window for each stock
gold_df = gold_df.withColumn(
    "7_days_avg",
    F.avg("close_price").over(moving_7_day_window)
)

# COMMAND ----------

# DBTITLE 1,Define 30-Day Window & Calculate 30-Day Moving Average
# ============================================================
# 7. DEFINE 30-DAY WINDOW & CALCULATE 30-DAY MOVING AVERAGE
# ============================================================
# Window spanning the current row and 29 preceding rows (30 days total)
moving_30_days_window = Window.partitionBy("symbol").orderBy(F.col("trade_date")).rowsBetween(-29, 0)

# Average the close_price over the 30-day rolling window for each stock
gold_df = gold_df.withColumn(
    "30_days_avg",
    F.avg("close_price").over(moving_30_days_window)
)

# COMMAND ----------

# DBTITLE 1,Preview Gold DataFrame with Moving Averages
# Preview the DataFrame with 7-day and 30-day moving averages
display(gold_df)

# COMMAND ----------

# DBTITLE 1,Round Moving Averages & Preview
# ============================================================
# 8. ROUND MOVING AVERAGES TO 2 DECIMAL PLACES
# ============================================================
# Round both the 7-day and 30-day moving averages for cleaner output
gold_df = gold_df.withColumn(
    "7_days_avg",
    F.round(F.col("7_days_avg"), 2)
)

gold_df = gold_df.withColumn(
    "30_days_avg",
    F.round(F.col("30_days_avg"), 2)
)

# Preview the rounded moving averages
display(gold_df)

# COMMAND ----------

# DBTITLE 1,Calculate Average Volume (7-Day & 30-Day)
# ============================================================
# 9. CALCULATE 7-DAY & 30-DAY AVERAGE VOLUME
# ============================================================
# Compute rolling average trading volume over 7-day and 30-day windows
# Rounded to 2 decimal places
gold_df = gold_df.withColumn(
    "avg_volume_7d",
    F.round(F.avg("volume").over(moving_7_day_window), 2)
)

gold_df = gold_df.withColumn(
    "avg_volume_30d",
    F.round(F.avg("volume").over(moving_30_days_window), 2)
)

# COMMAND ----------

# DBTITLE 1,Calculate Volatility (7-Day & 30-Day)
# ============================================================
# 10. CALCULATE 7-DAY & 30-DAY VOLATILITY
# ============================================================
# Compute the standard deviation of daily_return_pct over rolling windows
# This measures price volatility — higher values indicate more volatile stocks
# Rounded to 2 decimal places
gold_df = gold_df.withColumn(
    "volatility_7d",
    F.round(F.stddev("daily_return_pct").over(moving_7_day_window), 2)
)

gold_df = gold_df.withColumn(
    "volatility_30d",
    F.round(F.stddev("daily_return_pct").over(moving_30_days_window), 2)
)

# Preview the final gold DataFrame with all metrics
display(gold_df)

# COMMAND ----------

# DBTITLE 1,Write Gold Data to Delta Table
# ============================================================
# 11. WRITE GOLD DATA TO DELTA TABLE IN UNITY CATALOG
# ============================================================
# Persist the fully transformed gold DataFrame as a Delta table
# Using overwrite mode with schema overwrite enabled for full refresh
(
    gold_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("stock_catalog.gold.stock_data")
)