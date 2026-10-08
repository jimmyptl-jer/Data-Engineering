# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Alpha Vantage Company Overview ETL
# MAGIC
# MAGIC Transforms raw Alpha Vantage Company Overview data from the Bronze layer into a clean, standardized Silver dataset using PySpark.

# COMMAND ----------

# DBTITLE 1,Imports
"""
============================================================
ALPHA VANTAGE COMPANY OVERVIEW - PYSPARK ETL
============================================================

PURPOSE:
--------
Transform raw Alpha Vantage Company Overview data from the
Bronze layer into a clean, standardized Silver dataset.

TRANSFORMATION FLOW:
--------------------
1. Read Bronze Company Overview data.
2. Select required business columns.
3. Rename API fields to snake_case.
4. Trim whitespace and standardize string values.
5. Normalize fake NULL values.
6. Apply default values.
7. Cast numeric columns to proper data types.
8. Cast date columns to DATE.
9. Remove duplicate company records.
10. Add partition columns.
11. Add processing timestamp.
12. Write the final data to Silver as Parquet.

OUTPUT:
-------
Silver:
s3://graywolf--data--lake/stock/silver/
source=alphavantage/dataset=company_overview/

FORMAT:
-------
Parquet

PARTITION:
----------
year / month / day

============================================================
"""

from pyspark.sql.functions import (
    col,
    current_date,
    current_timestamp,
    dayofmonth,
    from_utc_timestamp,
    lower,
    regexp_replace,
    split,
    to_date,
    trim,
    upper,
    year,
    month,
    when
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType
)

# COMMAND ----------

# DBTITLE 1,1. Read Bronze
# ============================================================
# 1. READ BRONZE
# ============================================================

bronze_path = (
    "s3://graywolf--data--lake/"
    "stock/bronze/"
    "source=alphavantage/"
    "dataset=company_overview/"
)

company_overview_df = spark.read.json(bronze_path)

print("[BRONZE] Company Overview data loaded successfully")

# COMMAND ----------

company_overview_df.printSchema()

# COMMAND ----------

# DBTITLE 1,2. Select Business Columns
# ============================================================
# 2. SELECT BUSINESS COLUMNS
# ============================================================

OVERVIEW_BUSINESS_COLUMNS = [
    "Symbol",
    "AssetType",
    "Name",
    "CIK",
    "Exchange",
    "Currency",
    "Country",
    "Sector",
    "Industry",
    "OfficialSite",
    "FiscalYearEnd",
    "LatestQuarter",
    "MarketCapitalization",
    "EBITDA",
    "PERatio",
    "PEGRatio",
    "BookValue",
    "DividendPerShare",
    "DividendYield",
    "EPS",
    "RevenuePerShareTTM",
    "ProfitMargin",
    "OperatingMarginTTM",
    "ReturnOnAssetsTTM",
    "ReturnOnEquityTTM",
    "RevenueTTM",
    "GrossProfitTTM",
    "DilutedEPSTTM",
    "QuarterlyEarningsGrowthYOY",
    "QuarterlyRevenueGrowthYOY",
    "AnalystTargetPrice",
    "AnalystRatingStrongBuy",
    "AnalystRatingBuy",
    "AnalystRatingHold",
    "AnalystRatingSell",
    "AnalystRatingStrongSell",
    "TrailingPE",
    "ForwardPE",
    "PriceToSalesRatioTTM",
    "PriceToBookRatio",
    "EVToRevenue",
    "EVToEBITDA",
    "Beta",
    "52WeekHigh",
    "52WeekLow",
    "50DayMovingAverage",
    "200DayMovingAverage",
    "SharesOutstanding",
    "SharesFloat",
    "PercentInsiders",
    "PercentInstitutions",
    "DividendDate",
    "ExDividendDate"
]

company_overview_df = company_overview_df.select(
    *OVERVIEW_BUSINESS_COLUMNS
)

print("[TRANSFORMATION 1] Business columns selected")

# COMMAND ----------

# DBTITLE 1,3. Rename Columns
# ============================================================
# 3. RENAME COLUMNS
# ============================================================

OVERVIEW_COLUMN_MAPPING = {
    "Symbol": "symbol",
    "AssetType": "asset_type",
    "Name": "company_name",
    "CIK": "cik",
    "Exchange": "exchange",
    "Currency": "currency",
    "Country": "country",
    "Sector": "sector",
    "Industry": "industry",
    "OfficialSite": "official_site",
    "FiscalYearEnd": "fiscal_year_end",
    "LatestQuarter": "latest_quarter",
    "MarketCapitalization": "market_cap",
    "EBITDA": "ebitda",
    "PERatio": "pe_ratio",
    "PEGRatio": "peg_ratio",
    "BookValue": "book_value",
    "DividendPerShare": "dividend_per_share",
    "DividendYield": "dividend_yield",
    "EPS": "eps",
    "RevenuePerShareTTM": "revenue_per_share_ttm",
    "ProfitMargin": "profit_margin",
    "OperatingMarginTTM": "operating_margin_ttm",
    "ReturnOnAssetsTTM": "return_on_assets_ttm",
    "ReturnOnEquityTTM": "return_on_equity_ttm",
    "RevenueTTM": "revenue_ttm",
    "GrossProfitTTM": "gross_profit_ttm",
    "DilutedEPSTTM": "diluted_eps_ttm",
    "QuarterlyEarningsGrowthYOY": "quarterly_earnings_growth_yoy",
    "QuarterlyRevenueGrowthYOY": "quarterly_revenue_growth_yoy",
    "AnalystTargetPrice": "analyst_target_price",
    "AnalystRatingStrongBuy": "analyst_rating_strong_buy",
    "AnalystRatingBuy": "analyst_rating_buy",
    "AnalystRatingHold": "analyst_rating_hold",
    "AnalystRatingSell": "analyst_rating_sell",
    "AnalystRatingStrongSell": "analyst_rating_strong_sell",
    "TrailingPE": "trailing_pe",
    "ForwardPE": "forward_pe",
    "PriceToSalesRatioTTM": "price_to_sales_ratio_ttm",
    "PriceToBookRatio": "price_to_book_ratio",
    "EVToRevenue": "ev_to_revenue",
    "EVToEBITDA": "ev_to_ebitda",
    "Beta": "beta",
    "52WeekHigh": "fifty_two_week_high",
    "52WeekLow": "fifty_two_week_low",
    "50DayMovingAverage": "fifty_day_moving_average",
    "200DayMovingAverage": "two_hundred_day_moving_average",
    "SharesOutstanding": "shares_outstanding",
    "SharesFloat": "shares_float",
    "PercentInsiders": "percent_insiders",
    "PercentInstitutions": "percent_institutions",
    "DividendDate": "dividend_date",
    "ExDividendDate": "ex_dividend_date"
}

for old_name, new_name in OVERVIEW_COLUMN_MAPPING.items():
    company_overview_df = company_overview_df.withColumnRenamed(
        old_name,
        new_name
    )

print("[TRANSFORMATION 2] Columns renamed to snake_case")

# COMMAND ----------

# DBTITLE 1,4. Trim & Standardize Strings
# ============================================================
# 4. TRIM AND STANDARDIZE STRING COLUMNS
# ============================================================

string_columns = [
    "symbol",
    "asset_type",
    "company_name",
    "exchange",
    "currency",
    "country",
    "sector",
    "industry",
    "fiscal_year_end"
]

for column_name in string_columns:
    company_overview_df = company_overview_df.withColumn(
        column_name,
        trim(col(column_name))
    )

for column_name in string_columns:
    company_overview_df = company_overview_df.withColumn(
        column_name,
        regexp_replace(
            col(column_name),
            "[^A-Za-z &]",
            ""
        )
    )

company_overview_df = (
    company_overview_df
    .withColumn("symbol", upper(col("symbol")))
    .withColumn("exchange", upper(col("exchange")))
    .withColumn("currency", upper(col("currency")))
    .withColumn("asset_type", split(col("asset_type"), " ")[1])
    .withColumn("company_name", lower(col("company_name")))
    .withColumn("industry", lower(col("industry")))
    .withColumn("fiscal_year_end", lower(col("fiscal_year_end")))
    .withColumn("sector", lower(col("sector")))
)

print("[TRANSFORMATION 3] String values cleaned and standardized")

# COMMAND ----------

# DBTITLE 1,5. Normalize Fake NULL Values
# ============================================================
# 5. NORMALIZE FAKE NULL VALUES
# ============================================================

fake_null_values = [
    "",
    "n/a",
    "na",
    "null",
    "none",
    "-"
]

null_normalize_exprs = {
    col_name: when(
        lower(trim(col(col_name).cast("string"))).isin(fake_null_values),
        None
    ).otherwise(col(col_name))
    for col_name in company_overview_df.columns
}

company_overview_df = company_overview_df.withColumns(null_normalize_exprs)

print("[TRANSFORMATION 4] Fake NULL values normalized")

# COMMAND ----------

# DBTITLE 1,6. Apply Default Values
# ============================================================
# 6. DEFAULT VALUES
# ============================================================

company_overview_df = company_overview_df.fillna({
    "country": "Unknown",
    "sector": "Unknown",
    "industry": "Unknown",
    "official_site": "Not Available"
})

print("[TRANSFORMATION 5] Default values applied")

# COMMAND ----------

# DBTITLE 1,7. Cast Numeric Columns
# ============================================================
# 7. CAST NUMERIC COLUMNS
# ============================================================

integer_columns = [
    "cik",
    "analyst_rating_strong_buy",
    "analyst_rating_buy",
    "analyst_rating_hold",
    "analyst_rating_sell",
    "analyst_rating_strong_sell"
]

long_columns = [
    "market_cap",
    "ebitda",
    "revenue_ttm",
    "gross_profit_ttm",
    "shares_outstanding",
    "shares_float"
]

double_columns = [
    "pe_ratio",
    "peg_ratio",
    "book_value",
    "dividend_per_share",
    "dividend_yield",
    "eps",
    "revenue_per_share_ttm",
    "profit_margin",
    "operating_margin_ttm",
    "return_on_assets_ttm",
    "return_on_equity_ttm",
    "diluted_eps_ttm",
    "quarterly_earnings_growth_yoy",
    "quarterly_revenue_growth_yoy",
    "analyst_target_price",
    "trailing_pe",
    "forward_pe",
    "price_to_sales_ratio_ttm",
    "price_to_book_ratio",
    "ev_to_revenue",
    "ev_to_ebitda",
    "beta",
    "fifty_two_week_high",
    "fifty_two_week_low",
    "fifty_day_moving_average",
    "two_hundred_day_moving_average",
    "percent_insiders",
    "percent_institutions"
]

numeric_cast_exprs = {}
for column_name in integer_columns:
    numeric_cast_exprs[column_name] = col(column_name).cast(IntegerType())
for column_name in long_columns:
    numeric_cast_exprs[column_name] = col(column_name).cast(LongType())
for column_name in double_columns:
    numeric_cast_exprs[column_name] = col(column_name).cast(DoubleType())

company_overview_df = company_overview_df.withColumns(numeric_cast_exprs)

print("[TRANSFORMATION 6] Numeric columns cast successfully")

# COMMAND ----------

# DBTITLE 1,8. Cast Date Columns
# ============================================================
# 8. CAST DATE COLUMNS
# ============================================================

date_columns = [
    "latest_quarter",
    "dividend_date",
    "ex_dividend_date"
]

date_cast_exprs = {
    col_name: to_date(col(col_name), "yyyy-MM-dd")
    for col_name in date_columns
}

company_overview_df = company_overview_df.withColumns(date_cast_exprs)

print("[TRANSFORMATION 7] Date columns cast successfully")

# COMMAND ----------

# DBTITLE 1,9. Remove Duplicates
# ============================================================
# 9. REMOVE DUPLICATES
# ============================================================

company_overview_df = company_overview_df.dropDuplicates(["symbol"])

print("[TRANSFORMATION 8] Duplicate company records removed")

# COMMAND ----------

# DBTITLE 1,10. Add Partition Columns
# ============================================================
# 10. ADD PARTITION COLUMNS
# ============================================================

company_overview_df = (
    company_overview_df
    .withColumn("year", year(current_date()))
    .withColumn("month", month(current_date()))
    .withColumn("day", dayofmonth(current_date()))
)

print("[TRANSFORMATION 9] Partition columns added")

# COMMAND ----------

# DBTITLE 1,11. Add Processing Timestamp
# ============================================================
# 11. ADD PROCESSING TIMESTAMP
# ============================================================

company_overview_df = company_overview_df.withColumn(
    "processed_at",
    from_utc_timestamp(
        current_timestamp(),
        "Asia/Kolkata"
    )
)

print("[TRANSFORMATION 10] processed_at timestamp added")

# COMMAND ----------

# DBTITLE 1,12. Final Schema & Sample
# ============================================================
# 12. FINAL DATA
# ============================================================

print("[SILVER] Final schema:")
company_overview_df.printSchema()

print("[SILVER] Sample records:")
display(company_overview_df.limit(5))

# COMMAND ----------

# DBTITLE 1,13. Record Count
# ============================================================
# 13. RECORD COUNT
# ============================================================

total_records = company_overview_df.count()

print(f"[SILVER] Final record count: {total_records}")

# COMMAND ----------

# DBTITLE 1,14. Write Silver
# ============================================================
# 14. WRITE SILVER
# ============================================================

silver_path = (
    "s3://graywolf--data--lake/"
    "stock/silver/"
    "source=alphavantage/"
    "dataset=company_overview/"
)

silver_delta_table = "stock_catalog.silver.company_overview_data"

print(f"[SILVER] Writing data to: {silver_path}")

(company_overview_df.write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(silver_path)
)

(
    company_overview_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(silver_delta_table)
)


print(f"[SILVER] Company Overview written successfully to {silver_path}")