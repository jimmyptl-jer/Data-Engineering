"""
Company Overview — Bronze-to-Silver Transformation Module.

Transforms raw Bronze Alpha Vantage Company Overview fundamental reference data (JSON)
into a clean, typed, standardized Silver DataFrame.

Company Overview — Bronze-to-Silver Transformation Module.

Transforms raw Bronze Alpha Vantage Company Overview data from Bronze JSON
into a clean, typed, standardized Silver-ready PySpark DataFrame.

Transformation Flow:
    1. Select 52 standard business columns
    2. Rename PascalCase API keys to snake_case
    3. Trim whitespace and standardize casing
    4. Normalize fake-null tokens
    5. Default value imputation for missing text fields
    6. Explicit data type casting
    7. Partition keys & processing timestamp
    8. Row count metrics
    9. Return Silver DataFrame

Dataset:
  - `company_overview`

Fields Processed:

  Dataset Columns (52 Business Attributes)
  ['Symbol', 'AssetType', 'Name', 'CIK', 'Exchange', 'Currency', 'Country', 'Sector', 'Industry', ...]

  root
    |-- Symbol: string (nullable = true)
    |-- AssetType: string (nullable = true)
    |-- Name: string (nullable = true)
    |-- CIK: string (nullable = true)
    |-- Exchange: string (nullable = true)
    |-- Currency: string (nullable = true)
    |-- Country: string (nullable = true)
    |-- Sector: string (nullable = true)
    |-- Industry: string (nullable = true)
    |-- OfficialSite: string (nullable = true)
    |-- FiscalYearEnd: string (nullable = true)
    |-- LatestQuarter: string (nullable = true)
    |-- MarketCapitalization: string (nullable = true)
    |-- EBITDA: string (nullable = true)
    |-- PERatio: string (nullable = true)
    |-- PEGRatio: string (nullable = true)
    |-- BookValue: string (nullable = true)
    |-- DividendPerShare: string (nullable = true)
    |-- DividendYield: string (nullable = true)
    |-- EPS: string (nullable = true)
    |-- RevenuePerShareTTM: string (nullable = true)
    |-- ProfitMargin: string (nullable = true)
    |-- OperatingMarginTTM: string (nullable = true)
    |-- ReturnOnAssetsTTM: string (nullable = true)
    |-- ReturnOnEquityTTM: string (nullable = true)
    |-- RevenueTTM: string (nullable = true)
    |-- GrossProfitTTM: string (nullable = true)
    |-- DilutedEPSTTM: string (nullable = true)
    |-- QuarterlyEarningsGrowthYOY: string (nullable = true)
    |-- QuarterlyRevenueGrowthYOY: string (nullable = true)
    |-- AnalystTargetPrice: string (nullable = true)
    |-- AnalystRatingStrongBuy: string (nullable = true)
    |-- AnalystRatingBuy: string (nullable = true)
    |-- AnalystRatingHold: string (nullable = true)
    |-- AnalystRatingSell: string (nullable = true)
    |-- AnalystRatingStrongSell: string (nullable = true)
    |-- TrailingPE: string (nullable = true)
    |-- ForwardPE: string (nullable = true)
    |-- PriceToSalesRatioTTM: string (nullable = true)
    |-- PriceToBookRatio: string (nullable = true)
    |-- EVToRevenue: string (nullable = true)
    |-- EVToEBITDA: string (nullable = true)
    |-- Beta: string (nullable = true)
    |-- 52WeekHigh: string (nullable = true)
    |-- 52WeekLow: string (nullable = true)
    |-- 50DayMovingAverage: string (nullable = true)
    |-- 200DayMovingAverage: string (nullable = true)
    |-- SharesOutstanding: string (nullable = true)
    |-- SharesFloat: string (nullable = true)
    |-- PercentInsiders: string (nullable = true)
    |-- PercentInstitutions: string (nullable = true)
    |-- DividendDate: string (nullable = true)
    |-- ExDividendDate: string (nullable = true)

    +------+----------+---------------------------+--------+--------+--------+--------------+------------+---------------------+
    |symbol|asset_type|              company_name |exchange|currency|country |        sector|   industry |       official_site  |
    +------+----------+---------------------------+--------+--------+--------+--------------+------------+---------------------+
    |   IBM|    Stock |international business ... |    NYSE|     USD|     USA|    technology|  it services|    https://ibm.com  |
    |  AAPL|    Stock |               apple inc   |  NASDAQ|     USD|     USA|    technology|  electronics|  https://apple.com  |
    +------+----------+---------------------------+--------+--------+--------+--------------+------------+---------------------+

"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    current_date,
    current_timestamp,
    dayofmonth,
    from_utc_timestamp,
    lower,
    month,
    regexp_replace,
    split,
    to_date,
    trim,
    upper,
    when,
    year,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
)

logger = logging.getLogger(__name__)


# 52 Standard business columns selected from raw Alpha Vantage Overview JSON
OVERVIEW_BUSINESS_COLUMNS = [
    "Symbol", "AssetType", "Name", "CIK", "Exchange", "Currency",
    "Country", "Sector", "Industry", "OfficialSite", "FiscalYearEnd",
    "LatestQuarter", "MarketCapitalization", "EBITDA", "PERatio",
    "PEGRatio", "BookValue", "DividendPerShare", "DividendYield",
    "EPS", "RevenuePerShareTTM", "ProfitMargin", "OperatingMarginTTM",
    "ReturnOnAssetsTTM", "ReturnOnEquityTTM", "RevenueTTM",
    "GrossProfitTTM", "DilutedEPSTTM", "QuarterlyEarningsGrowthYOY",
    "QuarterlyRevenueGrowthYOY", "AnalystTargetPrice",
    "AnalystRatingStrongBuy", "AnalystRatingBuy", "AnalystRatingHold",
    "AnalystRatingSell", "AnalystRatingStrongSell", "TrailingPE",
    "ForwardPE", "PriceToSalesRatioTTM", "PriceToBookRatio",
    "EVToRevenue", "EVToEBITDA", "Beta", "52WeekHigh", "52WeekLow",
    "50DayMovingAverage", "200DayMovingAverage", "SharesOutstanding",
    "SharesFloat", "PercentInsiders", "PercentInstitutions",
    "DividendDate", "ExDividendDate",
]

# Mapping dictionary: converts API PascalCase field names to snake_case database schema names
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
    "ExDividendDate": "ex_dividend_date",
}

# List of business columns used for calculating hash digest (S3 watermark comparison)
OVERVIEW_HASH_COLUMNS = [
    "symbol", "asset_type", "company_name", "cik", "exchange", "currency",
    "country", "sector", "industry", "official_site", "fiscal_year_end",
    "latest_quarter", "market_cap", "ebitda", "pe_ratio", "peg_ratio",
    "book_value", "dividend_per_share", "dividend_yield", "eps",
    "revenue_per_share_ttm", "profit_margin", "operating_margin_ttm",
    "return_on_assets_ttm", "return_on_equity_ttm", "revenue_ttm",
    "gross_profit_ttm", "diluted_eps_ttm", "quarterly_earnings_growth_yoy",
    "quarterly_revenue_growth_yoy", "analyst_target_price",
    "analyst_rating_strong_buy", "analyst_rating_buy", "analyst_rating_hold",
    "analyst_rating_sell", "analyst_rating_strong_sell", "trailing_pe",
    "forward_pe", "price_to_sales_ratio_ttm", "price_to_book_ratio",
    "ev_to_revenue", "ev_to_ebitda", "beta", "fifty_two_week_high",
    "fifty_two_week_low", "fifty_day_moving_average",
    "two_hundred_day_moving_average", "shares_outstanding", "shares_float",
    "percent_insiders", "percent_institutions", "dividend_date",
    "ex_dividend_date",
]


def silver_transform_overview(
    spark: SparkSession,
    data_df: DataFrame,
) -> DataFrame:
    """
    Transform Alpha Vantage Company Overview Bronze JSON data
    into a cleaned, typed Silver DataFrame.
    """

    # ============================================================
    # STEP 1: START COMPANY OVERVIEW TRANSFORMATION
    # ============================================================


    logger.info(
        "[ALPHAVANTAGE][OVERVIEW] Starting Bronze-to-Silver "
        "transformation."
    )

    try:

        # ========================================================
        # STEP 2: SELECT 52 BUSINESS COLUMNS
        # ========================================================


        data_df = data_df.select(*OVERVIEW_BUSINESS_COLUMNS)

        logger.info(
            "[ALPHAVANTAGE][OVERVIEW] 52 business columns selected."
        )

        # ========================================================
        # STEP 3: RENAME TO SNAKE_CASE
        # ========================================================


        for old_name, new_name in OVERVIEW_COLUMN_MAPPING.items():
            if old_name in data_df.columns:
                data_df = data_df.withColumnRenamed(old_name, new_name)

        logger.info(
            "[ALPHAVANTAGE][OVERVIEW] Column renaming completed | "
            "PascalCase → snake_case."
        )

        # ========================================================
        # STEP 4: TRIM WHITESPACE & STANDARDIZE CASING
        # ========================================================


        string_columns = [
            "symbol",
            "asset_type",
            "company_name",
            "exchange",
            "currency",
            "country",
            "sector",
            "industry",
            "fiscal_year_end",
        ]

        for col_name in string_columns:
            data_df = data_df.withColumn(col_name, trim(col(col_name)))

        for column in string_columns:
            data_df = data_df.withColumn(
                column,
                regexp_replace(
                    col(column),
                    "[^A-Za-z &]",
                    ""
                )
            )

        data_df = data_df.withColumn("symbol", upper(col("symbol")))
        data_df = data_df.withColumn("exchange", upper(col("exchange")))
        data_df = data_df.withColumn("currency", upper(col("currency")))

        data_df = data_df.withColumn(
            "asset_type",
            split(col("asset_type"), " ")[1]
        )

        data_df = data_df.withColumn("company_name", lower(col("company_name")))
        data_df = data_df.withColumn("industry", lower(col("industry")))
        data_df = data_df.withColumn("fiscal_year_end", lower(col("fiscal_year_end")))
        data_df = data_df.withColumn("sector", lower(col("sector")))

        logger.info(
            "[ALPHAVANTAGE][OVERVIEW] Whitespace trimming and casing "
            "standardization completed."
        )

        # ========================================================
        # STEP 5: NORMALIZE FAKE NULL VALUES
        # ========================================================


        fake_null_values = ["", "n/a", "na", "null", "none", "-"]

        for col_name in OVERVIEW_HASH_COLUMNS:
            data_df = data_df.withColumn(
                col_name,
                when(
                    lower(col(col_name)).isin(fake_null_values),
                    None,
                ).otherwise(col(col_name)),
            )

        logger.info(
            "[ALPHAVANTAGE][OVERVIEW] Fake-null normalization completed."
        )

        # ========================================================
        # STEP 6: DEFAULT VALUE IMPUTATION
        # ========================================================


        data_df = data_df.fillna({
            "country": "Unknown",
            "sector": "Unknown",
            "industry": "Unknown",
            "official_site": "Not Available",
        })

        logger.info(
            "[ALPHAVANTAGE][OVERVIEW] Default value imputation completed."
        )

        # ========================================================
        # STEP 7: DATA TYPE CASTING
        # ========================================================


        integer_columns = [
            "cik", "analyst_rating_strong_buy", "analyst_rating_buy",
            "analyst_rating_hold", "analyst_rating_sell",
            "analyst_rating_strong_sell",
        ]

        long_columns = [
            "market_cap", "ebitda", "revenue_ttm", "gross_profit_ttm",
            "shares_outstanding", "shares_float",
        ]

        double_columns = [
            "pe_ratio", "peg_ratio", "book_value", "dividend_per_share",
            "dividend_yield", "eps", "revenue_per_share_ttm", "profit_margin",
            "operating_margin_ttm", "return_on_assets_ttm",
            "return_on_equity_ttm", "diluted_eps_ttm",
            "quarterly_earnings_growth_yoy", "quarterly_revenue_growth_yoy",
            "analyst_target_price", "trailing_pe", "forward_pe",
            "price_to_sales_ratio_ttm", "price_to_book_ratio",
            "ev_to_revenue", "ev_to_ebitda", "beta", "fifty_two_week_high",
            "fifty_two_week_low", "fifty_day_moving_average",
            "two_hundred_day_moving_average", "percent_insiders",
            "percent_institutions",
        ]

        date_columns = ["latest_quarter", "dividend_date", "ex_dividend_date"]

        for c in integer_columns:
            data_df = data_df.withColumn(c, col(c).cast(IntegerType()))

        for c in long_columns:
            data_df = data_df.withColumn(c, col(c).cast(LongType()))

        for c in double_columns:
            data_df = data_df.withColumn(c, col(c).cast(DoubleType()))

        for c in date_columns:
            data_df = data_df.withColumn(c, to_date(col(c), "yyyy-MM-dd"))

        logger.info(
            "[ALPHAVANTAGE][OVERVIEW] Type casting completed | "
            "integer=%d | long=%d | double=%d | date=%d",
            len(integer_columns),
            len(long_columns),
            len(double_columns),
            len(date_columns),
        )

        # ========================================================
        # STEP 8: PARTITION KEYS & PROCESSING TIMESTAMP
        # ========================================================


        data_df = (
            data_df
            .withColumn("year", year(current_date()))
            .withColumn("month", month(current_date()))
            .withColumn("day", dayofmonth(current_date()))
        )

        data_df = data_df.withColumn(
            "processed_at",
            from_utc_timestamp(current_timestamp(), "Asia/Kolkata"),
        )

        logger.info(
            "[ALPHAVANTAGE][OVERVIEW] Partition keys and "
            "processed_at timestamp added."
        )

        # ========================================================
        # STEP 9: ROW COUNT METRICS
        # ========================================================

        total_out = data_df.count()

        logger.info(
            "[ALPHAVANTAGE][OVERVIEW][METRICS] "
            "total_out=%d",
            total_out,
        )

        # ========================================================
        # STEP 10: RETURN SILVER DATAFRAME
        # ========================================================


        logger.info(
            "[ALPHAVANTAGE][OVERVIEW] Bronze-to-Silver transformation "
            "completed successfully."
        )

        return data_df

    except Exception as e:

        logger.exception(
            "[ALPHAVANTAGE][OVERVIEW] Error transforming overview "
            "data: %s", e
        )

        raise
