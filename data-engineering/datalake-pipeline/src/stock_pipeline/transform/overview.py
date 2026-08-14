"""
Company Overview — Bronze-to-Silver Transformation Module.

Transforms raw Bronze Alpha Vantage Company Overview fundamental reference data (JSON)
into a clean, typed, standardized Silver DataFrame.

Overview Data Characteristics:
  - Reference / Master data snapshot (contains 50+ financial attributes per stock symbol).
  - Attributes include Market Cap, P/E Ratio, EPS, Revenue, EBITDA, 52-Week High/Low, Ratings.

Transformation Pipeline Steps:
  1. Column Selection     ──► Extract 52 standard business columns from raw Bronze payload.
  2. Column Renaming      ──► Rename PascalCase API keys to standard Python snake_case.
  3. String Standardization──► Trim spaces, clean non-alphanumeric noise, convert case (upper/lower).
  4. Fake Null Replacement──► Map invalid text values ("n/a", "none", "-") to NULL.
  5. Default Imputation   ──► Fill missing text values (Country -> "Unknown", Sector -> "Unknown").
  6. Explicit Type Casting──► Cast string values to Integer, Long, Double, or Date data types.
  7. Partition Metadata   ──► Generate `year`, `month`, `day` execution partition keys.
  8. Timestamp Audit      ──► Add `processed_at` UTC timestamp column.
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
    Transform Alpha Vantage Company Overview Bronze JSON data into a cleaned, typed Silver DataFrame.

    Args:
        spark (SparkSession): Active PySpark session.
        data_df (DataFrame): Raw Bronze Overview PySpark DataFrame.

    Returns:
        DataFrame: Transformed Silver overview DataFrame with snake_case schema and typed columns.
    """
    logger.info("[TRANSFORM][OVERVIEW_START] Starting company overview transformation.")

    try:
        # ==============================================================================
        # Step 1: Select only the required 52 business fields
        # ==============================================================================
        data_df = data_df.select(*OVERVIEW_BUSINESS_COLUMNS)

        # ==============================================================================
        # Step 2: Rename columns to standardized snake_case schema
        # ==============================================================================
        for old_name, new_name in OVERVIEW_COLUMN_MAPPING.items():
            if old_name in data_df.columns:
                data_df = data_df.withColumnRenamed(old_name, new_name)

        # ==============================================================================
        # Step 3: Trim whitespace and clean string formatting
        # ==============================================================================
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

        # Clean special characters from string fields
        for column in string_columns:
            data_df = data_df.withColumn(
                column,
                regexp_replace(
                    col(column),
                    "[^A-Za-z &]",
                    ""
                )
            )

        # Uppercase code attributes
        data_df = data_df.withColumn("symbol", upper(col("symbol")))
        data_df = data_df.withColumn("exchange", upper(col("exchange")))
        data_df = data_df.withColumn("currency", upper(col("currency")))

        # Extract secondary word from asset type if present
        data_df = data_df.withColumn(
            "asset_type",
            split(col("asset_type"), " ")[1]
        )

        # Lowercase text attributes for case-insensitive querying
        data_df = data_df.withColumn("company_name", lower(col("company_name")))
        data_df = data_df.withColumn("industry", lower(col("industry")))
        data_df = data_df.withColumn("fiscal_year_end", lower(col("fiscal_year_end")))
        data_df = data_df.withColumn("sector", lower(col("sector")))

        # ==============================================================================
        # Step 4: Normalize Fake Nulls ("n/a", "-", "none" -> NULL)
        # ==============================================================================
        fake_null_values = ["", "n/a", "na", "null", "none", "-"]

        for col_name in OVERVIEW_HASH_COLUMNS:
            data_df = data_df.withColumn(
                col_name,
                when(
                    lower(col(col_name)).isin(fake_null_values),
                    None,
                ).otherwise(col(col_name)),
            )

        # ==============================================================================
        # Step 5: Default Value Imputation for Missing Text Fields
        # ==============================================================================
        data_df = data_df.fillna({
            "country": "Unknown",
            "sector": "Unknown",
            "industry": "Unknown",
            "official_site": "Not Available",
        })

        # ==============================================================================
        # Step 6: Explicit Data Type Casting
        # ==============================================================================
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

        # ==============================================================================
        # Step 7: Partitioning Keys & Processing Timestamp
        # ==============================================================================
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

        logger.info("[TRANSFORM][OVERVIEW_OK] Company overview transformation completed successfully.")
        return data_df

    except Exception as e:
        logger.exception("[TRANSFORM][OVERVIEW_FAIL] Error transforming overview data: %s", e)
        raise
