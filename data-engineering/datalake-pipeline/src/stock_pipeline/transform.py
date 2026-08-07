"""
Transform Stage — Bronze-to-Silver Data Transformations.

Each transform method receives a raw Bronze PySpark DataFrame and executes a
multi-step data processing pipeline, producing a cleaned, validated, enriched Silver-ready DataFrame.

Transformation Pipeline Sequence (Daily Time Series):
  Raw JSON ──► Flatten / Explode ──► Watermark Filter ──► String Trim & Clean ──► Fake Null Normalization
                 │
                 ▼
  Type Casting (double, long, date) ──► Data Quality Validation (VALID/INVALID) ──► Quarantine Invalid Rows
                 │
                 ▼
  Deduplication ──► Rounding ──► Metric Enrichment (daily_change %, Bull/Bear)
                 │
                 ▼
  Rolling Aggregations (30-day Avg, 52-week High/Low, All-time High/Low) ──► Partition Keys (year/month/day)
                 │
                 ▼
  Final Column Ordering & Silver Readyness

Transformation Pipeline Sequence (Company Overview):
  Raw JSON ──► Column Selection (52 business fields) ──► Trim & Upper Symbol ──► Fake Null Normalization
                 │
                 ▼
  Default Value Imputation (Country/Sector) ──► Explicit Type Casting ──► snake_case Mapping ──► Processed Timestamp
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    current_timestamp,
    dayofmonth,
    explode,
    lower,
    month,
    round,
    to_date,
    trim,
    upper,
    weekofyear,
    when,
    year,
    current_date,
    date_sub,
    from_utc_timestamp,
)

from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    DoubleType,
)

logger = logging.getLogger(__name__)


class StockDataTransformer:
    """
    Transforms raw Bronze stock DataFrames into clean, typed, validated, and enriched Silver DataFrames.
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize StockDataTransformer.

        Args:
            spark: Active SparkSession instance.
        """
        self.spark = spark

    # ============================================================
    # WEEKLY TIME SERIES (kept for future architecture expansion)
    # ============================================================

    def transform_weekly_timeseries(
        self,
        data_df: DataFrame,
        debug: bool = False,
    ) -> DataFrame:
        """
        Transform Alpha Vantage Weekly Time Series Bronze data
        into a cleaned, validated, enriched Silver-ready DataFrame.

        Args:
            data_df: Raw Bronze DataFrame matching `stock_schema_weekly`.
            debug: If True, prints schema and sample rows to debug logs.

        Returns:
            DataFrame: Transformed Silver weekly DataFrame.
        """
        logger.info("[TRANSFORM][WEEKLY_START] Starting weekly time-series transformation.")

        try:
            # Step 1: Flatten nested JSON structure
            raw_df = data_df.select(
                col("`Meta Data`.`2. Symbol`").alias("symbol"),
                col("`Meta Data`.`3. Last Refreshed`").alias("last_refreshed"),
                explode(col("`Weekly Time Series`")).alias("week_date", "weekly_data"),
            )

            # Step 2: Select and alias OHLCV fields
            stock_df = raw_df.select(
                col("symbol"),
                col("last_refreshed"),
                col("week_date"),
                col("weekly_data.`1. open`").alias("open"),
                col("weekly_data.`2. high`").alias("high"),
                col("weekly_data.`3. low`").alias("low"),
                col("weekly_data.`4. close`").alias("close"),
                col("weekly_data.`5. volume`").alias("volume"),
            )

            # Step 3: Trim whitespace and uppercase ticker symbol
            string_columns = [
                "symbol", "last_refreshed", "week_date",
                "open", "high", "low", "close", "volume",
            ]

            for column_name in string_columns:
                stock_df = stock_df.withColumn(column_name, trim(col(column_name)))

            stock_df = stock_df.withColumn("symbol", upper(col("symbol")))

            # Normalize string placeholders ("n/a", "none", "-", "null") to Spark NULL
            fake_null_values = ["", "n/a", "na", "null", "none", "-"]

            for column_name in string_columns:
                stock_df = stock_df.withColumn(
                    column_name,
                    when(
                        lower(col(column_name)).isin(fake_null_values),
                        None,
                    ).otherwise(col(column_name)),
                )

            # Step 4: Cast strings to explicit data types
            stock_df = (
                stock_df
                .withColumn("week_date", to_date(col("week_date"), "yyyy-MM-dd"))
                .withColumn("last_refreshed", to_date(col("last_refreshed"), "yyyy-MM-dd"))
                .withColumn("open", col("open").cast("double"))
                .withColumn("high", col("high").cast("double"))
                .withColumn("low", col("low").cast("double"))
                .withColumn("close", col("close").cast("double"))
                .withColumn("volume", col("volume").cast("long"))
            )

            # Step 5: Data Quality Validation
            stock_df = stock_df.withColumn(
                "validation_status",
                when(col("symbol").isNull(), "INVALID")
                .when(col("week_date").isNull(), "INVALID")
                .when(col("last_refreshed").isNull(), "INVALID")
                .when(col("open").isNull(), "INVALID")
                .when(col("high").isNull(), "INVALID")
                .when(col("low").isNull(), "INVALID")
                .when(col("close").isNull(), "INVALID")
                .when(col("volume").isNull(), "INVALID")
                .when(col("open") <= 0, "INVALID")
                .when(col("high") <= 0, "INVALID")
                .when(col("low") <= 0, "INVALID")
                .when(col("close") <= 0, "INVALID")
                .when(col("volume") < 0, "INVALID")
                .when(col("high") < col("low"), "INVALID")
                .otherwise("VALID"),
            )

            # Step 6: Validation Reason Tagging
            stock_df = stock_df.withColumn(
                "validation_reason",
                when(col("symbol").isNull(), "Missing symbol")
                .when(col("week_date").isNull(), "Missing or invalid week date")
                .when(col("last_refreshed").isNull(), "Missing or invalid last refreshed date")
                .when(col("open").isNull(), "Missing or invalid open price")
                .when(col("high").isNull(), "Missing or invalid high price")
                .when(col("low").isNull(), "Missing or invalid low price")
                .when(col("close").isNull(), "Missing or invalid close price")
                .when(col("volume").isNull(), "Missing or invalid volume")
                .when(col("open") <= 0, "Open price must be greater than zero")
                .when(col("high") <= 0, "High price must be greater than zero")
                .when(col("low") <= 0, "Low price must be greater than zero")
                .when(col("close") <= 0, "Close price must be greater than zero")
                .when(col("volume") < 0, "Volume cannot be negative")
                .when(col("high") < col("low"), "High price cannot be lower than low price")
                .otherwise(None),
            )

            # Step 7: Filter VALID Records
            valid_stock_df = stock_df.filter(col("validation_status") == "VALID")

            invalid_record_count = stock_df.filter(col("validation_status") == "INVALID").count()
            logger.info("[TRANSFORM][WEEKLY] Found %d invalid records.", invalid_record_count)

            # Step 8: Deduplicate on (symbol, week_date)
            valid_stock_df = valid_stock_df.dropDuplicates(["symbol", "week_date"])

            # Step 9: Round numeric columns
            valid_stock_df = (
                valid_stock_df
                .withColumn("open", round(col("open"), 2))
                .withColumn("high", round(col("high"), 2))
                .withColumn("low", round(col("low"), 2))
                .withColumn("close", round(col("close"), 2))
            )

            # Step 10: Derived Metrics & Timestamps
            valid_stock_df = (
                valid_stock_df
                .withColumn("weekly_change", round(col("close") - col("open"), 2))
                .withColumn(
                    "weekly_change_percentage",
                    round(((col("close") - col("open")) / col("open")) * 100, 2),
                )
                .withColumn(
                    "market_movement",
                    when(col("close") > col("open"), "Bull")
                    .when(col("close") < col("open"), "Bear")
                    .otherwise("Neutral"),
                )
                .withColumn("year", year(col("week_date")))
                .withColumn("month", month(col("week_date")))
                .withColumn("week_number", weekofyear(col("week_date")))
                .withColumn("processed_at", from_utc_timestamp(current_timestamp(), "Asia/Kolkata"))
                .withColumnRenamed("last_refreshed", "last_refreshed_date")
            )

            # Step 11: Final Selection and Sorting
            valid_stock_df = valid_stock_df.select(
                "symbol", "week_date",
                "open", "high", "low", "close", "volume",
                "weekly_change", "weekly_change_percentage", "market_movement",
                "year", "month", "week_number",
                "last_refreshed_date",
                "validation_status", "validation_reason",
                "processed_at",
            )

            valid_stock_df = valid_stock_df.orderBy(
                col("symbol").asc(),
                col("week_date").desc(),
            )

            if debug:
                logger.debug("[TRANSFORM][WEEKLY] Schema:")
                valid_stock_df.printSchema()
                logger.debug("[TRANSFORM][WEEKLY] Sample data:")
                valid_stock_df.show(10, truncate=False)

            logger.info("[TRANSFORM][WEEKLY_OK] Weekly time-series transformation completed.")
            return valid_stock_df

        except Exception as e:
            logger.exception("[TRANSFORM][WEEKLY_FAIL] Error transforming weekly data: %s", e)
            raise

    # ============================================================
    # DAILY TIME SERIES TRANSFORMATION
    # ============================================================

    def silver_transform_daily_timeseries(
        self,
        daily_dataset: str,
        data_df: DataFrame,
        watermark_value: str | None = None,
        debug: bool = False,
    ) -> DataFrame:
        """
        Transform Alpha Vantage Daily Time Series Bronze data into Silver-ready DataFrame.

        Execution DAG Pipeline Steps:
          1. Flatten nested JSON (`Time Series (Daily)`) via `explode()`.
          2. Alias OHLCV fields.
          3. Apply Date Watermark filter (`day_date > watermark_value`) if provided.
          4. String cleaning & Fake Null normalization (`"n/a"`, `"none"`, `"-"` -> NULL).
          5. Type casting (date, double, long).
          6. Data Quality assertions & quarantine tagging (`VALID` vs `INVALID`).
          7. Filter valid records & count invalids.
          8. Deduplicate on `(symbol, day_date)`.
          9. Round price columns to 2 decimal places.
         10. Enrich metrics (`daily_change`, `daily_change_percentage`, `market_movement`).
         11. Compute 30-day rolling average open & close per symbol.
         12. Compute 52-week high & low per symbol.
         13. Compute all-time high & low per symbol.
         14. Generate partition keys (`year`, `month`, `day`).
         15. Order and select final Silver schema.

        Args:
            daily_dataset: Dataset identifier string (for logging).
            data_df: Raw Bronze PySpark DataFrame matching `stock_schema_daily`.
            watermark_value: Last processed date string (ISO 'YYYY-MM-DD'). If set, filters new records.
            debug: If True, prints schema and sample rows.

        Returns:
            DataFrame: Transformed Silver daily DataFrame (empty if no new records).
        """
        logger.info("[TRANSFORM][DAILY_START] Starting daily time-series transformation pipeline.")

        try:
            # 1. Flatten nested JSON
            logger.info("[TRANSFORM][DAILY_STEP1] Flattening nested JSON structure.")
            raw_df = data_df.select(
                col("`Meta Data`.`2. Symbol`").alias("symbol"),
                col("`Meta Data`.`3. Last Refreshed`").alias("last_refreshed"),
                explode(col("Time Series (Daily)")).alias("day_date", "daily_data"),
            )

            # 2. Select and alias OHLCV columns
            logger.info("[TRANSFORM][DAILY_STEP2] Selecting and aliasing OHLCV fields.")
            stock_df = raw_df.select(
                col("symbol"),
                col("last_refreshed"),
                col("day_date"),
                col("daily_data.`1. open`").alias("open"),
                col("daily_data.`2. high`").alias("high"),
                col("daily_data.`3. low`").alias("low"),
                col("daily_data.`4. close`").alias("close"),
                col("daily_data.`5. volume`").alias("volume"),
            )

            # 3. Watermark Filter (Incremental Load Optimization)
            if watermark_value is not None:
                logger.info(
                    "[TRANSFORM][DAILY_WATERMARK] Applying date watermark filter: day_date > '%s'.",
                    watermark_value,
                )
                stock_df = stock_df.filter(col("day_date") > watermark_value)
            else:
                logger.info("[TRANSFORM][DAILY_WATERMARK] No watermark provided — executing full historical load.")

            # 4. String Cleaning & Fake Null Normalization
            string_columns = [
                "symbol", "last_refreshed", "day_date",
                "open", "high", "low", "close", "volume",
            ]

            for column_name in string_columns:
                stock_df = stock_df.withColumn(column_name, trim(col(column_name)))

            stock_df = stock_df.withColumn("symbol", upper(col("symbol")))

            fake_null_values = ["", "n/a", "na", "null", "none", "-"]

            for column_name in string_columns:
                stock_df = stock_df.withColumn(
                    column_name,
                    when(
                        lower(col(column_name)).isin(fake_null_values),
                        None,
                    ).otherwise(col(column_name)),
                )

            # 5. Data Type Conversion
            stock_df = (
                stock_df
                .withColumn("day_date", to_date(col("day_date"), "yyyy-MM-dd"))
                .withColumn("last_refreshed", to_date(col("last_refreshed"), "yyyy-MM-dd"))
                .withColumn("open", col("open").cast("double"))
                .withColumn("high", col("high").cast("double"))
                .withColumn("low", col("low").cast("double"))
                .withColumn("close", col("close").cast("double"))
                .withColumn("volume", col("volume").cast("long"))
            )

            # 6. Data Quality Validation (Quarantine Pattern)
            stock_df = stock_df.withColumn(
                "validation_status",
                when(col("symbol").isNull(), "INVALID")
                .when(col("day_date").isNull(), "INVALID")
                .when(col("last_refreshed").isNull(), "INVALID")
                .when(col("open").isNull(), "INVALID")
                .when(col("high").isNull(), "INVALID")
                .when(col("low").isNull(), "INVALID")
                .when(col("close").isNull(), "INVALID")
                .when(col("volume").isNull(), "INVALID")
                .when(col("open") <= 0, "INVALID")
                .when(col("high") <= 0, "INVALID")
                .when(col("low") <= 0, "INVALID")
                .when(col("close") <= 0, "INVALID")
                .when(col("volume") < 0, "INVALID")
                .when(col("high") < col("low"), "INVALID")
                .otherwise("VALID"),
            )

            # Validation Reason Tagging for Audit
            stock_df = stock_df.withColumn(
                "validation_reason",
                when(col("symbol").isNull(), "Missing symbol")
                .when(col("day_date").isNull(), "Missing or invalid day date")
                .when(col("last_refreshed").isNull(), "Missing or invalid last refreshed date")
                .when(col("open").isNull(), "Missing or invalid open price")
                .when(col("high").isNull(), "Missing or invalid high price")
                .when(col("low").isNull(), "Missing or invalid low price")
                .when(col("close").isNull(), "Missing or invalid close price")
                .when(col("volume").isNull(), "Missing or invalid volume")
                .when(col("open") <= 0, "Open price must be greater than zero")
                .when(col("high") <= 0, "High price must be greater than zero")
                .when(col("low") <= 0, "Low price must be greater than zero")
                .when(col("close") <= 0, "Close price must be greater than zero")
                .when(col("volume") < 0, "Volume cannot be negative")
                .when(col("high") < col("low"), "High price cannot be lower than low price")
                .otherwise(None),
            )

            # 7. Quarantine Filter: Keep VALID rows for Silver
            valid_stock_df = stock_df.filter(col("validation_status") == "VALID")

            invalid_record_count = stock_df.filter(col("validation_status") == "INVALID").count()
            logger.info("[TRANSFORM][DAILY_DQ] Data Quality check completed. Quarantined invalid records: %d", invalid_record_count)

            # 8. Deduplicate on (symbol, day_date)
            valid_stock_df = valid_stock_df.dropDuplicates(["symbol", "day_date"])

            # 9. Round Price Fields
            valid_stock_df = (
                valid_stock_df
                .withColumn("open", round(col("open"), 2))
                .withColumn("high", round(col("high"), 2))
                .withColumn("low", round(col("low"), 2))
                .withColumn("close", round(col("close"), 2))
            )

            # 10. Metric Enrichment (daily_change, daily_change_percentage, market_movement)
            valid_stock_df = (
                valid_stock_df
                .withColumn("daily_change", round(col("close") - col("open"), 2))
                .withColumn(
                    "daily_change_percentage",
                    round(((col("close") - col("open")) / col("open")) * 100, 2),
                )
                .withColumn(
                    "market_movement",
                    when(col("close") > col("open"), "Bull")
                    .when(col("close") < col("open"), "Bear")
                    .otherwise("Neutral"),
                )
                .withColumn("processed_at", from_utc_timestamp(current_timestamp(), "Asia/Kolkata"))
                .withColumnRenamed("last_refreshed", "last_refreshed_date")
            )

            if debug:
                logger.debug("[TRANSFORM][DAILY] Valid data schema:")
                valid_stock_df.printSchema()

            # 11. Rolling Aggregations: 30-Day Average Open & Close
            logger.info("[TRANSFORM][DAILY_AGG1] Computing 30-day average open/close metrics per symbol.")
            recent_stock_df = valid_stock_df.filter(
                col("day_date") >= date_sub(current_date(), 30)
            )

            thirty_day_avg_df = recent_stock_df.groupBy("symbol").agg(
                round(F.avg("close"), 2).alias("thirty_day_avg_close"),
                round(F.avg("open"), 2).alias("thirty_day_avg_open"),
            )

            valid_stock_df = valid_stock_df.join(
                thirty_day_avg_df, on="symbol", how="left",
            )

            # 12. Rolling Aggregations: 52-Week High & Low
            logger.info("[TRANSFORM][DAILY_AGG2] Computing 52-week high/low metrics per symbol.")
            fifty_two_week_df = valid_stock_df.filter(
                col("day_date") >= date_sub(current_date(), 365)
            )

            fifty_two_week_agg = fifty_two_week_df.groupBy("symbol").agg(
                round(F.max("high"), 2).alias("fifty_two_week_high"),
                round(F.min("low"), 2).alias("fifty_two_week_low"),
            )

            valid_stock_df = valid_stock_df.join(
                fifty_two_week_agg, on="symbol", how="left",
            )

            # 13. Rolling Aggregations: All-Time High & Low
            logger.info("[TRANSFORM][DAILY_AGG3] Computing all-time high/low metrics per symbol.")
            all_time_agg = valid_stock_df.groupBy("symbol").agg(
                F.round(F.max("high"), 2).alias("all_time_high"),
                F.round(F.min("low"), 2).alias("all_time_low"),
            )

            valid_stock_df = valid_stock_df.join(
                all_time_agg, on="symbol", how="left",
            )

            # 14. Partitioning Keys: year, month, day
            valid_stock_df = (
                valid_stock_df
                .withColumn("year", year(col("day_date")))
                .withColumn("month", month(col("day_date")))
                .withColumn("day", dayofmonth(col("day_date")))
            )

            # 15. Final Silver Column Selection & Sorting
            valid_stock_df = valid_stock_df.select(
                "symbol", "day_date",
                "open", "high", "low", "close", "volume",
                "daily_change", "daily_change_percentage", "market_movement",
                "thirty_day_avg_open", "thirty_day_avg_close",
                "fifty_two_week_high", "fifty_two_week_low",
                "all_time_high", "all_time_low",
                "last_refreshed_date",
                "validation_status", "validation_reason",
                "year", "month", "day",
                "processed_at",
            )

            valid_stock_df = valid_stock_df.orderBy(
                col("symbol").asc(),
                col("day_date").desc(),
            )

            if debug:
                logger.debug("[TRANSFORM][DAILY] Final Silver DataFrame Schema:")
                valid_stock_df.printSchema()
                logger.debug("[TRANSFORM][DAILY] Sample Silver DataFrame Rows:")
                valid_stock_df.show(10, truncate=False)

            logger.info("[TRANSFORM][DAILY_OK] Daily time-series transformation completed successfully.")
            return valid_stock_df

        except Exception as e:
            logger.exception("[TRANSFORM][DAILY_FAIL] Error transforming daily data: %s", e)
            raise

    # ============================================================
    # COMPANY OVERVIEW TRANSFORMATION
    # ============================================================

    # 52 Business columns to select from raw Alpha Vantage Overview API
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

    # PascalCase to snake_case column mapping dictionary
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

    def silver_transform_overview(
        self,
        data_df: DataFrame,
    ) -> DataFrame:
        """
        Transform Alpha Vantage Company Overview Bronze data into a cleaned, typed Silver DataFrame.

        Execution Steps:
          1. Select 52 business columns.
          2. Trim string columns & upper-case symbol.
          3. Normalize fake nulls ("n/a", "-", "null" -> NULL).
          4. Impute missing defaults (Country="Unknown", Sector="Unknown").
          5. Type cast fields to IntegerType, LongType, DoubleType, DateType.
          6. Generate partition metadata (year, month, day).
          7. Rename column names to snake_case.
          8. Add processed_at timestamp.

        Args:
            data_df: Raw Bronze overview PySpark DataFrame.

        Returns:
            DataFrame: Transformed Silver overview DataFrame.
        """
        logger.info("[TRANSFORM][OVERVIEW_START] Starting company overview transformation.")

        try:
            # 1. Select business columns
            data_df = data_df.select(*self.OVERVIEW_BUSINESS_COLUMNS)

            # 2. String Cleaning: Trim strings & uppercase symbol
            string_columns = [
                "Symbol", "AssetType", "Name", "Exchange", "Currency",
                "Country", "Sector", "Industry", "OfficialSite", "FiscalYearEnd",
            ]

            for col_name in string_columns:
                data_df = data_df.withColumn(col_name, trim(col(col_name)))
                data_df = data_df.withColumn(col_name, col(col_name).cast(StringType()))

            data_df = data_df.withColumn("Symbol", upper(col("Symbol")))

            # 3. Normalize Fake Nulls
            fake_null_values = ["", "n/a", "na", "null", "none", "-"]

            for col_name in self.OVERVIEW_BUSINESS_COLUMNS:
                data_df = data_df.withColumn(
                    col_name,
                    when(
                        lower(col(col_name)).isin(fake_null_values),
                        None,
                    ).otherwise(col(col_name)),
                )

            # 4. Impute Missing Defaults
            data_df = data_df.fillna({
                "Country": "Unknown",
                "Sector": "Unknown",
                "Industry": "Unknown",
                "OfficialSite": "Not Available",
            })

            # 5. Explicit Data Type Casting
            integer_columns = [
                "CIK", "AnalystRatingStrongBuy", "AnalystRatingBuy",
                "AnalystRatingHold", "AnalystRatingSell", "AnalystRatingStrongSell",
            ]

            long_columns = [
                "MarketCapitalization", "EBITDA", "RevenueTTM",
                "GrossProfitTTM", "SharesOutstanding", "SharesFloat",
            ]

            double_columns = [
                "PERatio", "PEGRatio", "BookValue", "DividendPerShare",
                "DividendYield", "EPS", "RevenuePerShareTTM", "ProfitMargin",
                "OperatingMarginTTM", "ReturnOnAssetsTTM", "ReturnOnEquityTTM",
                "DilutedEPSTTM", "QuarterlyEarningsGrowthYOY",
                "QuarterlyRevenueGrowthYOY", "AnalystTargetPrice",
                "TrailingPE", "ForwardPE", "PriceToSalesRatioTTM",
                "PriceToBookRatio", "EVToRevenue", "EVToEBITDA", "Beta",
                "52WeekHigh", "52WeekLow", "50DayMovingAverage",
                "200DayMovingAverage", "PercentInsiders", "PercentInstitutions",
            ]

            date_columns = ["LatestQuarter", "DividendDate", "ExDividendDate"]

            for c in integer_columns:
                data_df = data_df.withColumn(c, col(c).cast(IntegerType()))

            for c in long_columns:
                data_df = data_df.withColumn(c, col(c).cast(LongType()))

            for c in double_columns:
                data_df = data_df.withColumn(c, col(c).cast(DoubleType()))

            for c in date_columns:
                data_df = data_df.withColumn(c, to_date(col(c), "yyyy-MM-dd"))

            # 6. Partitioning Metadata
            data_df = (
                data_df
                .withColumn("year", year(current_date()))
                .withColumn("month", month(current_date()))
                .withColumn("day", dayofmonth(current_date()))
            )

            # 7. Rename to snake_case column names
            for old_name, new_name in self.OVERVIEW_COLUMN_MAPPING.items():
                if old_name in data_df.columns:
                    data_df = data_df.withColumnRenamed(old_name, new_name)

            # 8. Add Processed Timestamp
            data_df = data_df.withColumn(
                "processed_at",
                from_utc_timestamp(current_timestamp(), "Asia/Kolkata"),
            )

            logger.info("[TRANSFORM][OVERVIEW_OK] Company overview transformation completed successfully.")
            return data_df

        except Exception as e:
            logger.exception("[TRANSFORM][OVERVIEW_FAIL] Error transforming overview data: %s", e)
            raise