"""
Transform stage — Bronze-to-Silver data transformations.

Each transform method receives a raw Bronze DataFrame and returns a
cleaned, validated, enriched Silver-ready DataFrame.
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
    Transforms raw Bronze stock data into clean Silver-layer DataFrames.

    Responsibilities:
        - Flatten nested JSON structures.
        - Trim, normalize, and cast columns.
        - Validate data quality (flag VALID / INVALID rows).
        - Enrich with derived metrics (change %, rolling averages, etc.).
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # ============================================================
    # WEEKLY TIME SERIES (kept for future use)
    # ============================================================

    def transform_weekly_timeseries(
        self,
        data_df: DataFrame,
        debug: bool = False,
    ) -> DataFrame:
        """
        Transform Alpha Vantage Weekly Time Series Bronze data
        into a cleaned, validated, enriched Silver-ready DataFrame.
        """
        logger.info("[TRANSFORM][WEEKLY_START] Starting weekly time-series transformation.")

        try:
            # 1. FLATTEN
            raw_df = data_df.select(
                col("`Meta Data`.`2. Symbol`").alias("symbol"),
                col("`Meta Data`.`3. Last Refreshed`").alias("last_refreshed"),
                explode(col("`Weekly Time Series`")).alias("week_date", "weekly_data"),
            )

            # 2. SELECT COLUMNS
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

            # 3. DATA CLEANING
            string_columns = [
                "symbol", "last_refreshed", "week_date",
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

            # 4. TYPE CONVERSION
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

            # 5. DATA QUALITY VALIDATION
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

            # 6. VALIDATION REASON
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

            # 7. FILTER VALID RECORDS
            valid_stock_df = stock_df.filter(col("validation_status") == "VALID")

            invalid_record_count = stock_df.filter(col("validation_status") == "INVALID").count()
            logger.info("[TRANSFORM][WEEKLY] Found %d invalid records.", invalid_record_count)

            # 8. DEDUPLICATE
            valid_stock_df = valid_stock_df.dropDuplicates(["symbol", "week_date"])

            # 9. ROUND PRICE COLUMNS
            valid_stock_df = (
                valid_stock_df
                .withColumn("open", round(col("open"), 2))
                .withColumn("high", round(col("high"), 2))
                .withColumn("low", round(col("low"), 2))
                .withColumn("close", round(col("close"), 2))
            )

            # 10. ENRICHMENT
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

            # 11. FINAL COLUMN SELECTION
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
    # DAILY TIME SERIES
    # ============================================================

    def silver_transform_daily_timeseries(
        self,
        daily_dataset: str,
        data_df: DataFrame,
        watermark_value: str | None = None,
        debug: bool = False,
    ) -> DataFrame:
        """
        Transform Alpha Vantage Daily Time Series Bronze data
        into a cleaned, validated, enriched Silver-ready DataFrame.

        Args:
            daily_dataset: Dataset name (used for logging).
            data_df: Raw Bronze DataFrame.
            watermark_value: If provided, only rows with day_date > this value
                are included (incremental load). None means full load.
            debug: If True, print schema and sample rows.

        Returns:
            Silver-ready DataFrame (may be empty if no new records).
        """
        logger.info("[TRANSFORM][DAILY_START] Starting daily time-series transformation.")

        try:
            # 1. FLATTEN
            logger.info("[TRANSFORM][DAILY] Flattening daily time-series.")

            raw_df = data_df.select(
                col("`Meta Data`.`2. Symbol`").alias("symbol"),
                col("`Meta Data`.`3. Last Refreshed`").alias("last_refreshed"),
                explode(col("Time Series (Daily)")).alias("day_date", "daily_data"),
            )

            # 2. SELECT COLUMNS
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

            # WATERMARK FILTER (incremental load)
            if watermark_value is not None:
                logger.info(
                    "[TRANSFORM][DAILY] Applying watermark filter: day_date > '%s'.",
                    watermark_value,
                )
                stock_df = stock_df.filter(col("day_date") > watermark_value)
            else:
                logger.info("[TRANSFORM][DAILY] No watermark — performing full load.")

            # 3. DATA CLEANING
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

            # 4. TYPE CONVERSION
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

            # 5. DATA QUALITY VALIDATION
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

            # 6. VALIDATION REASON
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

            # 7. FILTER VALID RECORDS
            valid_stock_df = stock_df.filter(col("validation_status") == "VALID")

            invalid_record_count = stock_df.filter(col("validation_status") == "INVALID").count()
            logger.info("[TRANSFORM][DAILY] Found %d invalid records.", invalid_record_count)

            # 8. DEDUPLICATE
            valid_stock_df = valid_stock_df.dropDuplicates(["symbol", "day_date"])

            # 9. ROUND PRICE COLUMNS
            valid_stock_df = (
                valid_stock_df
                .withColumn("open", round(col("open"), 2))
                .withColumn("high", round(col("high"), 2))
                .withColumn("low", round(col("low"), 2))
                .withColumn("close", round(col("close"), 2))
            )

            # 10. ENRICHMENT
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

            # 11. 30-DAY ROLLING AVERAGES
            logger.info("[TRANSFORM][DAILY] Computing 30-day average open/close per symbol.")

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

            # 12. 52-WEEK HIGH / LOW
            logger.info("[TRANSFORM][DAILY] Computing 52-week high/low per symbol.")

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

            # 13. ALL-TIME HIGH / LOW
            all_time_agg = valid_stock_df.groupBy("symbol").agg(
                F.round(F.max("high"), 2).alias("all_time_high"),
                F.round(F.min("low"), 2).alias("all_time_low"),
            )

            valid_stock_df = valid_stock_df.join(
                all_time_agg, on="symbol", how="left",
            )

            # 14. DATE PARTITION METADATA
            valid_stock_df = (
                valid_stock_df
                .withColumn("year", year(col("day_date")))
                .withColumn("month", month(col("day_date")))
                .withColumn("day", dayofmonth(col("day_date")))
            )

            # 15. FINAL COLUMN SELECTION & ORDERING
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
                logger.debug("[TRANSFORM][DAILY] Final schema:")
                valid_stock_df.printSchema()
                logger.debug("[TRANSFORM][DAILY] Sample data:")
                valid_stock_df.show(10, truncate=False)

            logger.info("[TRANSFORM][DAILY_OK] Daily time-series transformation completed.")
            return valid_stock_df

        except Exception as e:
            logger.exception("[TRANSFORM][DAILY_FAIL] Error transforming daily data: %s", e)
            raise

    # ============================================================
    # COMPANY OVERVIEW
    # ============================================================

    # Columns to select from the raw Overview API response.
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

    # Snake_case mapping for Silver-layer column names.
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
        Transform Alpha Vantage Company Overview Bronze data into a
        cleaned, typed, Silver-ready DataFrame.

        Args:
            data_df: Raw Bronze overview DataFrame.

        Returns:
            Silver-ready overview DataFrame.
        """
        logger.info("[TRANSFORM][OVERVIEW_START] Starting company overview transformation.")

        try:
            # 1. SELECT BUSINESS COLUMNS
            data_df = data_df.select(*self.OVERVIEW_BUSINESS_COLUMNS)

            # 2. DATA CLEANING — trim strings
            string_columns = [
                "Symbol", "AssetType", "Name", "Exchange", "Currency",
                "Country", "Sector", "Industry", "OfficialSite", "FiscalYearEnd",
            ]

            for col_name in string_columns:
                data_df = data_df.withColumn(col_name, trim(col(col_name)))
                data_df = data_df.withColumn(col_name, col(col_name).cast(StringType()))

            data_df = data_df.withColumn("Symbol", upper(col("Symbol")))

            # 3. NORMALIZE FAKE NULLS
            fake_null_values = ["", "n/a", "na", "null", "none", "-"]

            columns_to_clean = self.OVERVIEW_BUSINESS_COLUMNS

            for col_name in columns_to_clean:
                data_df = data_df.withColumn(
                    col_name,
                    when(
                        lower(col(col_name)).isin(fake_null_values),
                        None,
                    ).otherwise(col(col_name)),
                )

            # 4. FILL DEFAULTS
            data_df = data_df.fillna({
                "Country": "Unknown",
                "Sector": "Unknown",
                "Industry": "Unknown",
                "OfficialSite": "Not Available",
            })

            # 5. TYPE CASTING
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

            # 6. DATE PARTITION METADATA
            data_df = (
                data_df
                .withColumn("year", year(current_date()))
                .withColumn("month", month(current_date()))
                .withColumn("day", dayofmonth(current_date()))
            )

            # 7. RENAME TO SNAKE_CASE
            for old_name, new_name in self.OVERVIEW_COLUMN_MAPPING.items():
                if old_name in data_df.columns:
                    data_df = data_df.withColumnRenamed(old_name, new_name)

            # 8. ADD PROCESSED TIMESTAMP
            data_df = data_df.withColumn(
                "processed_at",
                from_utc_timestamp(current_timestamp(), "Asia/Kolkata"),
            )

            logger.info("[TRANSFORM][OVERVIEW_OK] Company overview transformation completed.")
            return data_df

        except Exception as e:
            logger.exception("[TRANSFORM][OVERVIEW_FAIL] Error transforming overview data: %s", e)
            raise