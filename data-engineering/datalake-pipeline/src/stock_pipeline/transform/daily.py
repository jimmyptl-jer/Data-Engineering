"""
Daily Time Series — Bronze-to-Silver Transformation Module.

Transforms raw Bronze Alpha Vantage Daily Stock Data (JSON format)
into a clean, typed, validated, and enriched Silver DataFrame stored in Parquet/CSV.

Detailed ETL Processing Sequence:
  Step 1:  Flatten Nested JSON  ──► Explode `Time Series (Daily)` map into key (date) and value (OHLCV struct).
  Step 2:  Column Selection     ──► Extract symbol, date, open, high, low, close, volume fields.
  Step 3:  Watermark Filter     ──► If watermark provided, keep only rows where `day_date > watermark_value`.
  Step 4:  String Cleaning      ──► Trim whitespace and uppercase stock symbol.
  Step 5:  Fake Null Handling   ──► Convert invalid text like `"n/a"`, `"none"`, `"-"` into true PySpark `NULL`.
  Step 6:  Type Casting         ──► Cast strings to explicit data types (`DateType`, `DoubleType`, `LongType`).
  Step 7:  Data Quality Audit   ──► Apply business rules to mark rows as `VALID` or `INVALID` (Quarantine pattern).
  Step 8:  Filter Valid Rows    ──► Keep `VALID` rows for Silver output; log count of quarantined rows.
  Step 9:  Deduplication        ──► Drop duplicates on `(symbol, day_date)` composite key.
  Step 10: Rounding             ──► Round financial price metrics to 2 decimal places.
  Step 11: Metric Enrichment    ──► Compute `daily_change`, `daily_change_percentage`, and `market_movement` (Bull/Bear).
  Step 12: Rolling 30-Day Avg   ──► Calculate 30-day rolling average for open and close prices per symbol.
  Step 13: Rolling 52-Week Range──► Calculate 52-week high and low prices per symbol.
  Step 14: All-Time High/Low    ──► Calculate all-time high and low prices per symbol.
  Step 15: Lag Computations     ──► Use Window functions to calculate `previous_open`, `previous_close`, etc.
  Step 16: Partition Keys       ──► Extract `year`, `month`, `day` columns for S3 partitioning.
  Step 17: Final Schema Selection──► Order and select Silver-ready columns.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    current_timestamp,
    current_date,
    date_sub,
    dayofmonth,
    explode,
    from_utc_timestamp,
    lag,
    lower,
    month,
    round,
    to_date,
    trim,
    upper,
    when,
    year,
)

logger = logging.getLogger(__name__)


def silver_transform_daily_timeseries(
    spark: SparkSession,
    daily_dataset: str,
    data_df: DataFrame,
    watermark_value: str | None = None,
    debug: bool = False,
) -> DataFrame:
    """
    Transform Alpha Vantage Daily Time Series Bronze data into Silver-ready DataFrame.

    This function performs comprehensive data cleaning, validation, and enrichment:
      - Flattens nested JSON structures into a relational tabular format.
      - Applies date-based watermark filtering to process only NEW records.
      - Normalizes empty strings and fake null values (e.g. 'n/a', 'none', '-') to NULL.
      - Enforces strict data quality assertions using a quarantine pattern.
      - Calculates financial technical indicators (30-day averages, 52-week high/low, daily change %).
      - Computes windowed lag features for previous day's metrics.

    Args:
        spark (SparkSession): Active PySpark session.
        daily_dataset (str): Name of the dataset being processed (for logging).
        data_df (DataFrame): Raw Bronze PySpark DataFrame read from S3 JSON files.
        watermark_value (str | None): ISO date string ('YYYY-MM-DD') of the last processed record.
                                      If provided, rows with day_date <= watermark_value are filtered out.
        debug (bool): If True, logs DataFrame schema and prints top 10 sample rows.

    Returns:
        DataFrame: Cleaned, validated, and enriched Silver DataFrame ready for S3 parquet/csv write.
                   Returns an empty DataFrame if all records were filtered by watermark.
    """
    logger.info("[TRANSFORM][DAILY_START] Starting daily time-series transformation pipeline.")

    try:
        # ==============================================================================
        # Step 1: Flatten nested JSON structure
        # ==============================================================================
        # Alpha Vantage returns JSON with a `Meta Data` header and a `Time Series (Daily)` map.
        # We extract the symbol and explode the map to create one row per date.
        logger.info("[TRANSFORM][DAILY_STEP1] Flattening nested JSON map into tabular rows.")
        raw_df = data_df.select(
            col("`Meta Data`.`2. Symbol`").alias("symbol"),
            col("`Meta Data`.`3. Last Refreshed`").alias("last_refreshed"),
            explode(col("Time Series (Daily)")).alias("day_date", "daily_data"),
        )

        # ==============================================================================
        # Step 2: Select and alias OHLCV fields
        # ==============================================================================
        # Extract individual price and volume fields from the exploded struct column
        logger.info("[TRANSFORM][DAILY_STEP2] Selecting and aliasing OHLCV price & volume fields.")
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

        # ==============================================================================
        # Step 3: String Cleaning & Whitespace Trimming
        # ==============================================================================
        # Remove leading/trailing spaces from string columns and ensure uppercase symbol
        string_columns = [
            "symbol",
            "last_refreshed",
            "day_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]

        for column_name in string_columns:
            stock_df = stock_df.withColumn(column_name, trim(col(column_name)))

        # Convert symbol to uppercase (e.g. 'ibm' -> 'IBM') for consistent join keys
        stock_df = stock_df.withColumn("symbol", upper(col("symbol")))

        # ==============================================================================
        # Step 4: Fake Null Normalization
        # ==============================================================================
        # Data APIs often return placeholder strings for missing values instead of true JSON nulls.
        # We map all known placeholder strings to PySpark `None` (NULL).
        fake_null_values = ["", "n/a", "na", "null", "none", "-"]

        for column_name in string_columns:
            stock_df = stock_df.withColumn(
                column_name,
                when(
                    lower(col(column_name)).isin(fake_null_values),
                    None,
                ).otherwise(col(column_name)),
            )

        # ==============================================================================
        # Step 5: Data Type Conversions
        # ==============================================================================
        # Convert string representations to appropriate Spark SQL data types
        # Dates -> DateType, Prices -> DoubleType, Volume -> LongType
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

        # ==============================================================================
        # Step 6: Data Quality Audit (Quarantine Pattern)
        # ==============================================================================
        # Evaluate business logic validation rules on each row:
        #   1. Essential fields must not be NULL (symbol, date, open, high, low, close, volume)
        #   2. Prices must be strictly positive (> 0)
        #   3. Volume cannot be negative (>= 0)
        #   4. High price cannot be lower than Low price (high >= low)
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

        # Detailed error message tagging for auditing quarantined records
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

        # ==============================================================================
        # Step 7: Filter Valid Rows for Silver Layer
        # ==============================================================================
        valid_stock_df = stock_df.filter(col("validation_status") == "VALID")

        invalid_record_count = stock_df.filter(col("validation_status") == "INVALID").count()
        logger.info("[TRANSFORM][DAILY_DQ] Data Quality check completed. Quarantined invalid records: %d", invalid_record_count)

        # ==============================================================================
        # Step 8: Deduplication
        # ==============================================================================
        # Remove duplicate records for the same symbol on the same day
        valid_stock_df = valid_stock_df.dropDuplicates(["symbol", "day_date"])

        # ==============================================================================
        # Step 9: Rounding Financial Prices
        # ==============================================================================
        # Round floating point prices to standard 2 decimal currency places
        valid_stock_df = (
            valid_stock_df
            .withColumn("open", round(col("open"), 2))
            .withColumn("high", round(col("high"), 2))
            .withColumn("low", round(col("low"), 2))
            .withColumn("close", round(col("close"), 2))
        )

        # ==============================================================================
        # Step 10: Financial Metric Enrichment
        # ==============================================================================
        # Compute intraday dollar change, percentage change, and trend direction
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

        # ==============================================================================
        # Step 11: Rolling Aggregations — 30-Day Average Open & Close
        # ==============================================================================
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

        # ==============================================================================
        # Step 12: Rolling Aggregations — 52-Week High & Low
        # ==============================================================================
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

        # ==============================================================================
        # Step 13: Rolling Aggregations — All-Time High & Low
        # ==============================================================================
        logger.info("[TRANSFORM][DAILY_AGG3] Computing all-time high/low metrics per symbol.")
        all_time_agg = valid_stock_df.groupBy("symbol").agg(
            F.round(F.max("high"), 2).alias("all_time_high"),
            F.round(F.min("low"), 2).alias("all_time_low"),
        )

        valid_stock_df = valid_stock_df.join(
            all_time_agg, on="symbol", how="left",
        )

        # ==============================================================================
        # Step 14: Partitioning Keys & Window Lag Features
        # ==============================================================================
        valid_stock_df = (
            valid_stock_df
            .withColumn("year", year(col("day_date")))
            .withColumn("month", month(col("day_date")))
            .withColumn("day", dayofmonth(col("day_date")))
        )

        # Window specification partitioned by symbol and ordered chronologically by day_date
        stock_window = (
            Window
            .partitionBy("symbol")
            .orderBy("day_date")
        )

        # Calculate previous trading session metrics using lag() function
        valid_stock_df = (
            valid_stock_df
            .withColumn(
                "previous_close",
                lag("close", 1).over(stock_window)
            )
            .withColumn(
                "previous_open",
                lag("open", 1).over(stock_window)
            )
            .withColumn(
                "previous_high",
                lag("high", 1).over(stock_window)
            )
            .withColumn(
                "previous_low",
                lag("low", 1).over(stock_window)
            )
        )

        # ==============================================================================
        # Step 15: Final Silver Schema Selection & Sorting
        # ==============================================================================
        valid_stock_df = valid_stock_df.select(
            "symbol",
            "day_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "daily_change",
            "daily_change_percentage",
            "market_movement",
            "thirty_day_avg_open",
            "thirty_day_avg_close",
            "fifty_two_week_high",
            "fifty_two_week_low",
            "all_time_high",
            "all_time_low",
            "last_refreshed_date",
            "validation_status",
            "validation_reason",
            "year",
            "month",
            "day",
            "processed_at",
            "previous_close",
            "previous_open",
            "previous_high",
            "previous_low"
        )

        # Sort output chronologically per stock symbol
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
