"""
Daily Time Series — Bronze-to-Silver Transformation Module.

Transforms raw Bronze Alpha Vantage Daily Stock Data (JSON format)
into a clean, typed, validated, and enriched Silver DataFrame stored in Parquet/CSV.

Daily Time Series — Bronze-to-Silver Transformation Module.

Transforms raw Bronze Alpha Vantage Daily Time Series data from Bronze JSON
into a clean, typed, validated, and enriched Silver-ready PySpark DataFrame.

Transformation Flow:
    1. Flatten nested JSON structure
    2. Select + alias OHLCV fields
    3. Trim whitespace
    4. Normalize fake-null tokens
    5. Data type conversions
    6. Apply validation rules
    7. Generate validation_reason
    8. Filter valid rows for Silver layer
    9. Deduplicate on business key
    10. Round financial prices
    11. Financial metric enrichment
    12. Rolling 30-day average open & close
    13. Rolling 52-week high & low
    14. All-time high & low
    15. Partition keys & lag features
    16. Final Silver schema selection & sorting
    17. Row count metrics
    18. Return Silver DataFrame

Dataset:
  - `daily_time_series`

Fields Processed:

  Source JSON Structure:
    {
        "Meta Data": {
            "2. Symbol": "IBM",
            "3. Last Refreshed": "2026-08-14"
        },
        "Time Series (Daily)": {
            "2026-08-14": {
                "1. open": "192.50",
                "2. high": "195.20",
                "3. low": "191.80",
                "4. close": "194.10",
                "5. volume": "1250000"
            }
        }
    }

  root
    |-- Meta Data: struct (nullable = true)
    |    |-- 2. Symbol: string (nullable = true)
    |    |-- 3. Last Refreshed: string (nullable = true)
    |-- Time Series (Daily): map (nullable = true)
    |    |-- key: string
    |    |-- value: struct
    |    |    |-- 1. open: string (nullable = true)
    |    |    |-- 2. high: string (nullable = true)
    |    |    |-- 3. low: string (nullable = true)
    |    |    |-- 4. close: string (nullable = true)
    |    |    |-- 5. volume: string (nullable = true)

  Silver Output Schema:
    symbol                    STRING
    day_date                  DATE
    open                      DOUBLE
    high                      DOUBLE
    low                       DOUBLE
    close                     DOUBLE
    volume                    LONG
    daily_change              DOUBLE
    daily_change_percentage   DOUBLE
    market_movement           STRING
    thirty_day_avg_open       DOUBLE
    thirty_day_avg_close      DOUBLE
    fifty_two_week_high       DOUBLE
    fifty_two_week_low        DOUBLE
    all_time_high             DOUBLE
    all_time_low              DOUBLE
    last_refreshed_date       DATE
    validation_status         STRING
    validation_reason         STRING
    year                      INTEGER
    month                     INTEGER
    day                       INTEGER
    processed_at              TIMESTAMP
    previous_close            DOUBLE
    previous_open             DOUBLE
    previous_high             DOUBLE
    previous_low              DOUBLE

    +------+----------+------+------+------+------+-------+------------+------------------------+---------------+-----------------+
    |symbol|  day_date|  open|  high|   low| close| volume|daily_change|daily_change_percentage|market_movement|validation_status|
    +------+----------+------+------+------+------+-------+------------+------------------------+---------------+-----------------+
    |   IBM|2026-08-14|192.50|195.20|191.80|194.10|1250000|        1.60|                    0.83|           Bull|            VALID|
    |   IBM|2026-08-13|191.00|193.50|190.20|192.50| 980000|        1.50|                    0.79|           Bull|            VALID|
    |  AAPL|2026-08-14|175.30|178.90|174.50|177.20|2100000|        1.90|                    1.08|           Bull|            VALID|
    +------+----------+------+------+------+------+-------+------------+------------------------+---------------+-----------------+

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
    """

    # ============================================================
    # STEP 1: START DAILY TIME SERIES TRANSFORMATION
    # ============================================================


    logger.info(
        "[ALPHAVANTAGE][DAILY] Starting Bronze-to-Silver "
        "transformation."
    )

    try:

        # ========================================================
        # STEP 2: FLATTEN NESTED JSON STRUCTURE
        # ========================================================


        raw_df = data_df.select(
            col("`Meta Data`.`2. Symbol`").alias("symbol"),
            col("`Meta Data`.`3. Last Refreshed`").alias("last_refreshed"),
            explode(col("Time Series (Daily)")).alias("day_date", "daily_data"),
        )

        logger.info(
            "[ALPHAVANTAGE][DAILY] Nested JSON flattened into "
            "tabular rows."
        )

        # ========================================================
        # STEP 3: SELECT + ALIAS OHLCV FIELDS
        # ========================================================


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

        logger.info(
            "[ALPHAVANTAGE][DAILY] OHLCV fields selected and "
            "aliased."
        )

        # ========================================================
        # STEP 4: TRIM WHITESPACE
        # ========================================================


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

        stock_df = stock_df.withColumn("symbol", upper(col("symbol")))

        logger.info(
            "[ALPHAVANTAGE][DAILY] Whitespace trimming completed."
        )

        # ========================================================
        # STEP 5: NORMALIZE FAKE NULL VALUES
        # ========================================================


        fake_null_values = ["", "n/a", "na", "null", "none", "-"]

        for column_name in string_columns:
            stock_df = stock_df.withColumn(
                column_name,
                when(
                    lower(col(column_name)).isin(fake_null_values),
                    None,
                ).otherwise(col(column_name)),
            )

        logger.info(
            "[ALPHAVANTAGE][DAILY] Fake-null normalization completed."
        )

        # ========================================================
        # STEP 6: DATA TYPE CONVERSIONS
        # ========================================================


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

        logger.info(
            "[ALPHAVANTAGE][DAILY] Type casting completed."
        )

        # ========================================================
        # STEP 7: VALIDATION STATUS
        # ========================================================


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

        logger.info(
            "[ALPHAVANTAGE][DAILY] Validation status generated."
        )

        # ========================================================
        # STEP 8: VALIDATION REASON
        # ========================================================


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

        logger.info(
            "[ALPHAVANTAGE][DAILY] Validation reasons generated."
        )

        # ========================================================
        # STEP 9: FILTER VALID ROWS FOR SILVER LAYER
        # ========================================================


        valid_stock_df = stock_df.filter(col("validation_status") == "VALID")

        invalid_record_count = stock_df.filter(col("validation_status") == "INVALID").count()

        logger.info(
            "[ALPHAVANTAGE][DAILY] Valid rows filtered | "
            "quarantined_invalid_records=%d",
            invalid_record_count,
        )

        # ========================================================
        # STEP 10: DEDUPLICATE
        # ========================================================


        valid_stock_df = valid_stock_df.dropDuplicates(["symbol", "day_date"])

        logger.info(
            "[ALPHAVANTAGE][DAILY] Deduplication completed | "
            "business_key=symbol+day_date"
        )

        # ========================================================
        # STEP 11: ROUND FINANCIAL PRICES
        # ========================================================


        valid_stock_df = (
            valid_stock_df
            .withColumn("open", round(col("open"), 2))
            .withColumn("high", round(col("high"), 2))
            .withColumn("low", round(col("low"), 2))
            .withColumn("close", round(col("close"), 2))
        )

        logger.info(
            "[ALPHAVANTAGE][DAILY] Financial prices rounded to "
            "2 decimal places."
        )

        # ========================================================
        # STEP 12: FINANCIAL METRIC ENRICHMENT
        # ========================================================


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

        logger.info(
            "[ALPHAVANTAGE][DAILY] Metric enrichment completed."
        )

        if debug:
            logger.debug("[ALPHAVANTAGE][DAILY] Valid data schema:")
            valid_stock_df.printSchema()

        # ========================================================
        # STEP 13: ROLLING 30-DAY AVERAGE OPEN & CLOSE
        # ========================================================


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

        logger.info(
            "[ALPHAVANTAGE][DAILY] 30-day rolling average computed."
        )

        # ========================================================
        # STEP 14: ROLLING 52-WEEK HIGH & LOW
        # ========================================================


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

        logger.info(
            "[ALPHAVANTAGE][DAILY] 52-week high/low computed."
        )

        # ========================================================
        # STEP 15: ALL-TIME HIGH & LOW
        # ========================================================


        all_time_agg = valid_stock_df.groupBy("symbol").agg(
            F.round(F.max("high"), 2).alias("all_time_high"),
            F.round(F.min("low"), 2).alias("all_time_low"),
        )

        valid_stock_df = valid_stock_df.join(
            all_time_agg, on="symbol", how="left",
        )

        logger.info(
            "[ALPHAVANTAGE][DAILY] All-time high/low computed."
        )

        # ========================================================
        # STEP 16: PARTITION KEYS & LAG FEATURES
        # ========================================================


        valid_stock_df = (
            valid_stock_df
            .withColumn("year", year(col("day_date")))
            .withColumn("month", month(col("day_date")))
            .withColumn("day", dayofmonth(col("day_date")))
        )

        stock_window = (
            Window
            .partitionBy("symbol")
            .orderBy("day_date")
        )

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

        logger.info(
            "[ALPHAVANTAGE][DAILY] Partition keys and lag features "
            "computed."
        )

        # ========================================================
        # STEP 17: FINAL SILVER SCHEMA SELECTION & SORTING
        # ========================================================


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

        valid_stock_df = valid_stock_df.orderBy(
            col("symbol").asc(),
            col("day_date").desc(),
        )

        logger.info(
            "[ALPHAVANTAGE][DAILY] Final schema selected and sorted | "
            "columns=%s",
            valid_stock_df.columns,
        )

        if debug:
            logger.debug("[ALPHAVANTAGE][DAILY] Final Silver DataFrame Schema:")
            valid_stock_df.printSchema()
            logger.debug("[ALPHAVANTAGE][DAILY] Sample Silver DataFrame Rows:")
            valid_stock_df.show(10, truncate=False)

        # ========================================================
        # STEP 18: ROW COUNT METRICS
        # ========================================================

        logger.info(
            "[ALPHAVANTAGE][DAILY][METRICS] "
            "total_in=%d | total_out=%d | valid=%d | invalid=%d",
            total_in,
            total_out,
            valid_count,
            invalid_record_count,
        )

        # ========================================================
        # STEP 19: RETURN SILVER DATAFRAME
        # ========================================================


        logger.info(
            "[ALPHAVANTAGE][DAILY] Bronze-to-Silver transformation "
            "completed successfully."
        )

        return valid_stock_df

    except Exception as e:

        logger.exception(
            "[ALPHAVANTAGE][DAILY] Error transforming daily "
            "time-series data: %s", e
        )

        raise
