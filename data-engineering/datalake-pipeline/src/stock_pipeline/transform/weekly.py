"""
Weekly Time Series — Bronze-to-Silver Transformation Module.

Transforms Alpha Vantage Weekly Time Series Bronze data (JSON format)
into a cleaned, validated, enriched Silver-ready DataFrame stored in Parquet/CSV.

Weekly Time Series — Bronze-to-Silver Transformation Module.

Transforms raw Bronze Alpha Vantage Weekly Time Series data from Bronze JSON
into a clean, typed, validated, and enriched Silver-ready PySpark DataFrame.

Transformation Flow:
    1. Flatten JSON map
    2. Select + alias OHLCV fields
    3. Trim whitespace & uppercase symbol
    4. Normalize fake-null tokens
    5. Cast strings to explicit data types
    6. Apply data quality validation assertions
    7. Generate validation_reason
    8. Filter valid records for Silver layer
    9. Deduplicate on business key
    10. Round numeric price fields
    11. Metric enrichment & partition metadata
    12. Final selection & ordering
    13. Row count metrics
    14. Return Silver DataFrame

Dataset:
  - `weekly_time_series`

Fields Processed:

  Source JSON Structure:
    {
        "Meta Data": {
            "2. Symbol": "IBM",
            "3. Last Refreshed": "2026-08-14"
        },
        "Weekly Time Series": {
            "2026-08-14": {
                "1. open": "192.50",
                "2. high": "195.20",
                "3. low": "191.80",
                "4. close": "194.10",
                "5. volume": "5250000"
            }
        }
    }

  root
    |-- Meta Data: struct (nullable = true)
    |    |-- 2. Symbol: string (nullable = true)
    |    |-- 3. Last Refreshed: string (nullable = true)
    |-- Weekly Time Series: map (nullable = true)
    |    |-- key: string
    |    |-- value: struct
    |    |    |-- 1. open: string (nullable = true)
    |    |    |-- 2. high: string (nullable = true)
    |    |    |-- 3. low: string (nullable = true)
    |    |    |-- 4. close: string (nullable = true)
    |    |    |-- 5. volume: string (nullable = true)

  Silver Output Schema:
    symbol                    STRING
    week_date                 DATE
    open                      DOUBLE
    high                      DOUBLE
    low                       DOUBLE
    close                     DOUBLE
    volume                    LONG
    weekly_change             DOUBLE
    weekly_change_percentage  DOUBLE
    market_movement           STRING
    year                      INTEGER
    month                     INTEGER
    week_number               INTEGER
    last_refreshed_date       DATE
    validation_status         STRING
    validation_reason         STRING
    processed_at              TIMESTAMP

    +------+----------+------+------+------+------+-------+-------------+------------------------+---------------+-----------------+
    |symbol| week_date|  open|  high|   low| close| volume|weekly_change|weekly_change_percentage|market_movement|validation_status|
    +------+----------+------+------+------+------+-------+-------------+------------------------+---------------+-----------------+
    |   IBM|2026-08-14|190.50|195.20|189.80|194.10|5250000|         3.60|                    1.89|           Bull|            VALID|
    |  AAPL|2026-08-14|172.30|178.90|171.50|177.20|9100000|         4.90|                    2.84|           Bull|            VALID|
    +------+----------+------+------+------+------+-------+-------------+------------------------+---------------+-----------------+

"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    explode,
    from_utc_timestamp,
    lower,
    month,
    round,
    to_date,
    trim,
    upper,
    weekofyear,
    when,
    year,
)

logger = logging.getLogger(__name__)


def transform_weekly_timeseries(
    spark: SparkSession,
    data_df: DataFrame,
    debug: bool = False,
) -> DataFrame:
    """
    Transform Alpha Vantage Weekly Time Series Bronze data into a cleaned Silver DataFrame.
    """

    # ============================================================
    # STEP 1: START WEEKLY TIME SERIES TRANSFORMATION
    # ============================================================

    print("\n" + "=" * 80)
    print("STEP 1: WEEKLY TIME SERIES — BRONZE → SILVER")
    print("=" * 80)

    logger.info(
        "[ALPHAVANTAGE][WEEKLY] Starting Bronze-to-Silver "
        "transformation."
    )

    try:

        # ========================================================
        # STEP 2: FLATTEN JSON MAP
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 2: FLATTEN JSON MAP")
        print("=" * 80)

        raw_df = data_df.select(
            col("`Meta Data`.`2. Symbol`").alias("symbol"),
            col("`Meta Data`.`3. Last Refreshed`").alias("last_refreshed"),
            explode(col("`Weekly Time Series`")).alias("week_date", "weekly_data"),
        )

        logger.info(
            "[ALPHAVANTAGE][WEEKLY] Nested JSON map exploded into "
            "weekly tabular rows."
        )

        # ========================================================
        # STEP 3: SELECT + ALIAS OHLCV FIELDS
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 3: SELECT + ALIAS OHLCV FIELDS")
        print("=" * 80)

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

        logger.info(
            "[ALPHAVANTAGE][WEEKLY] OHLCV fields selected and aliased."
        )

        # ========================================================
        # STEP 4: TRIM WHITESPACE & UPPERCASE SYMBOL
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 4: TRIM WHITESPACE & UPPERCASE SYMBOL")
        print("=" * 80)

        string_columns = [
            "symbol",
            "last_refreshed",
            "week_date",
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
            "[ALPHAVANTAGE][WEEKLY] Whitespace trimming completed."
        )

        # ========================================================
        # STEP 5: NORMALIZE FAKE NULL VALUES
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 5: NORMALIZE FAKE NULL VALUES")
        print("=" * 80)

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
            "[ALPHAVANTAGE][WEEKLY] Fake-null normalization completed."
        )

        # ========================================================
        # STEP 6: DATA TYPE CONVERSIONS
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 6: EXPLICIT DATA TYPE CASTING")
        print("=" * 80)

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

        logger.info(
            "[ALPHAVANTAGE][WEEKLY] Type casting completed."
        )

        # ========================================================
        # STEP 7: VALIDATION STATUS
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 7: APPLY DATA QUALITY VALIDATION")
        print("=" * 80)

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

        logger.info(
            "[ALPHAVANTAGE][WEEKLY] Validation status generated."
        )

        # ========================================================
        # STEP 8: VALIDATION REASON
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 8: GENERATE VALIDATION REASONS")
        print("=" * 80)

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

        logger.info(
            "[ALPHAVANTAGE][WEEKLY] Validation reasons generated."
        )

        # ========================================================
        # STEP 9: FILTER VALID ROWS FOR SILVER LAYER
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 9: FILTER VALID ROWS FOR SILVER LAYER")
        print("=" * 80)

        valid_stock_df = stock_df.filter(col("validation_status") == "VALID")

        invalid_record_count = stock_df.filter(col("validation_status") == "INVALID").count()

        logger.info(
            "[ALPHAVANTAGE][WEEKLY] Valid rows filtered | "
            "quarantined_invalid_records=%d",
            invalid_record_count,
        )

        # ========================================================
        # STEP 10: DEDUPLICATE
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 10: DEDUPLICATE WEEKLY TIME SERIES")
        print("=" * 80)

        valid_stock_df = valid_stock_df.dropDuplicates(["symbol", "week_date"])

        logger.info(
            "[ALPHAVANTAGE][WEEKLY] Deduplication completed | "
            "business_key=symbol+week_date"
        )

        # ========================================================
        # STEP 11: ROUND NUMERIC PRICES
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 11: ROUND NUMERIC PRICES")
        print("=" * 80)

        valid_stock_df = (
            valid_stock_df
            .withColumn("open", round(col("open"), 2))
            .withColumn("high", round(col("high"), 2))
            .withColumn("low", round(col("low"), 2))
            .withColumn("close", round(col("close"), 2))
        )

        logger.info(
            "[ALPHAVANTAGE][WEEKLY] Financial prices rounded to "
            "2 decimal places."
        )

        # ========================================================
        # STEP 12: METRIC ENRICHMENT & PARTITION METADATA
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 12: METRIC ENRICHMENT & PARTITION METADATA")
        print("=" * 80)

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

        logger.info(
            "[ALPHAVANTAGE][WEEKLY] Metric enrichment and partition "
            "keys added."
        )

        # ========================================================
        # STEP 13: FINAL SELECTION & ORDERING
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 13: FINAL SELECTION & ORDERING")
        print("=" * 80)

        valid_stock_df = valid_stock_df.select(
            "symbol",
            "week_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "weekly_change",
            "weekly_change_percentage",
            "market_movement",
            "year",
            "month",
            "week_number",
            "last_refreshed_date",
            "validation_status",
            "validation_reason",
            "processed_at",
        )

        valid_stock_df = valid_stock_df.orderBy(
            col("symbol").asc(),
            col("week_date").desc(),
        )

        logger.info(
            "[ALPHAVANTAGE][WEEKLY] Final schema selected and sorted | "
            "columns=%s",
            valid_stock_df.columns,
        )

        if debug:
            logger.debug("[ALPHAVANTAGE][WEEKLY] Schema:")
            valid_stock_df.printSchema()
            logger.debug("[ALPHAVANTAGE][WEEKLY] Sample data:")
            valid_stock_df.show(10, truncate=False)

        # ========================================================
        # STEP 14: ROW COUNT METRICS
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 14: WEEKLY TIME SERIES ROW COUNT METRICS")
        print("=" * 80)

        total_in = data_df.count()
        total_out = valid_stock_df.count()

        valid_count = valid_stock_df.filter(
            col("validation_status") == "VALID"
        ).count()

        logger.info(
            "[ALPHAVANTAGE][WEEKLY][METRICS] "
            "total_in=%d | total_out=%d | valid=%d | invalid=%d",
            total_in,
            total_out,
            valid_count,
            invalid_record_count,
        )

        print(f"Total In      : {total_in}")
        print(f"Total Out     : {total_out}")
        print(f"Valid Records : {valid_count}")
        print(f"Invalid       : {invalid_record_count}")

        # ========================================================
        # STEP 15: RETURN SILVER DATAFRAME
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 15: WEEKLY TIME SERIES TRANSFORMATION COMPLETED")
        print("=" * 80)

        logger.info(
            "[ALPHAVANTAGE][WEEKLY] Bronze-to-Silver transformation "
            "completed successfully."
        )

        return valid_stock_df

    except Exception as e:

        print("\n" + "!" * 80)
        print("WEEKLY TIME SERIES TRANSFORMATION FAILED")
        print("!" * 80)

        logger.exception(
            "[ALPHAVANTAGE][WEEKLY] Error transforming weekly "
            "data: %s", e
        )

        raise
