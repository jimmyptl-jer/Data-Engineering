"""
Weekly Time Series — Bronze-to-Silver Transformation Module.

Transforms Alpha Vantage Weekly Time Series Bronze data (JSON format)
into a cleaned, validated, enriched Silver-ready DataFrame stored in Parquet/CSV.

Transformation Pipeline Steps:
  1. Flatten JSON Map   ──► Explode `Weekly Time Series` struct map into weekly rows.
  2. Column Aliasing    ──► Select open, high, low, close, volume fields.
  3. String Clean & Case ──► Trim whitespace and convert symbol to uppercase.
  4. Fake Null Mapping  ──► Convert empty text strings ('n/a', 'none', '-') to NULL.
  5. Type Casting       ──► Cast strings to DateType, DoubleType, and LongType.
  6. Data Quality Audit ──► Mark rows VALID/INVALID based on price/volume threshold assertions.
  7. Filter Valid Rows  ──► Retain valid records for Silver storage and audit quarantined rows.
  8. Deduplication      ──► Remove duplicates on (symbol, week_date).
  9. Rounding           ──► Round financial fields to 2 decimal places.
 10. Metric Enrichment  ──► Compute `weekly_change`, `weekly_change_percentage`, and `market_movement`.
 11. Partition Keys     ──► Extract `year`, `month`, `week_number` partition columns.
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

    Args:
        spark (SparkSession): Active SparkSession instance.
        data_df (DataFrame): Raw Bronze DataFrame matching `stock_schema_weekly`.
        debug (bool): If True, logs schema and prints top 10 sample rows.

    Returns:
        DataFrame: Transformed Silver weekly DataFrame.
    """
    logger.info("[TRANSFORM][WEEKLY_START] Starting weekly time-series transformation.")

    try:
        # ==============================================================================
        # Step 1: Flatten nested JSON structure
        # ==============================================================================
        raw_df = data_df.select(
            col("`Meta Data`.`2. Symbol`").alias("symbol"),
            col("`Meta Data`.`3. Last Refreshed`").alias("last_refreshed"),
            explode(col("`Weekly Time Series`")).alias("week_date", "weekly_data"),
        )

        # ==============================================================================
        # Step 2: Select and alias OHLCV fields
        # ==============================================================================
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

        # ==============================================================================
        # Step 3: Trim whitespace, uppercase symbol & normalize fake nulls
        # ==============================================================================
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
        # Step 4: Cast strings to explicit data types
        # ==============================================================================
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

        # ==============================================================================
        # Step 5: Data Quality Validation Assertions
        # ==============================================================================
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

        # Step 6: Detailed audit error messages
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

        # ==============================================================================
        # Step 7: Filter VALID Records for Silver Layer
        # ==============================================================================
        valid_stock_df = stock_df.filter(col("validation_status") == "VALID")

        invalid_record_count = stock_df.filter(col("validation_status") == "INVALID").count()
        logger.info("[TRANSFORM][WEEKLY] Quarantined invalid records count: %d.", invalid_record_count)

        # ==============================================================================
        # Step 8: Deduplicate on (symbol, week_date)
        # ==============================================================================
        valid_stock_df = valid_stock_df.dropDuplicates(["symbol", "week_date"])

        # ==============================================================================
        # Step 9: Round numeric price fields
        # ==============================================================================
        valid_stock_df = (
            valid_stock_df
            .withColumn("open", round(col("open"), 2))
            .withColumn("high", round(col("high"), 2))
            .withColumn("low", round(col("low"), 2))
            .withColumn("close", round(col("close"), 2))
        )

        # ==============================================================================
        # Step 10: Metric Enrichment & Partition Metadata
        # ==============================================================================
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

        # ==============================================================================
        # Step 11: Final Selection & Ordering
        # ==============================================================================
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

        if debug:
            logger.debug("[TRANSFORM][WEEKLY] Schema:")
            valid_stock_df.printSchema()
            logger.debug("[TRANSFORM][WEEKLY] Sample data:")
            valid_stock_df.show(10, truncate=False)

        logger.info("[TRANSFORM][WEEKLY_OK] Weekly time-series transformation completed successfully.")
        return valid_stock_df

    except Exception as e:
        logger.exception("[TRANSFORM][WEEKLY_FAIL] Error transforming weekly data: %s", e)
        raise
