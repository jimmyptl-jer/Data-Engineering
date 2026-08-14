"""
Massive Exchanges — Bronze-to-Silver Transformation Module.

Transforms Massive (Polygon.io) exchange reference data (JSON)
into a cleaned, validated Silver-ready PySpark DataFrame.

Fields Processed:
  - `asset_class`     (e.g., 'stocks', 'crypto')
  - `id`              (Unique exchange integer identifier)
  - `locale`          (e.g., 'us', 'global')
  - `name`            (e.g., 'New York Stock Exchange')
  - `operating_mic`   (Market Identifier Code, e.g., 'XNYS')
  - `participant_id`  (Participant identifier)
  - `type`            (Exchange type, e.g., 'EXCHANGE', 'TRF')
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    lower,
    trim,
    upper,
    when,
)
from pyspark.sql.types import (
    LongType,
    StringType,
)

logger = logging.getLogger(__name__)


def transform_massive_exchanges_dataset(
    spark: SparkSession,
    data_df: DataFrame,
) -> DataFrame:
    """
    Transform Massive exchange reference data into a cleaned,
    validated Silver-ready DataFrame.

    Args:
        spark (SparkSession): Active SparkSession instance.
        data_df (DataFrame): Raw Bronze DataFrame containing exchange records.

    Returns:
        DataFrame: Transformed and validated exchange DataFrame.
    """
    logger.info("[EXCHANGES] Processing massive exchanges dataset.")

    try:
        # ==============================================================================
        # 1. Select required columns from raw Bronze payload
        # ==============================================================================
        exchange_df = data_df.select(
            "asset_class",
            "id",
            "locale",
            "name",
            "operating_mic",
            "participant_id",
            "type",
        )

        # ==============================================================================
        # 2. Enforce explicit PySpark data types
        # ==============================================================================
        exchange_df = exchange_df.select(
            col("asset_class").cast(StringType()).alias("asset_class"),
            col("id").cast(LongType()).alias("id"),
            col("locale").cast(StringType()).alias("locale"),
            col("name").cast(StringType()).alias("name"),
            col("operating_mic").cast(StringType()).alias("operating_mic"),
            col("participant_id").cast(StringType()).alias("participant_id"),
            col("type").cast(StringType()).alias("type"),
        )

        required_string_cols = [
            "asset_class",
            "locale",
            "name",
            "operating_mic",
            "participant_id",
            "type",
        ]

        # ==============================================================================
        # 3. Trim string columns
        # ==============================================================================
        for column_name in required_string_cols:
            exchange_df = exchange_df.withColumn(
                column_name,
                trim(col(column_name)),
            )

        # ==============================================================================
        # 4. Convert fake NULL strings to actual PySpark NULL
        # ==============================================================================
        fake_null_values = [
            "",
            "n/a",
            "na",
            "null",
            "none",
            "-",
        ]

        for column_name in required_string_cols:
            exchange_df = exchange_df.withColumn(
                column_name,
                when(
                    lower(col(column_name)).isin(fake_null_values),
                    None,
                ).otherwise(col(column_name)),
            )

        # ==============================================================================
        # 5. Validation status tagging (Quarantine audit pattern)
        # ==============================================================================
        exchange_df = exchange_df.withColumn(
            "validation_status",
            when(col("asset_class").isNull(), "INVALID")
            .when(col("id").isNull(), "INVALID")
            .when(col("locale").isNull(), "INVALID")
            .when(col("name").isNull(), "INVALID")
            .when(col("operating_mic").isNull(), "INVALID")
            .when(col("participant_id").isNull(), "INVALID")
            .when(col("type").isNull(), "INVALID")
            .otherwise("VALID"),
        )

        # ==============================================================================
        # 6. Detailed validation error reason
        # ==============================================================================
        exchange_df = exchange_df.withColumn(
            "validation_reason",
            concat_ws(
                ", ",
                when(col("asset_class").isNull(), "MISSING ASSET_CLASS"),
                when(col("id").isNull(), "MISSING ID"),
                when(col("locale").isNull(), "MISSING LOCALE"),
                when(col("name").isNull(), "MISSING NAME"),
                when(col("operating_mic").isNull(), "MISSING OPERATING_MIC"),
                when(col("participant_id").isNull(), "MISSING PARTICIPANT_ID"),
                when(col("type").isNull(), "MISSING TYPE"),
            ),
        )

        # Standardize codes to uppercase/lowercase conventions
        exchange_df = exchange_df.withColumn(
            "mic",
            upper(col("operating_mic"))
        )

        exchange_df = exchange_df.withColumn(
            "type",
            upper(col("type"))
        )

        exchange_df = exchange_df.withColumn(
            "locale",
            lower(col("locale"))
        )

        logger.info("[EXCHANGES] Transformation completed successfully.")
        return exchange_df

    except Exception:
        logger.exception("[EXCHANGES] Error processing exchanges dataset.")
        raise
