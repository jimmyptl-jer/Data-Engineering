"""
Massive Exchanges — Bronze-to-Silver Transformation Module.

Transforms Massive (Polygon.io) exchange reference data (JSON)
into a cleaned, validated Silver-ready PySpark DataFrame.

Massive Exchanges — Bronze-to-Silver Transformation Module.

Transforms Massive (Polygon.io) exchange reference data from Bronze JSON
into a cleaned, validated Silver-ready PySpark DataFrame.

Transformation Flow:
    1. Select required columns from Bronze payload
    2. Enforce explicit PySpark data types
    3. Trim whitespace
    4. Normalize fake-null tokens
    5. Apply validation rules
    6. Generate validation_reason
    7. Standardize casing
    8. Row count metrics
    9. Return Silver DataFrame

Dataset:
  - `exchanges`

Fields Processed:

  Dataset Columns
  ['asset_class', 'id', 'locale', 'name', 'operating_mic', 'participant_id', 'type']

  root
    |-- asset_class: string (nullable = true)
    |-- id: long (nullable = true)
    |-- locale: string (nullable = true)
    |-- name: string (nullable = true)
    |-- operating_mic: string (nullable = true)
    |-- participant_id: string (nullable = true)
    |-- type: string (nullable = true)

    +-----------+---+------+-----------------------------+-------------+--------------+--------+
    |asset_class| id|locale|                         name|operating_mic|participant_id|    type|
    +-----------+---+------+-----------------------------+-------------+--------------+--------+
    |     stocks|  1|    us|       New York Stock Exchange|         XNYS|          NYSE|EXCHANGE|
    |     stocks|  2|    us|NYSE American, LLC (AMEX)    |         XASE|          AMEX|EXCHANGE|
    |     stocks| 10|    us|       IEX Market Inc.        |         IEXG|          IEXG|EXCHANGE|
    |     stocks| 11|    us|          Chicago Board Options|         XCBO|          CBOE|EXCHANGE|
    |     crypto|  6|global|         Coinbase              |         COINBASE|      COINBASE|EXCHANGE|
    +-----------+---+------+-----------------------------+-------------+--------------+--------+

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
    """

    # ============================================================
    # STEP 1: START MASSIVE EXCHANGES TRANSFORMATION
    # ============================================================


    logger.info(
        "[MASSIVE][EXCHANGES] Starting Bronze-to-Silver "
        "transformation."
    )

    try:

        # ========================================================
        # STEP 2: SELECT + CAST
        # ========================================================


        exchange_df = data_df.select(
            "asset_class",
            "id",
            "locale",
            "name",
            "operating_mic",
            "participant_id",
            "type",
        )

        exchange_df = exchange_df.select(
            col("asset_class").cast(StringType()).alias("asset_class"),
            col("id").cast(LongType()).alias("id"),
            col("locale").cast(StringType()).alias("locale"),
            col("name").cast(StringType()).alias("name"),
            col("operating_mic").cast(StringType()).alias("operating_mic"),
            col("participant_id").cast(StringType()).alias("participant_id"),
            col("type").cast(StringType()).alias("type"),
        )

        logger.info(
            "[MASSIVE][EXCHANGES] Required columns selected "
            "and cast to target types."
        )

        # ========================================================
        # STEP 3: TRIM WHITESPACE
        # ========================================================


        required_string_cols = [
            "asset_class",
            "locale",
            "name",
            "operating_mic",
            "participant_id",
            "type",
        ]

        for column_name in required_string_cols:
            exchange_df = exchange_df.withColumn(
                column_name,
                trim(col(column_name)),
            )

        logger.info(
            "[MASSIVE][EXCHANGES] Whitespace trimming completed."
        )

        # ========================================================
        # STEP 4: NORMALIZE FAKE NULL VALUES
        # ========================================================


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

        logger.info(
            "[MASSIVE][EXCHANGES] Fake-null normalization completed."
        )

        # ========================================================
        # STEP 5: VALIDATION STATUS
        # ========================================================


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

        logger.info(
            "[MASSIVE][EXCHANGES] Validation status generated."
        )

        # ========================================================
        # STEP 6: VALIDATION REASON
        # ========================================================


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

        logger.info(
            "[MASSIVE][EXCHANGES] Validation reasons generated."
        )

        # ========================================================
        # STEP 7: STANDARDIZE CASING
        # ========================================================


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

        logger.info(
            "[MASSIVE][EXCHANGES] Casing standardization completed."
        )

        # ========================================================
        # STEP 8: ROW COUNT METRICS
        # ========================================================

        logger.info(
            "[MASSIVE][EXCHANGES][METRICS] "
            "total_in=%d | total_out=%d | valid=%d | invalid=%d",
            total_in,
            total_out,
            valid_count,
            invalid_count,
        )

        # ========================================================
        # STEP 9: RETURN SILVER DATAFRAME
        # ========================================================


        logger.info(
            "[MASSIVE][EXCHANGES] Bronze-to-Silver transformation "
            "completed successfully."
        )

        return exchange_df

    except Exception:

        logger.exception(
            "[MASSIVE][EXCHANGES] Error processing exchanges "
            "dataset."
        )

        raise
