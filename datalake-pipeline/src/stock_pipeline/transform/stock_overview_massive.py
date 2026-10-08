"""
Massive Stock Overview — Bronze-to-Silver Transformation Module.

Transforms raw Bronze Massive Stock Overview reference data (JSON)
into a clean, typed, standardized Silver DataFrame.

Transformation Flow:
    1. Select required business columns
    2. Rename Massive API fields to standardized snake_case names
    3. Trim whitespace and standardize casing

Dataset:
    - `stock_overview`

Source:
    - Massive Stock Overview API
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    upper,
)

logger = logging.getLogger(__name__)


# ============================================================
# MASSIVE STOCK OVERVIEW — SOURCE COLUMNS
# ============================================================

MASSIVE_STOCK_OVERVIEW_COLUMNS = [
    "locale",
    "market",
    "ticker",
    "list_date",
    "market_cap",
    "name",
    "primary_exchange",
    "share_class_figi",
    "composite_figi",
    "cik",
    "share_class_shares_outstanding",
]


# ============================================================
# MASSIVE STOCK OVERVIEW — BRONZE → SILVER
# ============================================================

def transform_massive_stock_overview(
    spark: SparkSession,
    data_df: DataFrame,
) -> DataFrame:
    """
    Transform Massive Stock Overview Bronze JSON data
    into a cleaned and standardized Silver DataFrame.
    """

    # ========================================================
    # STEP 1: START MASSIVE STOCK OVERVIEW TRANSFORMATION
    # ========================================================

    logger.info(
        "[MASSIVE][STOCK_OVERVIEW] "
        "Starting Bronze-to-Silver transformation."
    )

    try:

        # ====================================================
        # STEP 2: SELECT REQUIRED BUSINESS COLUMNS
        # ====================================================

        logger.info(
            "[MASSIVE][STOCK_OVERVIEW] "
            "Selecting required business columns."
        )

        data_df = data_df.select(
            *MASSIVE_STOCK_OVERVIEW_COLUMNS
        )

        logger.info(
            "[MASSIVE][STOCK_OVERVIEW] "
            "Required business columns selected | "
            "columns=%d",
            len(MASSIVE_STOCK_OVERVIEW_COLUMNS),
        )

        # ====================================================
        # STEP 3: RENAME MASSIVE COLUMNS
        # ====================================================

        logger.info(
            "[MASSIVE][STOCK_OVERVIEW] "
            "Standardizing Massive column names."
        )

        data_df = (
            data_df
            .withColumnRenamed(
                "ticker",
                "symbol",
            )
            .withColumnRenamed(
                "name",
                "company_name",
            )
            .withColumnRenamed(
                "market",
                "asset_type"   
            )
            .withColumnRenamed(
                "share_class_shares_outstanding",
                "shares_outstanding",
            )
        )

        logger.info(
            "[MASSIVE][STOCK_OVERVIEW] "
            "Column standardization completed."
        )

        # ====================================================
        # STEP 4: TRIM WHITESPACE & STANDARDIZE CASING
        # ====================================================

        logger.info(
            "[MASSIVE][STOCK_OVERVIEW] "
            "Starting whitespace trimming and "
            "casing standardization."
        )

        # ----------------------------------------------------
        # Trim all string columns.
        # ----------------------------------------------------

        string_columns = [
            field.name
            for field in data_df.schema.fields
            if field.dataType.simpleString() == "string"
        ]

        for column_name in string_columns:
            data_df = data_df.withColumn(
                column_name,
                trim(col(column_name)),
            )

        # ----------------------------------------------------
        # Standardize stock symbol to uppercase.
        # ----------------------------------------------------

        data_df = data_df.withColumn(
            "symbol",
            upper(col("symbol")),
        )

        # ----------------------------------------------------
        # Standardize exchange MIC to uppercase.
        #
        # Example:
        #     XNYS
        # ----------------------------------------------------

        data_df = data_df.withColumn(
            "primary_exchange",
            upper(col("primary_exchange")),
        )

        # ----------------------------------------------------
        # Standardize locale and market values.
        #
        # Example:
        #     locale = us
        #     market = stocks
        # ----------------------------------------------------

        data_df = data_df.withColumn(
            "locale",
            lower(col("locale")),
        )

        data_df = data_df.withColumn(
            "asset_type",
            lower(col("asset_type")),
        )

        logger.info(
            "[MASSIVE][STOCK_OVERVIEW] "
            "Whitespace trimming and casing "
            "standardization completed."
        )

        return data_df

    except Exception as exc:

        logger.exception(
            "[MASSIVE][STOCK_OVERVIEW] "
            "Bronze-to-Silver transformation failed | "
            "error=%s",
            exc,
        )

        raise