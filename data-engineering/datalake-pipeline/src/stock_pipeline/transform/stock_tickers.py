"""
Finnhub Stock Tickers — Bronze-to-Silver Transformation Module.

Transforms Finnhub stock ticker reference data (JSON)
into a cleaned, validated Silver-ready PySpark DataFrame.

Finnhub Stock Tickers — Bronze-to-Silver Transformation Module.

Transforms Finnhub stock ticker reference data from Bronze JSON
into a cleaned, validated Silver-ready PySpark DataFrame.

Transformation Flow:
    1. Validate expected source columns
    2. Select and cast to target types
    3. Trim whitespace
    4. Normalize fake-null tokens
    5. Deduplicate on business key
    6. Standardize casing
    7. Apply validation rules
    8. Generate validation_reason
    9. Rename columns to Silver snake_case convention
    10. Log row counts
    11. Return Silver DataFrame

Dataset:
  - `ticker_reference`

Fields Processed:

  Dataset Columns
  ['currency', 'description', 'displaySymbol', 'figi', 'figiComposite', 'mic', 'shareClassFIGI', 'symbol', 'symbol2', 'type']

  root
    |-- currency: string (nullable = true)
    |-- description: string (nullable = true)
    |-- displaySymbol: string (nullable = true)
    |-- figi: string (nullable = true)
    |-- figiComposite: string (nullable = true)
    |-- isin: string (nullable = true)
    |-- mic: string (nullable = true)
    |-- shareClassFIGI: string (nullable = true)
    |-- symbol: string (nullable = true)
    |-- symbol2: string (nullable = true)
    |-- type: string (nullable = true)

    +--------+--------------------+-------------+------------+-------------+----+----+--------------+------+-------+------------+
    |currency|         description|displaySymbol|        figi|figiComposite|isin| mic|shareClassFIGI|symbol|symbol2|        type|
    +--------+--------------------+-------------+------------+-------------+----+----+--------------+------+-------+------------+
    |     USD|    ASANTE GOLD CORP|        ASGOF|BBG002W947B8| BBG002W947B8|    |OOTC|  BBG002CPHB76| ASGOF|       |Common Stock|
    |     USD|DEFSEC TECHNOLOGI...|         DFSC|BBG00Y2J0X44| BBG00Y2J0X44|    |XNAS|  BBG00K56JPN5|  DFSC|       |Common Stock|
    |     USD|PINNACLE BANCSHAR...|         PCLB|BBG000DDMV58| BBG000DDMV58|    |OOTC|  BBG001S7YS93|  PCLB|       |Common Stock|
    |     USD|INVERSE VIX ST FU...|         VYLD|BBG01SXN1MT9| BBG01SXN1MT9|    |ARCX|  BBG01SXN1NN3|  VYLD|       |         ETP|
    |     USD|APEX TREASURY COR...|         APXT|BBG01WR8YQ15| BBG01WR8YQ15|    |XNAS|  BBG01WR8YR59|  APXT|       |Common Stock|
    |     USD|RSE COLLECTION LL...|        RSSHS|BBG01BWMW3Y1| BBG01BWMW3Y1|    |OOTC|  BBG01BWMW4S6| RSSHS|       |Common Stock|
    |     USD|RSE ARCHIVE INT S...|        RWLMS|BBG018PM9H53| BBG018PM9H53|    |OOTC|  BBG018PM9J04| RWLMS|       |Common Stock|
    |     USD|  DYNO NOBEL LTD-ADR|        DNLZY|BBG007XH7HK3| BBG007XH7HK3|    |OOTC|  BBG007XH7JC8| DNLZY|       |         ADR|
    |     USD|KOITO MANUFACTURI...|        KOTMF|BBG000DW77Y4| BBG000DW77Y4|    |OOTC|  BBG001S6FVH0| KOTMF|       |Common Stock|
    |     USD|        TALPHERA INC|         TLPH|BBG0018YYFX7| BBG0018YYFX7|    |XNAS|  BBG001TFZCK5|  TLPH|       |Common Stock|
    +--------+--------------------+-------------+------------+-------------+----+----+--------------+------+-------+------------+

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
    initcap
)
from pyspark.sql.types import (
    LongType,
    StringType,
)

logger = logging.getLogger(__name__)


def transform_finnhub_stock_tickers_dataset(
    spark: SparkSession,
    data_df: DataFrame,
) -> DataFrame:
    """
    Transform Finnhub stock ticker reference data into a cleaned,
    validated Silver-ready DataFrame.
    """

    # ============================================================
    # STEP 1: START FINNHUB STOCK TICKER TRANSFORMATION
    # ============================================================

    print("\n" + "=" * 80)
    print("STEP 1: FINNHUB STOCK TICKERS — BRONZE → SILVER")
    print("=" * 80)

    logger.info(
        "[FINNHUB][STOCK_TICKERS] Starting Bronze-to-Silver "
        "transformation."
    )

    try:

        # ========================================================
        # STEP 2: VALIDATE EXPECTED SOURCE COLUMNS
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 2: VALIDATE FINNHUB BRONZE SCHEMA")
        print("=" * 80)

        expected_columns = [
            "currency",
            "description",
            "displaySymbol",
            "figi",
            "figiComposite",
            "isin",
            "mic",
            "shareClassFIGI",
            "symbol",
            "symbol2",
            "type",
        ]

        missing_columns = [
            column
            for column in expected_columns
            if column not in data_df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing expected Finnhub columns: {missing_columns}"
            )

        logger.info(
            "[FINNHUB][STOCK_TICKERS] Schema validation successful | "
            "columns=%s",
            data_df.columns,
        )

        # ========================================================
        # STEP 3: SELECT + CAST
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 3: SELECT + CAST FINNHUB STOCK TICKER DATA")
        print("=" * 80)

        ticker_df = data_df.select(
            col("currency").cast(StringType()).alias("currency"),
            col("description").cast(StringType()).alias("description"),
            col("displaySymbol").cast(StringType()).alias("displaySymbol"),
            col("figi").cast(StringType()).alias("figi"),
            col("figiComposite").cast(StringType()).alias("figiComposite"),
            col("isin").cast(StringType()).alias("isin"),
            col("mic").cast(StringType()).alias("mic"),
            col("shareClassFIGI").cast(StringType()).alias("shareClassFIGI"),
            col("symbol").cast(StringType()).alias("symbol"),
            col("symbol2").cast(StringType()).alias("symbol2"),
            col("type").cast(StringType()).alias("type"),
        )

        logger.info(
            "[FINNHUB][STOCK_TICKERS] Required columns selected "
            "and cast to StringType."
        )

        # ========================================================
        # STEP 4: TRIM WHITESPACE
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 4: TRIM WHITESPACE")
        print("=" * 80)

        dataset_cols = ticker_df.columns

        for cols in dataset_cols:
            ticker_df = ticker_df.withColumn(
                cols,
                trim(col(cols)),
            )

        logger.info(
            "[FINNHUB][STOCK_TICKERS] Whitespace trimming completed."
        )

        # ========================================================
        # STEP 5: NORMALIZE FAKE NULL VALUES
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 5: NORMALIZE FAKE NULL VALUES")
        print("=" * 80)

        fake_null_values = [
            "",
            "n/a",
            "na",
            "null",
            "-",
        ]

        for cols in dataset_cols:
            ticker_df = ticker_df.withColumn(
                cols,
                when(
                    lower(col(cols)).isin(fake_null_values),
                    None,
                ).otherwise(
                    col(cols)
                ),
            )

        logger.info(
            "[FINNHUB][STOCK_TICKERS] Fake-null normalization completed."
        )

        # ========================================================
        # STEP 6: DEDUPLICATE
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 6: DEDUPLICATE FINNHUB STOCK TICKERS")
        print("=" * 80)

        ticker_df = ticker_df.dropDuplicates(
            ["symbol", "mic"]
        )

        logger.info(
            "[FINNHUB][STOCK_TICKERS] Deduplication completed | "
            "business_key=symbol+mic"
        )

        # ========================================================
        # STEP 7: STANDARDIZE CASING
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 7: STANDARDIZE FINNHUB STOCK TICKER CASING")
        print("=" * 80)

        ticker_df = (
            ticker_df
            .withColumn(
                "currency",
                upper(col("currency")),
            )
            .withColumn(
                "description",
                initcap(col("description")),
            )
            .withColumn(
                "displaySymbol",
                upper(col("displaySymbol")),
            )
            .withColumn(
                "figi",
                upper(col("figi")),
            )
            .withColumn(
                "figiComposite",
                upper(col("figiComposite")),
            )
            .withColumn(
                "isin",
                upper(col("isin")),
            )
            .withColumn(
                "mic",
                upper(col("mic")),
            )
            .withColumn(
                "shareClassFIGI",
                upper(col("shareClassFIGI")),
            )
            .withColumn(
                "symbol",
                upper(col("symbol")),
            )
            .withColumn(
                "symbol2",
                upper(col("symbol2")),
            )
            .withColumn(
                "type",
                initcap(col("type")),
            )
        )

        logger.info(
            "[FINNHUB][STOCK_TICKERS] Casing standardization completed."
        )

        # ========================================================
        # STEP 8: VALIDATION STATUS
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 8: APPLY FINNHUB STOCK TICKER VALIDATION")
        print("=" * 80)

        ticker_df = ticker_df.withColumn(
            "validation_status",
            when(
                col("currency").isNull(),
                "INVALID",
            ).when(
                col("description").isNull(),
                "INVALID",
            ).when(
                col("displaySymbol").isNull(),
                "INVALID",
            ).when(
                col("symbol").isNull(),
                "INVALID",
            ).when(
                col("type").isNull(),
                "INVALID",
            ).otherwise(
                "VALID"
            ),
        )

        logger.info(
            "[FINNHUB][STOCK_TICKERS] Validation status generated."
        )

        # ========================================================
        # STEP 9: VALIDATION REASON
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 9: GENERATE VALIDATION REASONS")
        print("=" * 80)

        ticker_df = ticker_df.withColumn(
            "validation_reason",
            concat_ws(
                ", ",
                when(
                    col("currency").isNull(),
                    "Currency Invalid",
                ),
                when(
                    col("description").isNull(),
                    "Description Invalid",
                ),
                when(
                    col("displaySymbol").isNull(),
                    "Display Symbol Invalid",
                ),
                when(
                    col("symbol").isNull(),
                    "Symbol Invalid",
                ),
                when(
                    col("type").isNull(),
                    "Type Invalid",
                ),
            ),
        )

        logger.info(
            "[FINNHUB][STOCK_TICKERS] Validation reasons generated."
        )

        # ========================================================
        # STEP 10: RENAME TO SILVER NAMING CONVENTION
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 10: RENAME COLUMNS TO SILVER SNAKE_CASE")
        print("=" * 80)

        ticker_df = (
            ticker_df
            .withColumnRenamed(
                "displaySymbol",
                "display_symbol",
            )
            .withColumnRenamed(
                "figiComposite",
                "figi_composite",
            )
            .withColumnRenamed(
                "shareClassFIGI",
                "share_class_figi",
            )
        )

        logger.info(
            "[FINNHUB][STOCK_TICKERS] Silver column naming completed | "
            "columns=%s",
            ticker_df.columns,
        )

        # ========================================================
        # STEP 11: ROW COUNT METRICS
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 11: FINNHUB STOCK TICKER ROW COUNT METRICS")
        print("=" * 80)

        total_in = data_df.count()
        total_out = ticker_df.count()

        valid_count = ticker_df.filter(
            col("validation_status") == "VALID"
        ).count()

        invalid_count = ticker_df.filter(
            col("validation_status") == "INVALID"
        ).count()

        logger.info(
            "[FINNHUB][STOCK_TICKERS][METRICS] "
            "total_in=%d | total_out=%d | valid=%d | invalid=%d",
            total_in,
            total_out,
            valid_count,
            invalid_count,
        )

        print(f"Total In      : {total_in}")
        print(f"Total Out     : {total_out}")
        print(f"Valid Records : {valid_count}")
        print(f"Invalid       : {invalid_count}")

        # ========================================================
        # STEP 12: RETURN SILVER DATAFRAME
        # ========================================================

        print("\n" + "=" * 80)
        print("STEP 12: FINNHUB STOCK TICKER TRANSFORMATION COMPLETED")
        print("=" * 80)

        logger.info(
            "[FINNHUB][STOCK_TICKERS] Bronze-to-Silver transformation "
            "completed successfully."
        )

        return ticker_df

    except Exception:

        print("\n" + "!" * 80)
        print("FINNHUB STOCK TICKER TRANSFORMATION FAILED")
        print("!" * 80)

        logger.exception(
            "[FINNHUB][STOCK_TICKERS] Error processing "
            "Finnhub stock ticker dataset."
        )

        raise