import logging

from pyspark.sql import SparkSession, DataFrame

from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_date,
    year,
    month,
    weekofyear,
    explode,
    round,
    when,
)

logger = logging.getLogger(__name__)


class StockDataTransformer:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def transform_weekly_timeseries(
        self,
        data_df: DataFrame,
    ) -> DataFrame:

        logger.info("Starting Stock JSON data transformation.")

        try:
            # ====================================================
            # EXPLODE WEEKLY TIME SERIES
            # ====================================================

            logger.info("Exploding Weekly Time Series.")

            data_df = data_df.select(
                col("`Meta Data`.`2. Symbol`").alias("symbol"),
                col("`Meta Data`.`3. Last Refreshed`").alias("last_refreshed"),
                explode(col("`Weekly Time Series`")).alias(
                    "week_date",
                    "weekly_data",
                ),
            )

            data_df = data_df.filter(col("symbol").isNotNull())
            data_df = data_df.filter(col("last_refreshed").isNotNull())

            logger.info("Weekly Time Series exploded successfully.")

            # ====================================================
            # SELECT REQUIRED COLUMNS
            # ====================================================

            logger.info("Selecting required Stock columns.")

            data_df = data_df.filter(col("symbol") == "IBM")

            data_df = data_df.select(
                col("symbol"),
                col("last_refreshed"),
                col("week_date"),
                col("weekly_data.`1. open`").alias("open"),
                col("weekly_data.`2. high`").alias("high"),
                col("weekly_data.`3. low`").alias("low"),
                col("weekly_data.`4. close`").alias("close"),
                col("weekly_data.`5. volume`").alias("volume"),
            )

            logger.info("Required Stock columns selected successfully.")

            # ====================================================
            # DROP ROWS MISSING KEY IDENTIFIERS
            # ====================================================

            logger.info("Dropping rows with null symbol/last_refreshed.")

            data_df = data_df.dropna(subset=["symbol", "last_refreshed"])

            row_count = data_df.count()
            logger.info("Row count after dropna: %s", row_count)

            # ====================================================
            # CAST COLUMN TYPES
            # ====================================================

            logger.info("Casting columns to their target types.")

            data_df = (
                data_df
                .withColumn("week_date", col("week_date").cast("date"))
                .withColumn("last_refreshed", col("last_refreshed").cast("date"))
                .withColumn("open", col("open").cast("double"))
                .withColumn("high", col("high").cast("double"))
                .withColumn("low", col("low").cast("double"))
                .withColumn("close", col("close").cast("double"))
                .withColumn("volume", col("volume").cast("long"))
            )

            # ====================================================
            # FILL MISSING NUMERIC VALUES
            # ====================================================
            # Must happen AFTER casting - fillna with numeric literals
            # has no effect on columns that are still strings.

            logger.info("Filling null numeric values with 0.")

            data_df = data_df.fillna({
                "open": 0.0,
                "high": 0.0,
                "low": 0.0,
                "close": 0.0,
                "volume": 0,
            })

            # ====================================================
            # ROUND PRICE COLUMNS
            # ====================================================

            logger.info("Rounding price columns to 2 decimal places.")

            data_df = (
                data_df
                .withColumn("open", round(col("open"), 2))
                .withColumn("high", round(col("high"), 2))
                .withColumn("low", round(col("low"), 2))
                .withColumn("close", round(col("close"), 2))
            )

            # ====================================================
            # DERIVE ANALYTICAL COLUMNS
            # ====================================================

            logger.info("Deriving weekly change and market movement columns.")

            data_df = data_df.withColumn(
                "weekly_change",
                round(col("open") - col("close"), 2),
            )

            data_df = data_df.withColumn(
                "market_movement",
                when(col("open") > col("close"), "Bull")
                .when(col("open") < col("close"), "Bear")
                .otherwise("Neutral"),
            )

            data_df = data_df.withColumn(
                "weekly_change_percentage",
                round(
                    ((col("close") - col("open")) / col("open")) * 100,
                    2,
                ),
            )

            # ====================================================
            # RENAME AND SORT
            # ====================================================

            logger.info("Renaming columns and sorting by week_date.")

            data_df = data_df.withColumnRenamed(
                "last_refreshed",
                "last_refreshed_date",
            )

            data_df = data_df.orderBy(col("week_date").desc())
            
            data_df = (
                data_df

                # String -> Date
                .withColumn(
                    "week_date",
                    to_date(col("week_date"), "yyyy-MM-dd")
                )

                # Extract date components
                .withColumn(
                    "year",
                    year(col("week_date"))
                )
                .withColumn(
                    "month",
                    month(col("week_date"))
                )
                .withColumn(
                    "week_number",
                    weekofyear(col("week_date"))
                )

                # Add processing timestamp
                .withColumn(
                    "processed_at",
                    current_timestamp()
                )
            )

            # ====================================================
            # PRINT TRANSFORMED SCHEMA
            # ====================================================

            print("\n========== TRANSFORMED SCHEMA ==========")
            data_df.printSchema()

            # ====================================================
            # SHOW SAMPLE DATA
            # ====================================================

            print("\n========== TRANSFORMED DATA ==========")
            data_df.show(10, truncate=False)

            logger.info("Stock data transformation completed successfully.")

            return data_df

        except Exception as e:
            logger.exception(
                "Error occurred while transforming Stock data: %s",
                e,
            )
            raise
        