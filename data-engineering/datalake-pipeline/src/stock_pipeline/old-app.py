"""
Entry point — ties extract -> transform -> load together, and exposes
lambda_handler for AWS Lambda/SAM.
"""

import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from . import config
from .extract import StockDataExtractor
from .transform import StockDataTransformer
from .load import StockDataLoader

from .utils import APIKeyManager
from .ingestion.alpha_vantage_ingestion import AlphaVantageIngestion

# ============================================================
# LOGGING CONFIGURATION
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# SPARK SESSION FACTORY
# ============================================================

def create_spark_session() -> SparkSession:
    """Create and return a configured SparkSession."""

    logger.info("Initializing Spark session for Stock Data Pipeline.")

    try:
        spark = (
            SparkSession.builder
            .appName("StockDataPipeline")
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.4.1",
            )
            .getOrCreate()
        )

        logger.info(
            "Spark session initialized successfully. Spark version: %s",
            spark.version,
        )

        return spark

    except Exception as e:
        logger.exception(
            "Failed to initialize Spark session: %s",
            e,
        )
        raise


def write_layer_parquet(df, base_path, dataset_name, run_time):
    logger.info("Writing %s dataset to Silver layer at %s", dataset_name, base_path)
    logger.debug(
        "Silver parquet write parameters: dataset=%s, base_path=%s, run_time=%s",
        dataset_name,
        base_path,
        run_time.isoformat(),
    )

    (
        df.write
        .mode("overwrite")
        .parquet(
            f"{base_path}"
            f"dataset={dataset_name}/"
            f"year={run_time.year}/"
            f"month={run_time.month:02d}/"
            f"day={run_time.day:02d}/"
            f"format=parquet/"
        )
    )
    
def write_layer_csv(df, base_path, dataset_name, run_time):
    logger.info("Writing %s dataset to Silver layer at %s", dataset_name, base_path)
    logger.debug(
        "Silver CSV write parameters: dataset=%s, base_path=%s, run_time=%s",
        dataset_name,
        base_path,
        run_time.isoformat(),
    )

    (
        df.write
        .mode("overwrite")
        .options(header=True)
        .csv(
            f"{base_path}"
            f"dataset={dataset_name}/"
            f"year={run_time.year}/"
            f"month={run_time.month:02d}/"
            f"day={run_time.day:02d}/"
            f"format=csv/"
        )
    )


# ============================================================
# STOCK PIPELINE
# ============================================================

class StockPipeline:

    def __init__(self):

        logger.info("Initializing StockPipeline.")

        try:
            self.aws_access_key_id = config.AWS_ACCESS_KEY_ID
            self.aws_secret_access_key = config.AWS_SECRET_ACCESS_KEY
            self.s3_bucket_name = config.S3_BUCKET_NAME
            self.alpha_vantage_config = config.ALPHA_VANTAGE_ENDPOINTS
            
            self.silver_base_path = config.SILVER_BASE_PATH
            self.gold_base_path = config.GOLD_BASE_PATH

            self.spark = create_spark_session()

            self.extractor = StockDataExtractor(self.spark)
            self.transformer = StockDataTransformer(self.spark)
            self.loader = StockDataLoader(
                self.aws_access_key_id,
                self.aws_secret_access_key,
            )

            # api_key_manager must exist before AlphaVantageIngestion is
            # constructed, since ingestion depends on it.
            self.api_key_manager = APIKeyManager(
                config.ALPHA_VANTAGE_API_KEYS
            )

            self.ingestion = AlphaVantageIngestion(
                extractor=self.extractor,
                loader=self.loader,
                bucket_name=self.s3_bucket_name,
                api_key_manager=self.api_key_manager,
            )

            logger.info(
                "StockPipeline configuration initialized successfully."
            )

            logger.info(
                "Target S3 bucket configured: %s",
                self.s3_bucket_name,
            )

        except Exception as e:
            logger.exception(
                "Failed to initialize StockPipeline configuration: %s",
                e,
            )
            raise

    # ========================================================
    # ETL PIPELINE
    # ========================================================

    def run(self):
        logger.info("[PIPELINE] Starting Stock ETL pipeline.")

        start_time = datetime.now(timezone.utc)

        daily_dataset = config.get_dataset_name_by_function("TIME_SERIES_DAILY")
        overview_dataset = config.get_dataset_name_by_function("OVERVIEW")
        logger.debug(
            "[PIPELINE] Resolved datasets: daily=%s, overview=%s",
            daily_dataset,
            overview_dataset,
        )

        try:
            run_time = datetime.now(timezone.utc)

            # ============================================================
            # EXTRACT (BRONZE)
            # ============================================================
            logger.info("[BRONZE] Extracting daily and overview datasets.")

            extracted_daily_data = self.extractor.extract_bronze_daily_data(daily_dataset)
            extracted_overview_data = self.extractor.extract_bronze_overview_data(overview_dataset)

            logger.debug(
                "[BRONZE] Schemas: daily=%s | overview=%s",
                extracted_daily_data.schema.simpleString(),
                extracted_overview_data.schema.simpleString(),
            )

            # ============================================================
            # TRANSFORM + WRITE (SILVER)
            # ============================================================
            logger.info("[SILVER] Transforming bronze datasets to silver format.")

            daily_df = self.transformer.silver_transform_daily_timeseries(extracted_daily_data)
            overview_df = self.transformer.silver_transform_overview_timeseries(extracted_overview_data)

            logger.debug(
                "[SILVER] Schemas: daily=%s | overview=%s",
                daily_df.schema.simpleString(),
                overview_df.schema.simpleString(),
            )

            silver_base_path = self.silver_base_path
            gold_base_path = self.gold_base_path

            for df, dataset in [(daily_df, daily_dataset), (overview_df, overview_dataset)]:
                write_layer_csv(df, silver_base_path, dataset, run_time)
                logger.info("[SILVER] CSV write completed for dataset=%s.", dataset)

                write_layer_parquet(df, silver_base_path, dataset, run_time)
                logger.info("[SILVER] Parquet write completed for dataset=%s.", dataset)

            logger.info("[SILVER] Silver layer write completed successfully.")

            # ============================================================
            # GOLD - PARQUET PATH
            # ============================================================
            logger.info("[GOLD-PARQUET] Reading silver parquet datasets.")

            extracted_daily_data = self.extractor.extract_silver_daily_data_parquet(daily_dataset)
            extracted_overview_data = self.extractor.extract_silver_overview_data_parquet(overview_dataset)

            logger.debug(
                "[GOLD-PARQUET] Schemas before select: daily=%s | overview=%s",
                extracted_daily_data.schema.simpleString(),
                extracted_overview_data.schema.simpleString(),
            )

            extracted_daily_data = extracted_daily_data.select(
                "symbol", "day_date", "open", "high", "low", "close", "volume",
                "daily_change", "daily_change_percentage", "market_movement",
                "thirty_day_avg_open", "thirty_day_avg_close",
                "all_time_high", "all_time_low", "processed_at",
            )
            extracted_overview_data = extracted_overview_data.drop("processed_at")

            logger.debug(
                "[GOLD-PARQUET] Schemas after select/drop: daily=%s | overview=%s",
                extracted_daily_data.schema.simpleString(),
                extracted_overview_data.schema.simpleString(),
            )

            logger.info("[GOLD-PARQUET] Performing gold join.")
            gold_df = extracted_daily_data.join(
                extracted_overview_data, on="symbol", how="left"
            )
            logger.debug("[GOLD-PARQUET] Joined schema: %s", gold_df.schema.simpleString())

            # ============================================================
            # GOLD - CSV PATH
            # ============================================================
            logger.info("[GOLD-CSV] Reading silver csv datasets.")

            extracted_daily_data_csv = self.extractor.extract_silver_daily_data_csv(
                daily_dataset, "csv"
            )
            extracted_overview_data_csv = self.extractor.extract_silver_overview_data_csv(
                overview_dataset, "csv"
            )

            logger.debug(
                "[GOLD-CSV] Schemas before select: daily=%s | overview=%s",
                extracted_daily_data_csv.schema.simpleString(),
                extracted_overview_data_csv.schema.simpleString(),
            )

            extracted_daily_data_csv = extracted_daily_data_csv.select(
                "symbol", "day_date", "open", "high", "low", "close", "volume",
                "daily_change", "daily_change_percentage", "market_movement",
                "thirty_day_avg_open", "thirty_day_avg_close",
                "all_time_high", "all_time_low", "processed_at",
            )
            extracted_overview_data_csv = extracted_overview_data_csv.drop("processed_at")

            logger.info("[GOLD-CSV] Performing gold join.")
            gold_csv_df = extracted_daily_data_csv.join(
                extracted_overview_data_csv, on="symbol", how="left"
            )
            
            gold_csv_df = gold_csv_df.show(1, truncate=False)  # Show 10 rows for debugging
            logger.debug("[GOLD-CSV] Joined schema: %s", gold_csv_df.schema.simpleString())

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "[GOLD-CSV] Sample rows:\n%s",
                    gold_csv_df._jdf.showString(5, 20, False),
                )

            # ============================================================
            # WRITE (GOLD)
            # ============================================================
            logger.info("[GOLD] Writing gold datasets to S3 (CSV + Parquet).")
            logger.debug(
                "[GOLD] Write params: base_path=%s, run_time=%s",
                gold_base_path,
                run_time.isoformat(),
            )

            write_layer_csv(gold_csv_df, gold_base_path, "company_dataset", run_time)
            logger.info("[GOLD-CSV] Write completed for dataset=company_dataset.")

            write_layer_parquet(gold_df, gold_base_path, "company_dataset", run_time)
            logger.info("[GOLD-PARQUET] Write completed for dataset=company_dataset.")

            # ============================================================
            # PIPELINE COMPLETION
            # ============================================================
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            logger.info(
                "[PIPELINE] Completed successfully. Duration: %.2f seconds.", duration
            )

        except Exception:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            logger.exception(
                "[PIPELINE] Failed after %.2f seconds.", duration
            )
            raise

# ============================================================
# AWS LAMBDA HANDLER
# ============================================================
def lambda_handler(event, context):

    execution_start_time = datetime.now(timezone.utc)

    logger.info(
        "Stock Lambda execution started at %s",
        execution_start_time.isoformat(),
    )

    stock_symbols = [
        "IBM"
    ]

    try:
        logger.info("Creating StockPipeline instance.")

        stock_pipeline = StockPipeline()

        alpha_vantage_endpoints = stock_pipeline.alpha_vantage_config

        results = []

        for symbol in stock_symbols:

            for endpoint in alpha_vantage_endpoints:

                function = endpoint["function"]
                dataset = endpoint["dataset"]

                try:
                    response = stock_pipeline.ingestion.ingest(
                        symbol,
                        function=function,
                        dataset=dataset,
                    )

                    results.append({
                        "symbol": symbol,
                        "function": function,
                        "response": response,
                    })

                except Exception as e:
                    logger.exception(
                        "Error ingesting symbol %s via %s: %s",
                        symbol,
                        function,
                        e,
                    )

                    results.append({
                        "symbol": symbol,
                        "function": function,
                        "error": str(e),
                    })

        # ========================================================
        # INGESTION SUMMARY
        # ========================================================

        successful_symbols = [
            result["symbol"]
            for result in results
            if "error" not in result
        ]

        failed_symbols = [
            result["symbol"]
            for result in results
            if "error" in result
        ]

        logger.info(
            "Stock ingestion completed. "
            "Successful: %s, Failed: %s",
            len(successful_symbols),
            len(failed_symbols),
        )
        logger.debug("Ingestion results: %s", results)

        logger.info(
            "Successfully processed symbols: %s",
            successful_symbols,
        )

        if failed_symbols:
            logger.warning(
                "Failed stock symbols: %s",
                failed_symbols,
            )

        # ========================================================
        # RUN ETL
        # ========================================================

        logger.info("Starting Stock ETL processing from Bronze layer.")

        stock_pipeline.run()

        logger.info("Stock ETL processing completed successfully.")

        # ========================================================
        # EXECUTION SUMMARY
        # ========================================================

        execution_end_time = datetime.now(timezone.utc)
        total_duration = (
            execution_end_time - execution_start_time
        ).total_seconds()

        logger.info(
            "Stock Lambda execution completed successfully. "
            "Started: %s, Finished: %s, Duration: %.2f seconds",
            execution_start_time.isoformat(),
            execution_end_time.isoformat(),
            total_duration,
        )

        return {
            "statusCode": 200,
            "body": "Stock pipeline executed successfully.",
            "results": results,
        }

    except Exception as e:
        execution_end_time = datetime.now(timezone.utc)
        total_duration = (
            execution_end_time - execution_start_time
        ).total_seconds()

        logger.exception(
            "Stock Lambda execution failed after %.2f seconds: %s",
            total_duration,
            e,
        )

        raise


# ============================================================
# LOCAL EXECUTION
# ============================================================

if __name__ == "__main__":

    logger.info("Running Stock Data Pipeline locally.")

    try:
        lambda_handler({}, {})

        logger.info("Local Stock Data Pipeline execution completed.")

    except Exception as e:
        logger.exception(
            "Local Stock Data Pipeline execution failed: %s",
            e,
        )

        raise