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


# ============================================================
# LOGGING CONFIGURATION
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
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


# ============================================================
# STOCK PIPELINE
# ============================================================

class StockPipeline:

    def __init__(self):

        logger.info("Initializing StockPipeline.")

        try:
            self.alpha_vantage_api_key = config.ALPHA_VANTAGE_API_KEY
            self.aws_access_key_id = config.AWS_ACCESS_KEY_ID
            self.aws_secret_access_key = config.AWS_SECRET_ACCESS_KEY
            self.s3_bucket_name = config.S3_BUCKET_NAME

            self.spark = create_spark_session()

            self.extractor = StockDataExtractor(self.spark)
            self.transformer = StockDataTransformer(self.spark)
            self.loader = StockDataLoader(
                self.aws_access_key_id,
                self.aws_secret_access_key,
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

        logger.info("Starting Stock ETL pipeline.")

        start_time = datetime.now(timezone.utc)

        try:
            # ====================================================
            # INITIALIZE EXTRACTOR
            # ====================================================

            logger.info(
                "Initializing StockDataExtractor."
            )

            logger.info(
                "StockDataExtractor initialized successfully."
            )

            # ====================================================
            # JSON EXTRACTION
            # ====================================================

            logger.info(
                "Starting stock JSON extraction."
            )
            extracted_data = self.extractor.extract_data()

            transformed_data = (
                self.transformer.transform_weekly_timeseries(extracted_data)
            )

            silver_path = (
                f"s3a://graywolf--data--lake/"
                f"silver/stock_data/source=stock_data/"
            )

            transformed_data.write.mode("overwrite").option(
                "header", "true"
            ).csv(silver_path)

            logger.info(
                "Stock JSON extraction completed successfully."
            )

            # ====================================================
            # PIPELINE COMPLETION
            # ====================================================
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            logger.info(
                "Stock ETL pipeline completed successfully. "
                "Duration: %.2f seconds",
                duration,
            )

        except Exception as e:
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            logger.exception(
                "Stock ETL pipeline failed after %.2f seconds: %s",
                duration,
                e,
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
        "IBM",
        "AAPL",
        "MSFT",
        "GOOGL",
        "AMZN",
    ]

    logger.info(
        "Configured stock symbols for ingestion: %s",
        stock_symbols,
    )

    logger.info(
        "Total stock symbols to process: %s",
        len(stock_symbols),
    )

    try:
        logger.info(
            "Creating StockPipeline instance."
        )

        stock_pipeline = StockPipeline()

        logger.info(
            "StockPipeline instance created successfully."
        )

        results = []

        # ========================================================
        # INGEST EACH STOCK SYMBOL
        # ========================================================

        for index, symbol in enumerate(stock_symbols, start=1):

            symbol_start_time = datetime.now(timezone.utc)

            logger.info(
                "Processing stock symbol %s/%s: %s",
                index,
                len(stock_symbols),
                symbol,
            )

            try:
                params = {
                    "function": "TIME_SERIES_WEEKLY",
                    "symbol": symbol,
                    "apikey": stock_pipeline.alpha_vantage_api_key,
                }

                logger.info(
                    "Alpha Vantage request parameters prepared. "
                    "Symbol: %s, Function: %s",
                    symbol,
                    params["function"],
                )

                # =================================================
                # FETCH DATA
                # =================================================

                logger.info(
                    "Fetching Alpha Vantage data. Symbol: %s",
                    symbol,
                )

                stock_data = stock_pipeline.extractor.fetch_from_api(
                    params=params,
                )

                logger.info(
                    "Stock data retrieved successfully. Symbol: %s",
                    symbol,
                )

                # =================================================
                # PUSH RAW DATA TO S3 BRONZE
                # =================================================

                logger.info(
                    "Starting Bronze S3 upload. Symbol: %s",
                    symbol,
                )

                s3_response = stock_pipeline.loader.upload_raw_to_s3(
                    stock_data,
                    stock_pipeline.s3_bucket_name,
                    stock_symbol=symbol,
                )

                logger.info(
                    "Bronze S3 upload completed successfully. "
                    "Symbol: %s",
                    symbol,
                )

                results.append({
                    "symbol": symbol,
                    "s3_response": s3_response,
                })

                symbol_end_time = datetime.now(timezone.utc)
                symbol_duration = (
                    symbol_end_time - symbol_start_time
                ).total_seconds()

                logger.info(
                    "Stock symbol processed successfully. "
                    "Symbol: %s, Duration: %.2f seconds",
                    symbol,
                    symbol_duration,
                )

            except Exception as e:
                symbol_end_time = datetime.now(timezone.utc)
                symbol_duration = (
                    symbol_end_time - symbol_start_time
                ).total_seconds()

                logger.exception(
                    "Error processing stock symbol %s "
                    "after %.2f seconds: %s",
                    symbol,
                    symbol_duration,
                    e,
                )

                results.append({
                    "symbol": symbol,
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

        logger.info(
            "Starting Stock ETL processing from Bronze layer."
        )

        stock_pipeline.run()

        logger.info(
            "Stock ETL processing completed successfully."
        )

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

    logger.info(
        "Running Stock Data Pipeline locally."
    )

    try:
        lambda_handler({}, {})

        logger.info(
            "Local Stock Data Pipeline execution completed."
        )

    except Exception as e:
        logger.exception(
            "Local Stock Data Pipeline execution failed: %s",
            e,
        )

        raise
