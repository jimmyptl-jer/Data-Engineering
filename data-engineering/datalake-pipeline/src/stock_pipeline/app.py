"""
Stock Data Pipeline Entry Point & Orchestrator module.

This module acts as the core orchestrator for the Alpha Vantage ETL pipeline.
It ties together the Extractor, Transformer, Loader, and Watermark components,
implementing a Medallion Architecture (Bronze -> Silver -> Gold).

Key Pipeline Phases:
  1. Ingestion: Fetches raw JSON payloads from Alpha Vantage API and lands them in S3 Bronze.
  2. Bronze-to-Silver (Daily): Extracts raw JSON, applies date-based watermark filtering,
     cleans, validates, enriches metrics, and writes Silver CSV & Parquet outputs.
  3. Bronze-to-Silver (Overview): Extracts raw JSON, transforms reference company data into Silver CSV & Parquet.
  4. Gold Build: Reads Silver Daily & Overview datasets, performs a LEFT JOIN on stock symbol,
     and writes unified business dataset to S3 Gold layer.

Supports execution both as a local standalone script and as an AWS Lambda handler.
"""

import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max

from . import config
from .extract import StockDataExtractor
from .transform import StockDataTransformer
from .load import StockDataLoader

from .utils import APIKeyManager
from .ingestion.alpha_vantage_ingestion import AlphaVantageIngestion

from src.watermark.manager import WatermarkManager

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
    """
    Create, configure, and return an active SparkSession instance.

    Configures PySpark with the Hadoop AWS package (`org.apache.hadoop:hadoop-aws:3.4.1`)
    to allow direct read/write access to AWS S3 using `s3a://` URIs.

    Returns:
        SparkSession: Ready-to-use active SparkSession.

    Raises:
        Exception: If SparkSession initialization fails.
    """
    logger.info("[INIT] Initializing Spark session for StockDataPipeline.")

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

        logger.info("[INIT] Spark session ready. Version: %s", spark.version)
        return spark

    except Exception as e:
        logger.exception("[INIT] Failed to initialize Spark session: %s", e)
        raise


# ============================================================
# SILVER LAYER WRITE HELPERS
# ============================================================

def _write_silver_parquet(df, base_path: str, dataset_name: str, execution_start_time: datetime) -> None:
    """
    Write a processed DataFrame to the Silver layer in compressed Parquet format.

    Partitioning Hierarchy:
      `{base_path}dataset={dataset_name}/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/format=parquet/`

    Args:
        df: PySpark DataFrame to write.
        base_path: Base S3 path prefix for the Silver layer.
        dataset_name: Name of the dataset (e.g., 'daily_time_series', 'company_overview').
        execution_start_time: Timestamp representing the pipeline execution start.
    """
    logger.info(
        "[SILVER][WRITE_PARQUET] Writing dataset=%s to base_path=%s",
        dataset_name,
        base_path,
    )

    (
        df.write
        .mode("overwrite")
        .parquet(
            f"{base_path}"
            f"dataset={dataset_name}/"
            f"year={execution_start_time.year}/"
            f"month={execution_start_time.month:02d}/"
            f"day={execution_start_time.day:02d}/"
            f"hour={execution_start_time.hour:02d}/"
            f"minute={execution_start_time.minute:02d}/"
            f"format=parquet/"
        )
    )

    logger.info("[SILVER][WRITE_PARQUET_OK] Parquet write completed for dataset=%s.", dataset_name)


def _write_silver_csv(df, base_path: str, dataset_name: str, execution_start_time: datetime) -> None:
    """
    Write a processed DataFrame to the Silver layer in human-readable CSV format.

    Partitioning Hierarchy:
      `{base_path}dataset={dataset_name}/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/format=csv/`

    Args:
        df: PySpark DataFrame to write.
        base_path: Base S3 path prefix for the Silver layer.
        dataset_name: Name of the dataset (e.g., 'daily_time_series', 'company_overview').
        execution_start_time: Timestamp representing the pipeline execution start.
    """
    logger.info(
        "[SILVER][WRITE_CSV] Writing dataset=%s to base_path=%s",
        dataset_name,
        base_path,
    )

    (
        df.write
        .mode("overwrite")
        .options(header=True)
        .csv(
            f"{base_path}"
            f"dataset={dataset_name}/"
            f"year={execution_start_time.year}/"
            f"month={execution_start_time.month:02d}/"
            f"day={execution_start_time.day:02d}/"
            f"hour={execution_start_time.hour:02d}/"
            f"minute={execution_start_time.minute:02d}/"
            f"format=csv/"
        )
    )

    logger.info("[SILVER][WRITE_CSV_OK] CSV write completed for dataset=%s.", dataset_name)


# ============================================================
# STOCK PIPELINE CLASS
# ============================================================

class StockPipeline:
    """
    Main ETL Orchestrator Class for the Stock Data Pipeline.

    Responsibilities:
      - Initializing pipeline dependencies (Spark, Extractors, Transformers, Loaders, Watermarks).
      - Ingesting raw API responses from Alpha Vantage to Bronze S3.
      - Executing Bronze-to-Silver ETL cycles with date-based watermark incrementality.
      - Executing Bronze-to-Silver company overview reference dataset ETL cycles.
      - Building the Gold unified dataset by joining Silver datasets.
    """

    def __init__(self):
        """Initialize the StockPipeline and verify cloud configuration."""
        logger.info("[PIPELINE] Initializing StockPipeline orchestrator.")

        try:
            # 1. Load AWS & Pipeline Config
            self.aws_access_key_id = config.AWS_ACCESS_KEY_ID
            self.aws_secret_access_key = config.AWS_SECRET_ACCESS_KEY
            self.s3_bucket_name = config.S3_BUCKET_NAME
            self.alpha_vantage_config = config.ALPHA_VANTAGE_ENDPOINTS

            self.silver_base_path = config.SILVER_BASE_PATH
            self.gold_base_path = config.GOLD_BASE_PATH

            # 2. Initialize Spark Session
            self.spark = create_spark_session()

            # 3. Instantiate Subsystems
            self.extractor = StockDataExtractor(self.spark)
            self.transformer = StockDataTransformer(self.spark)
            self.watermark_manager = WatermarkManager(self.spark)

            self.loader = StockDataLoader(
                self.aws_access_key_id,
                self.aws_secret_access_key,
            )

            self.api_key = APIKeyManager()

            self.ingestion = AlphaVantageIngestion(
                extractor=self.extractor,
                loader=self.loader,
                bucket_name=self.s3_bucket_name,
            )

            logger.info("[PIPELINE] StockPipeline initialized successfully. S3 Bucket: %s", self.s3_bucket_name)

        except Exception as e:
            logger.exception("[PIPELINE] Failed to initialize StockPipeline: %s", e)
            raise

    # ============================================================
    # INGESTION LAYER
    # ============================================================

    def _ingest_from_api(
        self,
        stock_symbols: list[str],
        execution_start_time: datetime,
        full_load: bool = False,
    ) -> list[dict]:
        """
        Fetch raw market data from Alpha Vantage API for all symbols and endpoints,
        writing raw JSON files to S3 Bronze layer.

        Full Load vs Incremental Strategy for Bronze Ingestion:
          - Initial run (or full_load=True): Uses outputsize='full' to fetch 20+ years of historical data into Bronze.
          - Subsequent runs: Uses outputsize='compact' to fetch only the latest 100 daily data points into Bronze.

        Args:
            stock_symbols: List of stock ticker symbols (e.g., ['IBM', 'AAPL']).
            execution_start_time: Pipeline execution timestamp.
            full_load: If True, forces outputsize='full' for complete historical load.

        Returns:
            List of dictionary results summarizing ingestion outcomes per symbol & function.
        """
        logger.info("[INGEST] Starting API ingestion cycle for %d symbol(s).", len(stock_symbols))

        daily_dataset = config.get_dataset_name_by_function("TIME_SERIES_DAILY")
        watermark_exists = self.watermark_manager.watermark_exists("bronze_to_silver", daily_dataset)

        # Determine payload size: full (20+ years history) if initial run or requested, compact (last 100 days) if incremental
        if full_load or not watermark_exists:
            outputsize = "full"
            logger.info("[INGEST] Mode: FULL LOAD (outputsize='full' — 20+ years historical data).")
        else:
            outputsize = "compact"
            logger.info("[INGEST] Mode: INCREMENTAL LOAD (outputsize='compact' — last 100 days data).")

        results = []

        for symbol in stock_symbols:
            for endpoint in self.alpha_vantage_config:
                function = endpoint["function"]
                dataset = endpoint["dataset"]

                try:
                    response = self.ingestion.ingest(
                        symbol,
                        function=function,
                        dataset=dataset,
                        execution_start_time=execution_start_time,
                        outputsize=outputsize,
                    )

                    results.append({
                        "symbol": symbol,
                        "function": function,
                        "outputsize": outputsize if "TIME_SERIES" in function else "N/A",
                        "response": response,
                    })

                except Exception as e:
                    logger.exception(
                        "[INGEST] Error ingesting symbol=%s via function=%s: %s",
                        symbol, function, e,
                    )
                    results.append({
                        "symbol": symbol,
                        "function": function,
                        "error": str(e),
                    })

        logger.info("[INGEST] Ingestion cycle completed. Total requests processed: %d.", len(results))
        return results

    # ============================================================
    # DAILY DATASET — Date-Based Watermark Incremental
    # ============================================================

    def _process_daily_dataset(
        self,
        execution_start_time: datetime,
        batch_id: str,
    ) -> bool:
        """
        Execute the Bronze -> Silver processing cycle for Daily Time Series data.

        Applies date-based watermark state tracking:
          - Reads existing watermark (if present) for `daily_time_series`.
          - Filters Bronze records to only include rows with `day_date > last_watermark`.
          - Transforms, cleans, validates, and enriches new records.
          - Writes output in CSV and Parquet formats to Silver S3 layer.
          - Updates the watermark JSON file with the new maximum `day_date`.

        Args:
            execution_start_time: Pipeline execution timestamp.
            batch_id: Unique batch execution identifier.

        Returns:
            bool: True if Silver output was written, False if skipped due to no new records.
        """
        daily_dataset = config.get_dataset_name_by_function("TIME_SERIES_DAILY")
        pipeline_name = "bronze_to_silver"

        logger.info("[DAILY] Starting Bronze-to-Silver cycle for dataset=%s.", daily_dataset)

        # 1. Extract raw JSON from Bronze layer into PySpark DataFrame
        extracted_daily_data = self.extractor.extract_bronze_daily_data(
            daily_dataset,
            execution_start_time=execution_start_time,
        )

        # 2. Retrieve existing watermark value (if present)
        watermark_value = None

        if self.watermark_manager.watermark_exists(pipeline_name, daily_dataset):
            daily_watermark = self.watermark_manager.read_watermark(
                pipeline_name=pipeline_name,
                dataset_name=daily_dataset,
            )
            watermark_value = daily_watermark.get("watermark_value")

            logger.info(
                "[DAILY] Existing watermark found: watermark_value=%s. Executing incremental load.",
                watermark_value,
            )
        else:
            logger.info("[DAILY] No prior watermark found. Executing full historical load.")

        # 3. Transform Bronze DataFrame to Silver schema (filters by day_date > watermark_value)
        daily_df = self.transformer.silver_transform_daily_timeseries(
            daily_dataset,
            extracted_daily_data,
            watermark_value=watermark_value,
        )

        # 4. Check if new records exist after watermark filtering
        if daily_df.isEmpty():
            logger.info("[DAILY] No new records detected after watermark filtering. Skipping Silver write.")
            return False

        # 5. Extract latest trading day date to update the watermark
        daily_latest_watermark_value = (
            daily_df
            .agg(spark_max("day_date").alias("watermark_value"))
            .first()["watermark_value"]
        )

        logger.info(
            "[DAILY] New maximum watermark value computed: %s",
            daily_latest_watermark_value,
        )

        # 6. Write Silver outputs (CSV & Parquet)
        _write_silver_csv(
            daily_df,
            self.silver_base_path,
            daily_dataset,
            execution_start_time,
        )

        _write_silver_parquet(
            daily_df,
            self.silver_base_path,
            daily_dataset,
            execution_start_time,
        )

        # 7. Persist updated watermark payload
        self.watermark_manager.write_watermark(
            watermark={
                "pipeline_name": pipeline_name,
                "dataset_name": daily_dataset,
                "watermark_column": "day_date",
                "watermark_value": daily_latest_watermark_value,
                "last_processed_at": execution_start_time.isoformat(),
                "batch_id": batch_id,
                "status": "SUCCESS",
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "updated_by": "stock_pipeline",
                "remarks": "Bronze to Silver completed successfully.",
            },
        )

        logger.info("[DAILY] Bronze-to-Silver daily processing cycle completed successfully.")
        return True

    # ============================================================
    # OVERVIEW DATASET — Hash-Based Change Detection Incremental
    # ============================================================

    def _process_overview_dataset(
        self,
        execution_start_time: datetime,
        batch_id: str,
    ) -> bool:
        """
        Execute the Bronze -> Silver processing cycle for Company Overview reference data.

        Reference data changes infrequently. The hash-based incremental strategy:
          1. Generates `overview_hash` (SHA-256 digest of business columns).
          2. Reads existing watermark from `watermark/bronze_to_silver/company_overview.json`.
          3. Compares new content hash vs existing hash.
          4. If different -> writes Silver (CSV & Parquet) and updates watermark.
          5. If identical -> skips Silver write to save storage and compute.

        Args:
            execution_start_time: Pipeline execution timestamp.
            batch_id: Unique batch execution identifier.

        Returns:
            bool: True if Silver output was written, False if skipped due to no data changes.
        """
        overview_dataset = config.get_dataset_name_by_function("OVERVIEW")
        pipeline_name = "bronze_to_silver"

        logger.info("[OVERVIEW] Starting Bronze-to-Silver cycle for dataset=%s.", overview_dataset)

        # 1. Extract raw JSON overview from Bronze layer
        extracted_overview_data = self.extractor.extract_bronze_overview_data(
            overview_dataset,
            execution_start_time=execution_start_time,
        )

        # 2. Transform raw overview JSON into typed Silver DataFrame
        overview_df = self.transformer.silver_transform_overview(
            extracted_overview_data,
        )

        # -------------------------------------------------------
        # TODO: Add hash-based change detection here:
        #   1. new_hash = compute_content_hash(overview_df)
        #   2. Read existing watermark -> get old overview_hash
        #   3. if new_hash == old_hash: skip write & return False
        # -------------------------------------------------------

        # 3. Write Silver outputs (CSV & Parquet)
        _write_silver_csv(
            overview_df,
            self.silver_base_path,
            overview_dataset,
            execution_start_time,
        )

        _write_silver_parquet(
            overview_df,
            self.silver_base_path,
            overview_dataset,
            execution_start_time,
        )

        # 4. Update Overview Watermark JSON
        self.watermark_manager.write_watermark(
            watermark={
                "pipeline_name": pipeline_name,
                "dataset_name": overview_dataset,
                "watermark_column": "overview_hash",
                "watermark_value": "",
                "overview_hash": "",
                "last_processed_at": execution_start_time.isoformat(),
                "batch_id": batch_id,
                "status": "SUCCESS",
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "updated_by": "stock_pipeline",
                "remarks": "Bronze to Silver completed. TODO: add content hash value.",
            },
        )

        logger.info("[OVERVIEW] Bronze-to-Silver overview cycle completed successfully.")
        return True

    # ============================================================
    # GOLD LAYER — Join Daily + Overview
    # ============================================================

    def _build_gold_layer(self, execution_start_time: datetime) -> None:
        """
        Rebuild the Gold layer dataset by joining Silver Daily Time Series
        and Silver Company Overview reference data on `symbol`.

        The Gold layer represents an analytics-ready unified data model,
        combining daily price movement metrics with company fundamental attributes
        (e.g., Sector, Industry, PE Ratio, Market Cap).

        Gold is always rebuilt to maintain schema and state consistency across runs.

        Args:
            execution_start_time: Pipeline execution timestamp.
        """
        daily_dataset = config.get_dataset_name_by_function("TIME_SERIES_DAILY")
        overview_dataset = config.get_dataset_name_by_function("OVERVIEW")

        logger.info("[GOLD] Starting Gold layer build from Silver datasets.")

        # 1. Parquet Path: Read Silver Parquet inputs
        logger.info("[GOLD] Extracting Silver Parquet DataFrames.")

        daily_parquet = self.extractor.extract_silver_daily_data_parquet(
            daily_dataset, execution_start_time=execution_start_time,
        )
        overview_parquet = self.extractor.extract_silver_overview_data_parquet(
            overview_dataset, execution_start_time=execution_start_time,
        )

        # Select business columns and avoid duplicate metadata timestamps
        daily_parquet = daily_parquet.select(
            "symbol", "day_date",
            "open", "high", "low", "close", "volume",
            "daily_change", "daily_change_percentage", "market_movement",
            "thirty_day_avg_open", "thirty_day_avg_close",
            "all_time_high", "all_time_low",
            "processed_at",
        )

        overview_parquet = overview_parquet.drop("processed_at")

        # Perform Left Outer Join on stock symbol
        gold_parquet_df = daily_parquet.join(
            overview_parquet, on="symbol", how="left",
        )

        # 2. CSV Path: Read Silver CSV inputs
        logger.info("[GOLD] Extracting Silver CSV DataFrames.")

        daily_csv = self.extractor.extract_silver_daily_data_csv(
            daily_dataset, "csv", execution_start_time=execution_start_time,
        )
        overview_csv = self.extractor.extract_silver_overview_data_csv(
            overview_dataset, "csv", execution_start_time=execution_start_time,
        )

        daily_csv = daily_csv.select(
            "symbol", "day_date",
            "open", "high", "low", "close", "volume",
            "daily_change", "daily_change_percentage", "market_movement",
            "thirty_day_avg_open", "thirty_day_avg_close",
            "all_time_high", "all_time_low",
            "processed_at",
        )

        overview_csv = overview_csv.drop("processed_at")

        gold_csv_df = daily_csv.join(
            overview_csv, on="symbol", how="left",
        )

        # 3. Write Gold Layer Outputs
        logger.info("[GOLD] Writing Gold layer dataset (CSV & Parquet).")

        _write_silver_csv(
            gold_csv_df,
            self.gold_base_path,
            "company_dataset",
            execution_start_time=execution_start_time,
        )

        logger.info("[GOLD] CSV write completed for dataset=company_dataset.")

        _write_silver_parquet(
            gold_parquet_df,
            self.gold_base_path,
            "company_dataset",
            execution_start_time=execution_start_time,
        )

        logger.info("[GOLD] Parquet write completed for dataset=company_dataset.")

    # ============================================================
    # PIPELINE ORCHESTRATOR
    # ============================================================

    def run(
        self,
        execution_start_time: datetime,
        stock_symbols: list[str],
        full_load: bool = False,
    ) -> list[dict]:
        """
        Execute the full end-to-end Medallion ETL pipeline:
          1. API Ingestion -> Bronze S3 JSON (Full load on initial run or if full_load=True, Incremental load otherwise)
          2. Bronze -> Silver Daily processing (date-based watermark)
          3. Bronze -> Silver Overview processing (hash-based change detection)
          4. Gold dataset construction (Daily + Overview join)

        Args:
            execution_start_time: UTC execution timestamp.
            stock_symbols: List of stock symbols to process.
            full_load: If True, forces outputsize='full' on Bronze ingestion for complete 20+ year history.

        Returns:
            List of ingestion result dictionaries.
        """
        logger.info("[PIPELINE] Starting end-to-end ETL execution (full_load=%s).", full_load)

        batch_id = f"batch_{execution_start_time.strftime('%Y%m%d_%H%M%S')}"

        try:
            # Step 1: API Ingestion -> Bronze
            results = self._ingest_from_api(
                stock_symbols,
                execution_start_time,
                full_load=full_load,
            )

            # Step 2: Bronze -> Silver Daily Time Series
            daily_written = self._process_daily_dataset(execution_start_time, batch_id)
            logger.info("[PIPELINE] Daily Silver output written: %s", daily_written)

            # Step 3: Bronze -> Silver Company Overview
            overview_written = self._process_overview_dataset(execution_start_time, batch_id)
            logger.info("[PIPELINE] Overview Silver output written: %s", overview_written)

            # Step 4: Silver -> Gold Unified Dataset
            self._build_gold_layer(execution_start_time)

            # Execution duration metric & metrics summary log
            duration = (datetime.now(timezone.utc) - execution_start_time).total_seconds()
            successful_count = len([r for r in results if "error" not in r])
            failed_count = len([r for r in results if "error" in r])

            logger.info(
                "\n"
                "============================================================\n"
                "               PIPELINE EXECUTION METRICS SUMMARY           \n"
                "============================================================\n"
                " Batch ID        : %s\n"
                " Execution Time  : %s\n"
                " Duration        : %.2f seconds\n"
                " Target Symbols  : %s\n"
                " Load Mode       : %s\n"
                " API Results     : %d Successful, %d Failed\n"
                " Silver Daily    : %s\n"
                " Silver Overview : %s\n"
                " Gold Dataset    : Rebuilt & Persisted\n"
                "============================================================",
                batch_id,
                execution_start_time.isoformat(),
                duration,
                stock_symbols,
                "FULL LOAD (20+ Years History)" if full_load else "INCREMENTAL LOAD (Last 100 Days)",
                successful_count,
                failed_count,
                "WRITTEN (New Records Found)" if daily_written else "SKIPPED (No New Records)",
                "WRITTEN" if overview_written else "SKIPPED",
            )

            return results

        except Exception:
            duration = (datetime.now(timezone.utc) - execution_start_time).total_seconds()
            logger.exception("[PIPELINE] ETL pipeline execution failed after %.2f seconds.", duration)
            raise


# ============================================================
# AWS LAMBDA HANDLER
# ============================================================

def lambda_handler(event, context):
    """
    AWS Lambda handler entry point.

    Accepts an event payload containing optional `stock_symbols` and `full_load` flag.
    Also checks `FULL_LOAD` environment variable if not passed in event payload.

    Sample Event Payload:
      {
        "stock_symbols": ["IBM", "AAPL"],
        "full_load": true
      }

    Args:
        event: Dict containing invocation event details.
        context: AWS Lambda context object.

    Returns:
        dict: Standard HTTP response dictionary with execution status and summary results.
    """
    execution_start_time = datetime.now(timezone.utc)

    logger.info(
        "[LAMBDA] Execution triggered at %s",
        execution_start_time.isoformat(),
    )

    is_dict = isinstance(event, dict)
    stock_symbols = event.get("stock_symbols", ["IBM"]) if is_dict else ["IBM"]

    # Check event dict first, fall back to FULL_LOAD environment variable
    env_full_load = os.getenv("FULL_LOAD", "false").lower() in ("true", "1", "yes")
    full_load = bool(event.get("full_load", env_full_load)) if is_dict else env_full_load

    try:
        stock_pipeline = StockPipeline()

        results = stock_pipeline.run(
            execution_start_time=execution_start_time,
            stock_symbols=stock_symbols,
            full_load=full_load,
        )

        successful = [r["symbol"] for r in results if "error" not in r]
        failed = [r["symbol"] for r in results if "error" in r]

        logger.info(
            "[LAMBDA] Execution summary — Successful symbols: %d, Failed symbols: %d",
            len(successful),
            len(failed),
        )

        if failed:
            logger.warning("[LAMBDA] Failed ingestion symbols: %s", failed)

        execution_end_time = datetime.now(timezone.utc)
        total_duration = (execution_end_time - execution_start_time).total_seconds()

        logger.info(
            "[LAMBDA] Execution completed successfully in %.2f seconds.",
            total_duration,
        )

        return {
            "statusCode": 200,
            "body": "Stock pipeline executed successfully.",
            "results": results,
        }

    except Exception as e:
        execution_end_time = datetime.now(timezone.utc)
        total_duration = (execution_end_time - execution_start_time).total_seconds()

        logger.exception(
            "[LAMBDA] Execution failed after %.2f seconds: %s",
            total_duration,
            e,
        )

        raise


# ============================================================
# LOCAL EXECUTION ENTRY POINT
# ============================================================

if __name__ == "__main__":
    import os
    env_full_load = os.getenv("FULL_LOAD", "false").lower() in ("true", "1", "yes")
    logger.info("[LOCAL] Executing Stock Data Pipeline locally (FULL_LOAD=%s).", env_full_load)

    try:
        lambda_handler({"full_load": env_full_load}, {})
        logger.info("[LOCAL] Execution completed successfully.")

    except Exception as e:
        logger.exception("[LOCAL] Execution failed: %s", e)
        raise