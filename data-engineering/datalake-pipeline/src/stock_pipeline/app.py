"""
Entry point — ties extract -> transform -> load together, and exposes
lambda_handler for AWS Lambda/SAM.

Incremental strategies:
  - Daily Time Series : date-based watermark (day_date column).
  - Company Overview  : hash-based change detection (SHA-256 of business columns).
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
    """Create and return a configured SparkSession."""
    logger.info("[INIT] Initializing Spark session.")

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

def _write_silver_parquet(df, base_path, dataset_name, execution_start_time):
    """Write a DataFrame to the Silver layer in Parquet format."""
    logger.info(
        "[SILVER][WRITE_PARQUET] dataset=%s, base_path=%s",
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


def _write_silver_csv(df, base_path, dataset_name, execution_start_time):
    """Write a DataFrame to the Silver layer in CSV format."""
    logger.info(
        "[SILVER][WRITE_CSV] dataset=%s, base_path=%s",
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
# STOCK PIPELINE
# ============================================================

class StockPipeline:
    """
    Orchestrates the full ETL pipeline:
      Ingestion -> Bronze -> Silver -> Gold

    Supports two incremental strategies:
      - Date-based watermark (Daily Time Series)
      - Hash-based change detection (Company Overview)
    """

    def __init__(self):
        logger.info("[PIPELINE] Initializing StockPipeline.")

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

            logger.info("[PIPELINE] StockPipeline initialized. S3 bucket: %s", self.s3_bucket_name)

        except Exception as e:
            logger.exception("[PIPELINE] Failed to initialize StockPipeline: %s", e)
            raise

    # ============================================================
    # INGESTION
    # ============================================================

    def _ingest_from_api(
        self,
        stock_symbols: list,
        execution_start_time: datetime,
    ) -> list:
        """
        Ingest raw data from Alpha Vantage API into the Bronze S3 layer
        for all configured endpoints and symbols.

        Returns:
            List of per-symbol/endpoint result dicts.
        """
        logger.info("[INGEST] Starting API ingestion for %d symbol(s).", len(stock_symbols))

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
                    )

                    results.append({
                        "symbol": symbol,
                        "function": function,
                        "response": response,
                    })

                except Exception as e:
                    logger.exception(
                        "[INGEST] Error ingesting symbol=%s via %s: %s",
                        symbol, function, e,
                    )
                    results.append({
                        "symbol": symbol,
                        "function": function,
                        "error": str(e),
                    })

        logger.info("[INGEST] Ingestion completed. %d result(s).", len(results))
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
        Full Bronze -> Silver cycle for the Daily Time Series dataset.

        Uses date-based watermark: only rows with day_date > last watermark
        are processed.

        Args:
            execution_start_time: Pipeline execution timestamp.
            batch_id: Current batch identifier.

        Returns:
            True if Silver was written, False if skipped (no new data).
        """
        daily_dataset = config.get_dataset_name_by_function("TIME_SERIES_DAILY")
        pipeline_name = "bronze_to_silver"

        logger.info("[DAILY] Starting Bronze-to-Silver cycle for dataset=%s.", daily_dataset)

        # --- Bronze Extract ---
        extracted_daily_data = self.extractor.extract_bronze_daily_data(
            daily_dataset,
            execution_start_time=execution_start_time,
        )

        # --- Watermark Check ---
        watermark_value = None

        if self.watermark_manager.watermark_exists(pipeline_name, daily_dataset):
            daily_watermark = self.watermark_manager.read_watermark(
                pipeline_name=pipeline_name,
                dataset_name=daily_dataset,
            )
            watermark_value = daily_watermark.get("watermark_value")

            logger.info(
                "[DAILY] Existing watermark found: watermark_value=%s. Incremental load.",
                watermark_value,
            )
        else:
            logger.info("[DAILY] No watermark found. Performing full load.")

        # --- Silver Transform (watermark filtering happens inside) ---
        daily_df = self.transformer.silver_transform_daily_timeseries(
            daily_dataset,
            extracted_daily_data,
            watermark_value=watermark_value,
        )

        if daily_df.isEmpty():
            logger.info("[DAILY] No new records after watermark filter. Skipping Silver write.")
            return False

        # --- Compute new watermark value ---
        daily_latest_watermark_value = (
            daily_df
            .agg(spark_max("day_date").alias("watermark_value"))
            .first()["watermark_value"]
        )

        logger.info(
            "[DAILY] New watermark value: %s",
            daily_latest_watermark_value,
        )

        # --- Silver Write ---
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

        # --- Watermark Write ---
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

        logger.info("[DAILY] Bronze-to-Silver cycle completed successfully.")
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
        Full Bronze -> Silver cycle for the Company Overview dataset.

        TODO: Implement hash-based change detection:
          1. Generate overview_hash (hash only business columns,
             exclude processed_at, batch_id, watermark fields).
          2. Read existing watermark from watermark/bronze_to_silver/company_overview.json.
          3. Compare new hash vs old hash.
          4. If different -> write Silver (CSV + Parquet) + update watermark.
          5. If same -> skip Silver write + skip watermark update.

        Args:
            execution_start_time: Pipeline execution timestamp.
            batch_id: Current batch identifier.

        Returns:
            True if Silver was written, False if skipped (hash unchanged).
        """
        overview_dataset = config.get_dataset_name_by_function("OVERVIEW")
        pipeline_name = "bronze_to_silver"

        logger.info("[OVERVIEW] Starting Bronze-to-Silver cycle for dataset=%s.", overview_dataset)

        # --- Bronze Extract ---
        extracted_overview_data = self.extractor.extract_bronze_overview_data(
            overview_dataset,
            execution_start_time=execution_start_time,
        )

        # --- Silver Transform ---
        overview_df = self.transformer.silver_transform_overview(
            extracted_overview_data,
        )

        # -------------------------------------------------------
        # TODO: Add hash-based change detection here.
        #
        # Steps:
        #   1. new_hash = compute hash of overview_df business columns
        #   2. Read existing watermark -> get stored overview_hash
        #   3. if new_hash == old_hash: skip Silver write, return False
        #   4. if different: proceed to Silver write below
        # -------------------------------------------------------

        # --- Silver Write ---
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

        # --- Watermark Write ---
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
                "remarks": "Bronze to Silver completed. TODO: add hash value.",
            },
        )

        logger.info("[OVERVIEW] Bronze-to-Silver cycle completed successfully.")
        return True

    # ============================================================
    # GOLD LAYER — Join Daily + Overview
    # ============================================================

    def _build_gold_layer(self, execution_start_time: datetime) -> None:
        """
        Read Silver datasets, join Daily + Overview, and write the
        Gold-layer company dataset in both CSV and Parquet.

        Gold is always rebuilt for consistency regardless of whether
        individual Silver datasets were updated in this run.
        """
        daily_dataset = config.get_dataset_name_by_function("TIME_SERIES_DAILY")
        overview_dataset = config.get_dataset_name_by_function("OVERVIEW")

        logger.info("[GOLD] Building Gold layer from Silver datasets.")

        # --- Parquet path ---
        logger.info("[GOLD] Reading Silver Parquet datasets.")

        daily_parquet = self.extractor.extract_silver_daily_data_parquet(
            daily_dataset, execution_start_time=execution_start_time,
        )
        overview_parquet = self.extractor.extract_silver_overview_data_parquet(
            overview_dataset, execution_start_time=execution_start_time,
        )

        daily_parquet = daily_parquet.select(
            "symbol", "day_date",
            "open", "high", "low", "close", "volume",
            "daily_change", "daily_change_percentage", "market_movement",
            "thirty_day_avg_open", "thirty_day_avg_close",
            "all_time_high", "all_time_low",
            "processed_at",
        )

        overview_parquet = overview_parquet.drop("processed_at")

        gold_parquet_df = daily_parquet.join(
            overview_parquet, on="symbol", how="left",
        )

        # --- CSV path ---
        logger.info("[GOLD] Reading Silver CSV datasets.")

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

        # --- Write Gold ---
        logger.info("[GOLD] Writing Gold layer.")

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

    def run(self, execution_start_time: datetime, stock_symbols: list):
        """
        Execute the full ETL pipeline:
          1. Ingest from Alpha Vantage API -> Bronze
          2. Process Daily dataset (date-based incremental) -> Silver
          3. Process Overview dataset (hash-based incremental) -> Silver
          4. Build Gold layer (join Daily + Overview)

        Args:
            execution_start_time: Pipeline execution timestamp.
            stock_symbols: List of ticker symbols to process.

        Returns:
            List of per-symbol/endpoint ingestion result dicts.
        """
        logger.info("[PIPELINE] Starting ETL pipeline.")

        batch_id = f"batch_{execution_start_time.strftime('%Y%m%d_%H%M%S')}"

        try:
            # 1. INGESTION
            results = self._ingest_from_api(stock_symbols, execution_start_time)

            # 2. DAILY — Date-Based Watermark
            daily_written = self._process_daily_dataset(execution_start_time, batch_id)
            logger.info("[PIPELINE] Daily Silver written: %s", daily_written)

            # 3. OVERVIEW — Hash-Based Change Detection
            overview_written = self._process_overview_dataset(execution_start_time, batch_id)
            logger.info("[PIPELINE] Overview Silver written: %s", overview_written)

            # 4. GOLD LAYER (always rebuild for consistency)
            self._build_gold_layer(execution_start_time)

            # 5. SUMMARY
            duration = (datetime.now(timezone.utc) - execution_start_time).total_seconds()
            logger.info("[PIPELINE] Completed successfully. Duration: %.2f seconds.", duration)

            return results

        except Exception:
            duration = (datetime.now(timezone.utc) - execution_start_time).total_seconds()
            logger.exception("[PIPELINE] Failed after %.2f seconds.", duration)
            raise


# ============================================================
# AWS LAMBDA HANDLER
# ============================================================

def lambda_handler(event, context):
    """
    AWS Lambda entry point for the Stock Data Pipeline.

    Accepts an optional ``stock_symbols`` list in the event payload.
    Defaults to ``["IBM"]`` if not provided.
    """
    execution_start_time = datetime.now(timezone.utc)

    logger.info(
        "[LAMBDA] Execution started at %s",
        execution_start_time.isoformat(),
    )

    stock_symbols = event.get("stock_symbols", ["IBM"]) if isinstance(event, dict) else ["IBM"]

    try:
        stock_pipeline = StockPipeline()

        results = stock_pipeline.run(
            execution_start_time=execution_start_time,
            stock_symbols=stock_symbols,
        )

        # Ingestion summary
        successful = [r["symbol"] for r in results if "error" not in r]
        failed = [r["symbol"] for r in results if "error" in r]

        logger.info(
            "[LAMBDA] Ingestion summary — Successful: %d, Failed: %d",
            len(successful),
            len(failed),
        )

        if failed:
            logger.warning("[LAMBDA] Failed symbols: %s", failed)

        execution_end_time = datetime.now(timezone.utc)
        total_duration = (execution_end_time - execution_start_time).total_seconds()

        logger.info(
            "[LAMBDA] Execution completed. Duration: %.2f seconds.",
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
# LOCAL EXECUTION
# ============================================================

if __name__ == "__main__":
    logger.info("[LOCAL] Running Stock Data Pipeline locally.")

    try:
        lambda_handler({}, {})
        logger.info("[LOCAL] Execution completed.")

    except Exception as e:
        logger.exception("[LOCAL] Execution failed: %s", e)
        raise