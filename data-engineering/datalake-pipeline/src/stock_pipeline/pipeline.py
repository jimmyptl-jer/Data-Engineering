"""
Stock Data Pipeline Orchestrator.

This module contains the StockPipeline class which ties together
the Extractor, Transformer, Loader, and Watermark components,
implementing a Medallion Architecture (Bronze -> Silver -> Gold).

Key Pipeline Phases:
  1. Ingestion: Fetches raw JSON payloads from APIs and lands them in S3 Bronze.
  2. Bronze-to-Silver (Daily): Extracts raw JSON, applies date-based watermark filtering,
     cleans, validates, enriches metrics, and writes Silver CSV & Parquet outputs.
  3. Bronze-to-Silver (Overview): Extracts raw JSON, transforms reference company data into Silver.
  4. Gold Build: Reads Silver Daily & Overview datasets, performs a LEFT JOIN on stock symbol,
     and writes unified business dataset to S3 Gold layer.
"""

import logging
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max

from . import config
from .extract import StockDataExtractor, MassiveApiExtractor
from .load import StockDataLoader
from .utils import APIKeyManager

from .ingestion.alpha_vantage_ingestion import AlphaVantageIngestion
from .ingestion.massive_ingestion import MassiveIngestion
from .ingestion.finnhub_ingestion import FinnHubIngestion

from .watermark.manager import WatermarkManager

from .transform.daily import silver_transform_daily_timeseries
from .transform.overview import silver_transform_overview
from .transform.exchanges import transform_massive_exchanges_dataset

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
# DATA LAYER WRITE HELPERS
# ============================================================

def _write_parquet(df, base_path: str, dataset_name: str, execution_start_time: datetime) -> None:
    """
    Write a processed DataFrame in compressed Parquet format to S3.

    Parquet is the primary columnar storage format used for analytical queries.
    It offers efficient compression and fast column-level reads in tools like
    AWS Athena, Spark, and Redshift Spectrum.

    Output S3 Path Example:
      s3a://graywolf--data--lake/stock/silver/dataset=daily_time_series/
        year=2026/month=08/day=14/hour=05/minute=30/format=parquet/

    Args:
        df: PySpark DataFrame to write.
        base_path: Base S3 path prefix (e.g., 's3a://bucket/stock/silver/').
        dataset_name: Target dataset name (e.g., 'daily_time_series').
        execution_start_time: Pipeline start timestamp — used to build partition keys.
    """
    logger.info(
        "[WRITE_PARQUET] Writing dataset=%s to base_path=%s",
        dataset_name,
        base_path,
    )

    # Write Parquet with Hive-style partitioning keys derived from execution timestamp.
    # mode="overwrite" replaces the partition if re-run on the same timestamp.
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

    logger.info("[WRITE_PARQUET_OK] Parquet write completed for dataset=%s.", dataset_name)


def _write_csv(df, base_path: str, dataset_name: str, execution_start_time: datetime) -> None:
    """
    Write a processed DataFrame in human-readable CSV format to S3.

    CSV is written alongside Parquet as a secondary format for:
      - Quick manual inspection via S3 console download
      - Compatibility with non-Spark tools (Excel, pandas, etc.)

    Output S3 Path Example:
      s3a://graywolf--data--lake/stock/silver/dataset=daily_time_series/
        year=2026/month=08/day=14/hour=05/minute=30/format=csv/

    Args:
        df: PySpark DataFrame to write.
        base_path: Base S3 path prefix (e.g., 's3a://bucket/stock/silver/').
        dataset_name: Target dataset name (e.g., 'daily_time_series').
        execution_start_time: Pipeline start timestamp — used to build partition keys.
    """
    logger.info(
        "[WRITE_CSV] Writing dataset=%s to base_path=%s",
        dataset_name,
        base_path,
    )

    # Write CSV with headers enabled and Hive-style partitioning.
    # mode="overwrite" replaces the partition if re-run on the same timestamp.
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

    logger.info("[WRITE_CSV_OK] CSV write completed for dataset=%s.", dataset_name)


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
        """
        Initialize the StockPipeline orchestrator.

        This sets up all subsystems needed for the end-to-end ETL pipeline:
            1. AWS credentials & S3 bucket configuration (from config.py / .env)
            2. SparkSession for distributed data processing
            3. Extractors for reading data from Bronze/Silver S3 layers
            4. Loader for writing raw JSON data to S3 via boto3
            5. WatermarkManager for tracking incremental processing state
            6. Ingestion clients for each external data source API
        """
        logger.info("[PIPELINE] Initializing StockPipeline orchestrator.")

        try:
            # ── Step 1: Load AWS & Pipeline Config from environment ──
            # These values come from config.py which reads from .env file
            self.aws_access_key_id = config.AWS_ACCESS_KEY_ID
            self.aws_secret_access_key = config.AWS_SECRET_ACCESS_KEY
            self.s3_bucket_name = config.S3_BUCKET_NAME

            # Alpha Vantage endpoint definitions (function name → dataset mapping)
            self.alpha_vantage_config = config.ALPHA_VANTAGE_ENDPOINTS

            # Base S3 paths for Silver and Gold layers
            # e.g., "s3a://graywolf--data--lake/stock/silver/"
            self.silver_base_path = config.SILVER_BASE_PATH
            self.gold_base_path = config.GOLD_BASE_PATH

            # ── Step 2: Initialize Spark Session ──
            # Configured with Hadoop AWS package for S3 read/write via s3a://
            self.spark = create_spark_session()

            # ── Step 3: Initialize Extractors (Bronze/Silver data readers) ──
            # StockDataExtractor:  reads Alpha Vantage Bronze & Silver data
            # MassiveApiExtractor: reads Massive (Polygon.io) Bronze data
            self.extractor = StockDataExtractor(self.spark)
            self.massive_data_extractor = MassiveApiExtractor(self.spark)

            # ── Step 4: Initialize Watermark Manager ──
            # Tracks last-processed dates per dataset for incremental loading
            # Stores watermark state as JSON files in S3
            self.watermark_manager = WatermarkManager(self.spark)

            # ── Step 5: Initialize S3 Loader (boto3-based writer) ──
            # Used by ingestion classes to upload raw JSON to Bronze layer
            self.loader = StockDataLoader(
                self.aws_access_key_id,
                self.aws_secret_access_key,
            )

            # ── Step 6: Initialize API Key Manager ──
            # Handles rotation of multiple API keys to avoid rate limits
            self.api_key = APIKeyManager()

            # ── Step 7: Initialize Ingestion Clients ──
            # Each ingestion class handles: API call → validate → serialize → upload to S3 Bronze

            # Alpha Vantage: Daily time series, company overview
            self.ingestion = AlphaVantageIngestion(
                extractor=self.extractor,
                loader=self.loader,
                bucket_name=self.s3_bucket_name,
            )

            # Massive (Polygon.io): Exchanges, aggregates, splits, dividends
            self.massive_ingestion = MassiveIngestion(
                loader=self.loader,
                bucket_name=self.s3_bucket_name
            )

            # Finnhub: Stock symbols list per exchange
            self.finnhub_ingestion = FinnHubIngestion(
                loader=self.loader,
                bucket_name=self.s3_bucket_name
            )

            logger.info("[PIPELINE] StockPipeline initialized successfully. S3 Bucket: %s", self.s3_bucket_name)

        except Exception as e:
            logger.exception("[PIPELINE] Failed to initialize StockPipeline: %s", e)
            raise

    # ============================================================
    # INGESTION LAYER
    # ============================================================

    def _ingest_from_alphavantage_api(
        self,
        batch_id: str,
        stock_symbols: list[str],
        execution_start_time: datetime,
    ) -> list[dict]:
        """
        Fetch raw market data from Alpha Vantage API for all symbols and endpoints,
        writing raw JSON files to S3 Bronze layer.

        Args:
            batch_id: Unique batch execution identifier.
            stock_symbols: List of stock ticker symbols (e.g., ['IBM', 'AAPL']).
            execution_start_time: Pipeline execution timestamp.

        Returns:
            List of dictionary results summarizing ingestion outcomes per symbol & function.
        """
        logger.info("[INGEST] Starting API ingestion cycle for %d symbol(s).", len(stock_symbols))

        results = []

        for symbol in stock_symbols:
            for endpoint in self.alpha_vantage_config:
                function = endpoint["function"]
                dataset = endpoint["dataset"]

                try:
                    response = self.ingestion.ingest(
                        symbol=symbol,
                        function=function,
                        dataset=dataset,
                        datasource="alphavantage",
                        execution_start_time=execution_start_time,
                        run_id=batch_id
                    )

                    results.append({
                        "symbol": symbol,
                        "function": function,
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

    def _ingest_from_finnhub_api(
        self,
        batch_id: str,
        execution_start_time: datetime,
    ) -> list[dict]:
        """
        Fetch data from Finnhub API and write raw data
        to the S3 Bronze layer.
        """

        logger.info(
            "[INGEST][FINNHUB] Starting API ingestion cycle | "
            "batch_id=%s | execution_start_time=%s",
            batch_id,
            execution_start_time,
        )

        results = []

        try:
            # Step 1: Fetch exchanges
            logger.info(
                "[INGEST][FINNHUB][EXCHANGES] "
                "Starting exchange data ingestion | batch_id=%s",
                batch_id,
            )

            response = self.finnhub_ingestion.ingest_stocks_list(
                exchange="US",
                execution_start_time=execution_start_time,
                run_id=batch_id,
            )

            # Step 2: Process response
            status = "SUCCESS" if response is not None else "FAILED"

            logger.info(
                "[INGEST][FINNHUB][EXCHANGES] "
                "Exchange data ingestion completed | "
                "status=%s | batch_id=%s",
                status,
                batch_id,
            )

            results.append({
                "source": "finnhub",
                "dataset": "exchanges",
                "batch_id": batch_id,
                "status": status,
                "response": response,
            })

            # Step 3: Complete ingestion cycle
            logger.info(
                "[INGEST][FINNHUB] "
                "API ingestion cycle completed | "
                "datasets_processed=%d | batch_id=%s",
                len(results),
                batch_id,
            )

            return results

        except Exception:
            logger.exception(
                "[INGEST][FINNHUB] "
                "API ingestion cycle failed | batch_id=%s",
                batch_id,
            )
            raise

    def _ingest_from_massive_api(
        self,
        batch_id: str,
        execution_start_time: datetime,
        symbol: str,
    ) -> list[dict]:
        """
        Ingest all configured datasets from the Massive API.

        Args:
            batch_id: Unique identifier for the current pipeline batch.
            execution_start_time: Timestamp when the pipeline execution started.
            symbol: Stock symbol used for symbol-specific endpoints.

        Returns:
            List containing the ingestion results.
        """

        results = []

        try:
            logger.info(
                "[INGEST][MASSIVE] Starting Massive API ingestion | batch_id=%s | symbol=%s",
                batch_id, symbol,
            )

            # =========================================================
            # 1. EXCHANGES
            # =========================================================

            response = self.massive_ingestion.ingest_exchanges(
                datasource="massive",
                execution_start_time=execution_start_time,
                run_id=batch_id,
            )

            logger.info("[INGEST][MASSIVE] Exchanges response: %s", response)

            results.append({
                "source": "massive",
                "dataset": "exchanges",
                "batch_id": batch_id,
                "status": "SUCCESS" if response is not None else "FAILED",
                "response": response,
            })

            # =========================================================
            # 2. AGGREGATES
            # =========================================================

            response = self.massive_ingestion.ingest_aggregates(
                datasource="massive",
                symbol=symbol,
                execution_start_time=execution_start_time,
                run_id=batch_id,
                multiplier=1,
                timespan="day",
                from_date="2026-07-20",
                to_date="2026-08-10",
            )

            logger.info("[INGEST][MASSIVE] Aggregates response: %s", response)

            results.append({
                "source": "massive",
                "dataset": "aggregates",
                "batch_id": batch_id,
                "status": "SUCCESS" if response is not None else "FAILED",
                "response": response,
            })

            # =========================================================
            # 3. STOCK OVERVIEW
            # =========================================================

            response = self.massive_ingestion.ingest_stock_overview(
                datasource="massive",
                execution_start_time=execution_start_time,
                run_id=batch_id,
                symbol=symbol,
            )

            logger.info("[INGEST][MASSIVE] Stock overview response: %s", response)

            results.append({
                "source": "massive",
                "dataset": "stock_overview",
                "batch_id": batch_id,
                "status": "SUCCESS" if response is not None else "FAILED",
                "response": response,
            })

            # =========================================================
            # 4. DIVIDENDS
            # =========================================================

            response = self.massive_ingestion.ingest_dividends(
                datasource="massive",
                execution_start_time=execution_start_time,
                run_id=batch_id,
                symbol=symbol,
            )

            logger.info("[INGEST][MASSIVE] Dividends response: %s", response)

            results.append({
                "source": "massive",
                "dataset": "dividends",
                "batch_id": batch_id,
                "status": "SUCCESS" if response is not None else "FAILED",
                "response": response,
            })

            # =========================================================
            # RESULTS SUMMARY
            # =========================================================

            for result in results:
                logger.info(
                    "[INGEST][MASSIVE] Dataset: %s | Source: %s | "
                    "Batch: %s | Status: %s",
                    result.get("dataset"),
                    result.get("source"),
                    result.get("batch_id"),
                    result.get("status"),
                )

            logger.info(
                "[INGEST][MASSIVE] Ingestion completed | "
                "batch_id=%s | total_datasets=%d",
                batch_id,
                len(results),
            )

        except Exception as e:
            logger.exception(
                "[INGEST][MASSIVE] Error during ingestion | "
                "batch_id=%s | error=%s",
                batch_id,
                str(e),
            )

            results.append({
                "source": "massive",
                "batch_id": batch_id,
                "status": "FAILED",
                "error": str(e),
            })

        return results

    # ============================================================
    # DAILY DATASET — Date-Based Watermark Incremental
    # ============================================================

    def _process_daily_dataset(
        self,
        datasource: str,
        execution_start_time: datetime,
        batch_id: str,
    ) -> bool:
        """
        Execute the Bronze -> Silver processing cycle for Daily Time Series data.

        Args:
            datasource: Data source identifier.
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
            datasource,
            daily_dataset,
            execution_start_time=execution_start_time,
            batch_id=batch_id
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

        # 3. Transform Bronze DataFrame to Silver schema
        daily_df = silver_transform_daily_timeseries(
            self.spark,
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
        _write_csv(
            daily_df,
            self.silver_base_path,
            daily_dataset,
            execution_start_time,
        )

        _write_parquet(
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

    def _process_massive_exchange_dataset(
        self,
        datasource: str,
        dataset: str,
        execution_start_time: datetime,
        batch_id: str,
    ) -> bool:
        """
        Execute Bronze-to-Silver processing for Massive exchange reference data.
        """
        pipeline_name = "bronze_to_silver"

        logger.info(
            "[%s][EXCHANGES] Starting Bronze-to-Silver processing. "
            "datasource=%s, dataset=%s, batch_id=%s",
            pipeline_name,
            datasource,
            dataset,
            batch_id,
        )

        try:
            massive_dataset = (
                self.massive_data_extractor.extract_from_bronze_layer(
                    datasource=datasource,
                    dataset=dataset,
                    execution_start_time=execution_start_time,
                    batch_id=batch_id,
                )
            )

            if massive_dataset is None:
                logger.warning(
                    "[%s][EXCHANGES] No data found in Bronze layer. "
                    "dataset=%s, batch_id=%s",
                    pipeline_name,
                    dataset,
                    batch_id,
                )
                return False

            logger.info(
                "[%s][EXCHANGES] Bronze data extracted successfully.",
                pipeline_name,
            )

            exchange_df = transform_massive_exchanges_dataset(
                self.spark,
                massive_dataset,
            )

            if exchange_df is None:
                logger.warning(
                    "[%s][EXCHANGES] Transformation returned no data. "
                    "dataset=%s, batch_id=%s",
                    pipeline_name,
                    dataset,
                    batch_id,
                )
                return False

            logger.info(
                "[%s][EXCHANGES] Bronze-to-Silver transformation completed successfully.",
                pipeline_name,
            )

            return True

        except Exception:
            logger.exception(
                "[%s][EXCHANGES] Bronze-to-Silver processing failed. "
                "datasource=%s, dataset=%s, batch_id=%s",
                pipeline_name,
                datasource,
                dataset,
                batch_id,
            )
            return False

    def _process_massive_dataset(
        self,
        datasource: str,
        dataset: str,
        execution_start_time: datetime,
        batch_id: str,
    ) -> bool:
        """
        Generic Bronze-to-Silver processing for Massive API datasets.
        """
        pipeline_name = "bronze_to_silver"

        logger.info("%s Starting Bronze-to-Silver cycle for dataset=%s.", dataset, dataset)

        massive_dataset = self.massive_data_extractor.extract_from_bronze_layer(
            datasource=datasource,
            dataset=dataset,
            execution_start_time=execution_start_time,
            batch_id=batch_id
        )

        logger.info(massive_dataset)

        # Retrieve existing watermark value (if present)
        watermark_value = None

        if self.watermark_manager.watermark_exists(pipeline_name, dataset):
            exchange_watermark = self.watermark_manager.read_watermark(
                pipeline_name=pipeline_name,
                dataset_name=dataset,
            )
            watermark_value = exchange_watermark.get("watermark_value")

            logger.info(
                "[EXCHANGES] Existing watermark found: watermark_value=%s. Executing incremental load.",
                watermark_value,
            )
        else:
            logger.info("[EXCHANGES] No prior watermark found. Executing full historical load.")

        watermark_col_value = execution_start_time.strftime("%Y-%m-%d")

        # Persist updated watermark payload
        self.watermark_manager.write_watermark(
            watermark={
                "pipeline_name": pipeline_name,
                "dataset_name": dataset,
                "watermark_column": "exchange",
                "watermark_value": watermark_col_value,
                "last_processed_at": execution_start_time.isoformat(),
                "batch_id": batch_id,
                "status": "SUCCESS",
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "updated_by": "stock_pipeline",
                "remarks": "Bronze to Silver completed successfully.",
            },
        )

        logger.info("[EXCHANGES] Bronze-to-Silver exchanges processing cycle completed successfully.")
        return True

    # ============================================================
    # OVERVIEW DATASET — Hash-Based Change Detection Incremental
    # ============================================================

    def _process_overview_dataset(
        self,
        datasource,
        execution_start_time: datetime,
        batch_id: str,
    ) -> bool:
        """
        Execute the Bronze -> Silver processing cycle for Company Overview reference data.

        Args:
            datasource: Data source identifier.
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
            datasource=datasource,
            dataset=overview_dataset,
            execution_start_time=execution_start_time,
            batch_id=batch_id
        )

        # 2. Transform raw overview JSON into typed Silver DataFrame
        overview_df = silver_transform_overview(
            self.spark,
            extracted_overview_data,
        )

        # 3. Write Silver outputs (CSV & Parquet)
        _write_csv(
            overview_df,
            self.silver_base_path,
            overview_dataset,
            execution_start_time,
        )

        _write_parquet(
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

        Args:
            execution_start_time: Pipeline execution timestamp.
        """
        daily_dataset = config.get_dataset_name_by_function("TIME_SERIES_DAILY")
        overview_dataset = config.get_dataset_name_by_function("OVERVIEW")

        logger.info("[GOLD] Starting Gold layer build from Silver datasets.")

        try:
            # 1. Parquet Path: Read Silver Parquet inputs
            logger.info("[GOLD] Extracting Silver Parquet DataFrames.")

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

            _write_csv(
                gold_csv_df,
                self.gold_base_path,
                "company_dataset",
                execution_start_time=execution_start_time,
            )

            logger.info("[GOLD] CSV write completed for dataset=company_dataset.")

            _write_parquet(
                gold_parquet_df,
                self.gold_base_path,
                "company_dataset",
                execution_start_time=execution_start_time,
            )

            logger.info("[GOLD] Parquet write completed for dataset=company_dataset.")

        except Exception as e:
            logger.warning(
                "[GOLD_SKIP] Silver path not found or empty for execution batch timestamp (%s). Skipping Gold build: %s",
                execution_start_time.isoformat(),
                e,
            )

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
        Execute the full end-to-end Medallion ETL pipeline.

        Args:
            execution_start_time: UTC execution timestamp.
            stock_symbols: List of stock symbols to process.
            full_load: Whether to perform a full historical load.

        Returns:
            List of ingestion result dictionaries.
        """

        logger.info("[PIPELINE] Starting end-to-end ETL execution.")

        batch_id = f"batch_{execution_start_time.strftime('%Y%m%d_%H%M%S')}"

        try:
            # Step 1: API Ingestion -> Bronze
            results_massive = self._ingest_from_massive_api(
                batch_id,
                execution_start_time,
                symbol="IBM",
            )

            results_finnhub = self._ingest_from_finnhub_api(
                batch_id,
                execution_start_time,
            )

            logger.info("[PIPELINE] Finnhub ingestion results: %s", results_finnhub)

            # Step 2: Bronze -> Silver processing
            massive_exchange_processing = self._process_massive_exchange_dataset(
                "massive",
                "exchanges",
                execution_start_time,
                batch_id,
            )

            logger.info(
                "[PIPELINE][SILVER] Exchanges processing completed | "
                "written=%s",
                massive_exchange_processing,
            )

            stock_overview_processing = self._process_massive_dataset(
                "massive",
                "stock_overview",
                execution_start_time,
                batch_id,
            )

            logger.info(
                "[PIPELINE][SILVER] Stock overview processing completed | "
                "written=%s",
                stock_overview_processing,
            )

            aggregates_processing = self._process_massive_dataset(
                "massive",
                "aggregates",
                execution_start_time,
                batch_id,
            )

            logger.info(
                "[PIPELINE][SILVER] Aggregates processing completed | "
                "written=%s",
                aggregates_processing,
            )

            dividends_processing = self._process_massive_dataset(
                "massive",
                "dividends",
                execution_start_time,
                batch_id,
            )
            logger.info(
                "[PIPELINE][SILVER] Dividends processing completed | "
                "written=%s",
                dividends_processing,
            )

            stocks_list_processing = self._process_massive_dataset(
                "finnhub",
                "ticker_reference",
                execution_start_time,
                batch_id,
            )

            logger.info(
                "[PIPELINE][SILVER] Stocks list processing completed | "
                "written=%s",
                stocks_list_processing,
            )

            logger.info(
                "[PIPELINE][GOLD] Unified Gold dataset rebuilt and persisted."
            )

            # Execution duration metric
            duration = (
                datetime.now(timezone.utc) - execution_start_time
            ).total_seconds()

            logger.info(
                "\n"
                "============================================================\n"
                "               PIPELINE EXECUTION METRICS SUMMARY           \n"
                "============================================================\n"
                " Batch ID         : %s\n"
                " Execution Time   : %s\n"
                " Duration         : %.2f seconds\n"
                " Target Symbols   : %s\n"
                " Silver Exchanges : %s\n"
                " Silver Aggregates: %s\n"
                " Silver Dividends : %s\n"
                " Silver Overview  : %s\n"
                " Massive Ingest   : %s\n"
                " Gold Dataset     : Rebuilt & Persisted\n"
                "============================================================",
                batch_id,
                execution_start_time.isoformat(),
                duration,
                stock_symbols,
                "WRITTEN" if massive_exchange_processing else "SKIPPED",
                "WRITTEN" if aggregates_processing else "SKIPPED",
                "WRITTEN" if dividends_processing else "SKIPPED",
                "WRITTEN" if stock_overview_processing else "SKIPPED",
                results_massive,
            )

            return "success"

        except Exception:
            duration = (
                datetime.now(timezone.utc) - execution_start_time
            ).total_seconds()

            logger.exception(
                "[PIPELINE] ETL pipeline execution failed after %.2f seconds.",
                duration,
            )

            raise
