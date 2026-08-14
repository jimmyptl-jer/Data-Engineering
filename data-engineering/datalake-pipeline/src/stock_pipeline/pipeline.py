"""
Stock Data Pipeline Orchestrator.

This module contains the StockPipeline class which ties together
the Extractor, Transformer, Loader, and Watermark components,
implementing a Medallion Architecture (Bronze -> Silver -> Gold).

Key Pipeline Phases:
    1. Ingestion:
       Fetches raw JSON payloads from APIs and lands them in S3 Bronze.

    2. Bronze-to-Silver:
       Extracts raw data, applies transformations, validations,
       watermark processing, and writes Silver outputs.

    3. Gold Build:
       Reads Silver datasets, joins business datasets,
       and writes the unified dataset to S3 Gold.
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
from .transform.stock_tickers import transform_finnhub_stock_tickers_dataset


logger = logging.getLogger(__name__)


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session() -> SparkSession:
    """
    Create and configure the SparkSession used by the pipeline.

    Configures Hadoop AWS support so Spark can read and write
    data directly from S3 using s3a:// URIs.

    Returns:
        SparkSession: Ready-to-use SparkSession.

    Raises:
        Exception: If SparkSession initialization fails.
    """

    logger.info(
        "[INIT][SPARK] Initializing Spark session."
    )

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
            "[INIT][SPARK] Spark session initialized | version=%s",
            spark.version,
        )

        return spark

    except Exception:
        logger.exception(
            "[INIT][SPARK] Failed to initialize Spark session."
        )
        raise


# ============================================================
# SILVER / GOLD — PARQUET WRITER
# ============================================================

def _write_parquet(
    df,
    datasource: str,
    base_path: str,
    dataset_name: str,
    execution_start_time: datetime,
) -> None:
    """
    Write a processed DataFrame to S3 in Parquet format.

    Output is partitioned using the pipeline execution timestamp.
    """

    logger.info(
        "[WRITE][PARQUET] Writing dataset | "
        "datasource=%s | dataset=%s",
        datasource,
        dataset_name,
    )

    try:
        (
            df.write
            .mode("overwrite")
            .parquet(
                f"{base_path}"
                f"datasource={datasource}/"
                f"dataset={dataset_name}/"
                f"year={execution_start_time.year}/"
                f"month={execution_start_time.month:02d}/"
                f"day={execution_start_time.day:02d}/"
                f"hour={execution_start_time.hour:02d}/"
                f"minute={execution_start_time.minute:02d}/"
                f"format=parquet/"
            )
        )

        logger.info(
            "[WRITE][PARQUET] Write completed | "
            "datasource=%s | dataset=%s",
            datasource,
            dataset_name,
        )

    except Exception:
        logger.exception(
            "[WRITE][PARQUET] Write failed | "
            "datasource=%s | dataset=%s",
            datasource,
            dataset_name,
        )
        raise


# ============================================================
# SILVER / GOLD — CSV WRITER
# ============================================================

def _write_csv(
    df,
    datasource: str,
    base_path: str,
    dataset_name: str,
    execution_start_time: datetime,
) -> None:
    """
    Write a processed DataFrame to S3 in CSV format.

    CSV is written alongside Parquet for manual inspection
    and compatibility with non-Spark tools.
    """

    logger.info(
        "[WRITE][CSV] Writing dataset | "
        "datasource=%s | dataset=%s",
        datasource,
        dataset_name,
    )

    try:
        (
            df.write
            .mode("overwrite")
            .options(header=True)
            .csv(
                f"{base_path}"
                f"datasource={datasource}/"
                f"dataset={dataset_name}/"
                f"year={execution_start_time.year}/"
                f"month={execution_start_time.month:02d}/"
                f"day={execution_start_time.day:02d}/"
                f"hour={execution_start_time.hour:02d}/"
                f"minute={execution_start_time.minute:02d}/"
                f"format=csv/"
            )
        )

        logger.info(
            "[WRITE][CSV] Write completed | "
            "datasource=%s | dataset=%s",
            datasource,
            dataset_name,
        )

    except Exception:
        logger.exception(
            "[WRITE][CSV] Write failed | "
            "datasource=%s | dataset=%s",
            datasource,
            dataset_name,
        )
        raise


# ============================================================
# STOCK PIPELINE — ORCHESTRATOR
# ============================================================

class StockPipeline:
    """
    Main ETL orchestrator for the Stock Data Pipeline.

    Responsibilities:
        - Initialize pipeline dependencies.
        - Ingest external API data into Bronze.
        - Process Bronze data into Silver.
        - Manage incremental watermarks.
        - Build the Gold unified dataset.
    """

    # ========================================================
    # PIPELINE INITIALIZATION
    # ========================================================

    def __init__(self):
        """
        Initialize the StockPipeline and all required dependencies.

        Initializes:
            - AWS configuration
            - SparkSession
            - Bronze/Silver extractors
            - S3 loader
            - Watermark manager
            - API key manager
            - External API ingestion clients
        """

        logger.info(
            "[PIPELINE][INIT] Initializing StockPipeline."
        )

        try:
            self.aws_access_key_id = config.AWS_ACCESS_KEY_ID
            self.aws_secret_access_key = config.AWS_SECRET_ACCESS_KEY
            self.s3_bucket_name = config.S3_BUCKET_NAME

            self.alpha_vantage_config = (
                config.ALPHA_VANTAGE_ENDPOINTS
            )

            self.silver_base_path = config.SILVER_BASE_PATH
            self.gold_base_path = config.GOLD_BASE_PATH

            self.spark = create_spark_session()

            self.extractor = StockDataExtractor(
                self.spark
            )

            self.massive_data_extractor = MassiveApiExtractor(
                self.spark
            )

            self.watermark_manager = WatermarkManager(
                self.spark
            )

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

            self.massive_ingestion = MassiveIngestion(
                loader=self.loader,
                bucket_name=self.s3_bucket_name,
            )

            self.finnhub_ingestion = FinnHubIngestion(
                loader=self.loader,
                bucket_name=self.s3_bucket_name,
            )

            logger.info(
                "[PIPELINE][INIT] StockPipeline initialized successfully | "
                "bucket=%s",
                self.s3_bucket_name,
            )

        except Exception:
            logger.exception(
                "[PIPELINE][INIT] Failed to initialize StockPipeline."
            )
            raise

    # ========================================================
    # INGESTION — ALPHA VANTAGE → BRONZE
    # ========================================================

    def _ingest_from_alphavantage_api(
        self,
        batch_id: str,
        stock_symbols: list[str],
        execution_start_time: datetime,
    ) -> list[dict]:
        """
        Fetch Alpha Vantage data and write raw responses to Bronze.

        Processes every configured endpoint for every stock symbol.

        Returns:
            List of ingestion results.
        """

        logger.info(
            "[INGEST][ALPHAVANTAGE] Starting ingestion | "
            "batch_id=%s | symbols=%d",
            batch_id,
            len(stock_symbols),
        )

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
                        run_id=batch_id,
                    )

                    results.append(
                        {
                            "symbol": symbol,
                            "function": function,
                            "response": response,
                        }
                    )

                except Exception as e:
                    logger.exception(
                        "[INGEST][ALPHAVANTAGE] Ingestion failed | "
                        "symbol=%s | function=%s | batch_id=%s",
                        symbol,
                        function,
                        batch_id,
                    )

                    results.append(
                        {
                            "symbol": symbol,
                            "function": function,
                            "error": str(e),
                        }
                    )

        logger.info(
            "[INGEST][ALPHAVANTAGE] Ingestion completed | "
            "batch_id=%s | requests=%d",
            batch_id,
            len(results),
        )

        return results

    # ========================================================
    # INGESTION — FINNHUB → BRONZE
    # ========================================================

    def _ingest_from_finnhub_api(
        self,
        batch_id: str,
        execution_start_time: datetime,
    ) -> list[dict]:
        """
        Fetch Finnhub data and write raw responses to Bronze.
        """

        logger.info(
            "[INGEST][FINNHUB] Starting ingestion | "
            "batch_id=%s",
            batch_id,
        )

        results = []

        try:
            response = self.finnhub_ingestion.ingest_stocks_list(
                exchange="US",
                execution_start_time=execution_start_time,
                run_id=batch_id,
            )

            status = (
                "SUCCESS"
                if response is not None
                else "FAILED"
            )

            results.append(
                {
                    "source": "finnhub",
                    "dataset": "exchanges",
                    "batch_id": batch_id,
                    "status": status,
                    "response": response,
                }
            )

            logger.info(
                "[INGEST][FINNHUB] Ingestion completed | "
                "batch_id=%s | status=%s | datasets=%d",
                batch_id,
                status,
                len(results),
            )

            return results

        except Exception:
            logger.exception(
                "[INGEST][FINNHUB] Ingestion failed | "
                "batch_id=%s",
                batch_id,
            )
            raise

    # ========================================================
    # INGESTION — MASSIVE → BRONZE
    # ========================================================

    def _ingest_from_massive_api(
        self,
        batch_id: str,
        execution_start_time: datetime,
        symbol: str,
    ) -> list[dict]:
        """
        Ingest configured datasets from the Massive API.

        Datasets:
            - exchanges
            - aggregates
            - stock overview
            - dividends
        """

        logger.info(
            "[INGEST][MASSIVE] Starting ingestion | "
            "batch_id=%s | symbol=%s",
            batch_id,
            symbol,
        )

        results = []

        try:
            response = self.massive_ingestion.ingest_exchanges(
                datasource="massive",
                execution_start_time=execution_start_time,
                run_id=batch_id,
            )

            results.append(
                {
                    "source": "massive",
                    "dataset": "exchanges",
                    "batch_id": batch_id,
                    "status": (
                        "SUCCESS"
                        if response is not None
                        else "FAILED"
                    ),
                    "response": response,
                }
            )

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

            results.append(
                {
                    "source": "massive",
                    "dataset": "aggregates",
                    "batch_id": batch_id,
                    "status": (
                        "SUCCESS"
                        if response is not None
                        else "FAILED"
                    ),
                    "response": response,
                }
            )

            response = self.massive_ingestion.ingest_stock_overview(
                datasource="massive",
                execution_start_time=execution_start_time,
                run_id=batch_id,
                symbol=symbol,
            )

            results.append(
                {
                    "source": "massive",
                    "dataset": "stock_overview",
                    "batch_id": batch_id,
                    "status": (
                        "SUCCESS"
                        if response is not None
                        else "FAILED"
                    ),
                    "response": response,
                }
            )

            response = self.massive_ingestion.ingest_dividends(
                datasource="massive",
                execution_start_time=execution_start_time,
                run_id=batch_id,
                symbol=symbol,
            )

            results.append(
                {
                    "source": "massive",
                    "dataset": "dividends",
                    "batch_id": batch_id,
                    "status": (
                        "SUCCESS"
                        if response is not None
                        else "FAILED"
                    ),
                    "response": response,
                }
            )

            logger.info(
                "[INGEST][MASSIVE] Ingestion completed | "
                "batch_id=%s | datasets=%d",
                batch_id,
                len(results),
            )

        except Exception as e:
            logger.exception(
                "[INGEST][MASSIVE] Ingestion failed | "
                "batch_id=%s | error=%s",
                batch_id,
                str(e),
            )

            results.append(
                {
                    "source": "massive",
                    "batch_id": batch_id,
                    "status": "FAILED",
                    "error": str(e),
                }
            )

        return results

    # ========================================================
    # SILVER — DAILY TIME SERIES
    # ========================================================

    def _process_daily_dataset(
        self,
        datasource: str,
        execution_start_time: datetime,
        batch_id: str,
    ) -> bool:
        """
        Process Daily Time Series data from Bronze to Silver.

        Flow:
            1. Extract Bronze data.
            2. Read existing watermark.
            3. Transform and apply watermark filtering.
            4. Check for new records.
            5. Calculate latest watermark.
            6. Write CSV and Parquet.
            7. Persist updated watermark.
        """

        daily_dataset = config.get_dataset_name_by_function(
            "TIME_SERIES_DAILY"
        )

        pipeline_name = "bronze_to_silver"

        logger.info(
            "[SILVER][DAILY] Starting processing | "
            "dataset=%s | batch_id=%s",
            daily_dataset,
            batch_id,
        )

        extracted_daily_data = (
            self.extractor.extract_bronze_daily_data(
                datasource,
                daily_dataset,
                execution_start_time=execution_start_time,
                batch_id=batch_id,
            )
        )

        watermark_value = None

        if self.watermark_manager.watermark_exists(
            pipeline_name,
            daily_dataset,
        ):
            daily_watermark = (
                self.watermark_manager.read_watermark(
                    pipeline_name=pipeline_name,
                    dataset_name=daily_dataset,
                )
            )

            watermark_value = daily_watermark.get(
                "watermark_value"
            )

            logger.info(
                "[SILVER][DAILY] Existing watermark found | "
                "dataset=%s | watermark=%s",
                daily_dataset,
                watermark_value,
            )

        else:
            logger.info(
                "[SILVER][DAILY] No watermark found | "
                "dataset=%s | mode=FULL_LOAD",
                daily_dataset,
            )

        daily_df = silver_transform_daily_timeseries(
            self.spark,
            daily_dataset,
            extracted_daily_data,
            watermark_value=watermark_value,
        )

        if daily_df.isEmpty():
            logger.info(
                "[SILVER][DAILY] No new records | "
                "dataset=%s | batch_id=%s",
                daily_dataset,
                batch_id,
            )
            return False

        daily_latest_watermark_value = (
            daily_df
            .agg(
                spark_max("day_date").alias(
                    "watermark_value"
                )
            )
            .first()["watermark_value"]
        )

        logger.info(
            "[SILVER][DAILY] Latest watermark calculated | "
            "dataset=%s | watermark=%s",
            daily_dataset,
            daily_latest_watermark_value,
        )

        _write_csv(
            daily_df,
            "alphavantage",
            self.silver_base_path,
            daily_dataset,
            execution_start_time,
        )

        _write_parquet(
            daily_df,
            "alphavantage",
            self.silver_base_path,
            daily_dataset,
            execution_start_time,
        )

        self.watermark_manager.write_watermark(
            watermark={
                "pipeline_name": pipeline_name,
                "dataset_name": daily_dataset,
                "watermark_column": "day_date",
                "watermark_value": daily_latest_watermark_value,
                "last_processed_at": execution_start_time.isoformat(),
                "batch_id": batch_id,
                "status": "SUCCESS",
                "updated_at": datetime.now(
                    timezone.utc
                ).isoformat(),
                "updated_by": "stock_pipeline",
                "remarks": (
                    "Bronze to Silver completed successfully."
                ),
            },
        )

        logger.info(
            "[SILVER][DAILY] Processing completed | "
            "dataset=%s | batch_id=%s",
            daily_dataset,
            batch_id,
        )

        return True

    # ========================================================
    # SILVER — MASSIVE EXCHANGES
    # ========================================================

    def _process_massive_exchange_dataset(
        self,
        datasource: str,
        dataset: str,
        execution_start_time: datetime,
        batch_id: str,
    ) -> bool:
        """
        Process Massive exchange reference data from Bronze to Silver.
        """

        pipeline_name = "bronze_to_silver"

        logger.info(
            "[SILVER][EXCHANGES] Starting processing | "
            "datasource=%s | dataset=%s | batch_id=%s",
            datasource,
            dataset,
            batch_id,
        )

        try:
            exchange_dataset = (
                self.massive_data_extractor.extract_from_bronze_layer(
                    datasource=datasource,
                    dataset=dataset,
                    execution_start_time=execution_start_time,
                    batch_id=batch_id,
                )
            )

            if exchange_dataset is None:
                logger.warning(
                    "[SILVER][EXCHANGES] No Bronze data found | "
                    "dataset=%s | batch_id=%s",
                    dataset,
                    batch_id,
                )
                return False

            exchange_df = transform_massive_exchanges_dataset(
                self.spark,
                exchange_dataset,
            )

            if exchange_df is None:
                logger.warning(
                    "[SILVER][EXCHANGES] Transformation returned no data | "
                    "dataset=%s | batch_id=%s",
                    dataset,
                    batch_id,
                )
                return False

            _write_csv(
                exchange_df,
                datasource,
                self.silver_base_path,
                dataset,
                execution_start_time,
            )

            _write_parquet(
                exchange_df,
                datasource,
                self.silver_base_path,
                dataset,
                execution_start_time,
            )

            logger.info(
                "[SILVER][EXCHANGES] Processing completed | "
                "dataset=%s | batch_id=%s",
                dataset,
                batch_id,
            )

            return True

        except Exception:
            logger.exception(
                "[SILVER][EXCHANGES] Processing failed | "
                "datasource=%s | dataset=%s | batch_id=%s",
                datasource,
                dataset,
                batch_id,
            )
            return False

    # ========================================================
    # SILVER — MASSIVE DATASETS
    # ========================================================

    def _process_massive_dataset(
        self,
        datasource: str,
        dataset: str,
        execution_start_time: datetime,
        batch_id: str,
    ) -> bool:
        """
        Process a generic Massive dataset from Bronze to Silver.

        This function is currently responsible for extracting the
        Bronze dataset and updating its watermark.
        """

        pipeline_name = "bronze_to_silver"

        logger.info(
            "[SILVER][MASSIVE] Starting processing | "
            "dataset=%s | batch_id=%s",
            dataset,
            batch_id,
        )

        massive_dataset = (
            self.massive_data_extractor.extract_from_bronze_layer(
                datasource=datasource,
                dataset=dataset,
                execution_start_time=execution_start_time,
                batch_id=batch_id,
            )
        )

        watermark_value = None

        if self.watermark_manager.watermark_exists(
            pipeline_name,
            dataset,
        ):
            dataset_watermark = (
                self.watermark_manager.read_watermark(
                    pipeline_name=pipeline_name,
                    dataset_name=dataset,
                )
            )

            watermark_value = dataset_watermark.get(
                "watermark_value"
            )

            logger.info(
                "[SILVER][MASSIVE] Existing watermark found | "
                "dataset=%s | watermark=%s",
                dataset,
                watermark_value,
            )

        else:
            logger.info(
                "[SILVER][MASSIVE] No watermark found | "
                "dataset=%s | mode=FULL_LOAD",
                dataset,
            )

        watermark_col_value = (
            execution_start_time.strftime("%Y-%m-%d")
        )

        self.watermark_manager.write_watermark(
            watermark={
                "pipeline_name": pipeline_name,
                "dataset_name": dataset,
                "watermark_column": "exchange",
                "watermark_value": watermark_col_value,
                "last_processed_at": execution_start_time.isoformat(),
                "batch_id": batch_id,
                "status": "SUCCESS",
                "updated_at": datetime.now(
                    timezone.utc
                ).isoformat(),
                "updated_by": "stock_pipeline",
                "remarks": (
                    "Bronze to Silver completed successfully."
                ),
            },
        )

        logger.info(
            "[SILVER][MASSIVE] Processing completed | "
            "dataset=%s | batch_id=%s",
            dataset,
            batch_id,
        )

        return True

    # ========================================================
    # SILVER — FINNHUB TICKER REFERENCE
    # ========================================================

    def _process_finnhub_dataset(
        self,
        datasource: str,
        dataset: str,
        execution_start_time: datetime,
        batch_id: str,
    ) -> bool:
        """
        Process Finnhub ticker reference data from Bronze to Silver.

        Flow:
            1. Extract Bronze data.
            2. Check existing watermark.
            3. Transform ticker reference data.
            4. Write Silver CSV and Parquet.
            5. Persist updated watermark.
        """

        pipeline_name = "bronze_to_silver"

        logger.info(
            "[SILVER][FINNHUB] Starting processing | "
            "dataset=%s | batch_id=%s",
            dataset,
            batch_id,
        )

        extract_finnhub_dataset = (
            self.massive_data_extractor.extract_from_bronze_layer(
                datasource=datasource,
                dataset=dataset,
                execution_start_time=execution_start_time,
                batch_id=batch_id,
            )
        )

        watermark_value = None

        if self.watermark_manager.watermark_exists(
            pipeline_name,
            dataset,
        ):
            finnhub_watermark = (
                self.watermark_manager.read_watermark(
                    pipeline_name=pipeline_name,
                    dataset_name=dataset,
                )
            )

            watermark_value = finnhub_watermark.get(
                "watermark_value"
            )

            logger.info(
                "[SILVER][FINNHUB] Existing watermark found | "
                "dataset=%s | watermark=%s",
                dataset,
                watermark_value,
            )

        else:
            logger.info(
                "[SILVER][FINNHUB] No watermark found | "
                "dataset=%s | mode=FULL_LOAD",
                dataset,
            )

        stock_ticker_df = (
            transform_finnhub_stock_tickers_dataset(
                self.spark,
                extract_finnhub_dataset,
            )
        )

        watermark_col_value = (
            execution_start_time.strftime("%Y-%m-%d")
        )

        _write_csv(
            stock_ticker_df,
            datasource,
            self.silver_base_path,
            dataset,
            execution_start_time,
        )

        _write_parquet(
            stock_ticker_df,
            datasource,
            self.silver_base_path,
            dataset,
            execution_start_time,
        )

        self.watermark_manager.write_watermark(
            watermark={
                "pipeline_name": pipeline_name,
                "dataset_name": dataset,
                "watermark_column": "exchange",
                "watermark_value": watermark_col_value,
                "last_processed_at": execution_start_time.isoformat(),
                "batch_id": batch_id,
                "status": "SUCCESS",
                "updated_at": datetime.now(
                    timezone.utc
                ).isoformat(),
                "updated_by": "stock_pipeline",
                "remarks": (
                    "Finnhub Bronze to Silver completed successfully."
                ),
            },
        )

        logger.info(
            "[SILVER][FINNHUB] Processing completed | "
            "dataset=%s | batch_id=%s",
            dataset,
            batch_id,
        )

        return True

    # ========================================================
    # SILVER — COMPANY OVERVIEW
    # ========================================================

    def _process_overview_dataset(
        self,
        datasource,
        execution_start_time: datetime,
        batch_id: str,
    ) -> bool:
        """
        Process Company Overview reference data from Bronze to Silver.

        Flow:
            1. Extract Bronze overview data.
            2. Transform into Silver schema.
            3. Write CSV and Parquet.
            4. Update overview watermark.
        """

        overview_dataset = config.get_dataset_name_by_function(
            "OVERVIEW"
        )

        pipeline_name = "bronze_to_silver"

        logger.info(
            "[SILVER][OVERVIEW] Starting processing | "
            "dataset=%s | batch_id=%s",
            overview_dataset,
            batch_id,
        )

        extracted_overview_data = (
            self.extractor.extract_bronze_overview_data(
                datasource=datasource,
                dataset=overview_dataset,
                execution_start_time=execution_start_time,
                batch_id=batch_id,
            )
        )

        overview_df = silver_transform_overview(
            self.spark,
            extracted_overview_data,
        )

        _write_csv(
            overview_df,
            datasource,
            self.silver_base_path,
            overview_dataset,
            execution_start_time,
        )

        _write_parquet(
            overview_df,
            datasource,
            self.silver_base_path,
            overview_dataset,
            execution_start_time,
        )

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
                "updated_at": datetime.now(
                    timezone.utc
                ).isoformat(),
                "updated_by": "stock_pipeline",
                "remarks": (
                    "Bronze to Silver completed. "
                    "TODO: add content hash value."
                ),
            },
        )

        logger.info(
            "[SILVER][OVERVIEW] Processing completed | "
            "dataset=%s | batch_id=%s",
            overview_dataset,
            batch_id,
        )

        return True

    # ========================================================
    # GOLD — UNIFIED COMPANY DATASET
    # ========================================================

    def _build_gold_layer(
        self,
        execution_start_time: datetime,
    ) -> None:
        """
        Build the Gold unified dataset from Silver datasets.

        Silver Daily and Company Overview datasets are joined
        using the stock symbol.

        Outputs:
            - Gold CSV
            - Gold Parquet
        """

        daily_dataset = config.get_dataset_name_by_function(
            "TIME_SERIES_DAILY"
        )

        overview_dataset = config.get_dataset_name_by_function(
            "OVERVIEW"
        )

        logger.info(
            "[GOLD] Starting Gold layer build | "
            "daily_dataset=%s | overview_dataset=%s",
            daily_dataset,
            overview_dataset,
        )

        try:
            daily_parquet = (
                self.extractor.extract_silver_daily_data_parquet(
                    daily_dataset,
                    execution_start_time=execution_start_time,
                )
            )

            overview_parquet = (
                self.extractor.extract_silver_overview_data_parquet(
                    overview_dataset,
                    execution_start_time=execution_start_time,
                )
            )

            daily_parquet = daily_parquet.select(
                "symbol",
                "day_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "daily_change",
                "daily_change_percentage",
                "market_movement",
                "thirty_day_avg_open",
                "thirty_day_avg_close",
                "all_time_high",
                "all_time_low",
                "processed_at",
            )

            overview_parquet = overview_parquet.drop(
                "processed_at"
            )

            gold_parquet_df = daily_parquet.join(
                overview_parquet,
                on="symbol",
                how="left",
            )

            daily_csv = (
                self.extractor.extract_silver_daily_data_csv(
                    daily_dataset,
                    "csv",
                    execution_start_time=execution_start_time,
                )
            )

            overview_csv = (
                self.extractor.extract_silver_overview_data_csv(
                    overview_dataset,
                    "csv",
                    execution_start_time=execution_start_time,
                )
            )

            daily_csv = daily_csv.select(
                "symbol",
                "day_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "daily_change",
                "daily_change_percentage",
                "market_movement",
                "thirty_day_avg_open",
                "thirty_day_avg_close",
                "all_time_high",
                "all_time_low",
                "processed_at",
            )

            overview_csv = overview_csv.drop(
                "processed_at"
            )

            gold_csv_df = daily_csv.join(
                overview_csv,
                on="symbol",
                how="left",
            )

            _write_csv(
                gold_csv_df,
                "stock",
                self.gold_base_path,
                "company_dataset",
                execution_start_time,
            )

            _write_parquet(
                gold_parquet_df,
                "stock",
                self.gold_base_path,
                "company_dataset",
                execution_start_time,
            )

            logger.info(
                "[GOLD] Gold layer build completed successfully | "
                "dataset=company_dataset"
            )

        except Exception as e:
            logger.warning(
                "[GOLD] Gold layer build skipped | "
                "execution_time=%s | reason=%s",
                execution_start_time.isoformat(),
                str(e),
            )

    # ========================================================
    # PIPELINE — END-TO-END ORCHESTRATION
    # ========================================================

    def run(
        self,
        execution_start_time: datetime,
        stock_symbols: list[str],
        full_load: bool = False,
    ) -> list[dict]:
        """
        Execute the complete end-to-end Medallion ETL pipeline.

        Pipeline flow:

            Massive API
                ↓
            Bronze
                ↓
            Silver
                ↓
            Gold

        Also processes:
            Finnhub API → Bronze → Silver

        Args:
            execution_start_time:
                UTC timestamp for the pipeline execution.

            stock_symbols:
                Stock symbols to process.

            full_load:
                Whether the pipeline should perform a full load.
        """

        batch_id = (
            f"batch_{execution_start_time.strftime('%Y%m%d_%H%M%S')}"
        )

        pipeline_start_time = datetime.now(
            timezone.utc
        )

        logger.info(
            "[PIPELINE] Starting ETL execution | "
            "batch_id=%s | execution_start_time=%s | "
            "full_load=%s | symbols=%s",
            batch_id,
            execution_start_time.isoformat(),
            full_load,
            stock_symbols,
        )

        try:
            logger.info(
                "[PIPELINE][STEP 1/6] Massive API → Bronze | "
                "batch_id=%s",
                batch_id,
            )

            results_massive = (
                self._ingest_from_massive_api(
                    batch_id,
                    execution_start_time,
                    symbol="IBM",
                )
            )

            logger.info(
                "[PIPELINE][STEP 1/6] Massive API → Bronze completed."
            )

            logger.info(
                "[PIPELINE][STEP 2/6] Finnhub API → Bronze | "
                "batch_id=%s",
                batch_id,
            )

            results_finnhub = (
                self._ingest_from_finnhub_api(
                    batch_id,
                    execution_start_time,
                )
            )

            logger.info(
                "[PIPELINE][STEP 2/6] Finnhub API → Bronze completed."
            )

            logger.info(
                "[PIPELINE][STEP 3/6] Bronze → Silver | Exchanges."
            )

            massive_exchange_processing = (
                self._process_massive_exchange_dataset(
                    datasource="massive",
                    dataset="exchanges",
                    execution_start_time=execution_start_time,
                    batch_id=batch_id,
                )
            )

            logger.info(
                "[PIPELINE][STEP 3/6] Exchanges processing completed | "
                "written=%s",
                massive_exchange_processing,
            )

            logger.info(
                "[PIPELINE][STEP 4/6] Bronze → Silver | Stock Overview."
            )

            stock_overview_processing = (
                self._process_massive_dataset(
                    "massive",
                    "stock_overview",
                    execution_start_time,
                    batch_id,
                )
            )

            logger.info(
                "[PIPELINE][STEP 4/6] Stock Overview processing completed | "
                "written=%s",
                stock_overview_processing,
            )

            logger.info(
                "[PIPELINE][STEP 5/6] Bronze → Silver | "
                "Aggregates + Dividends + Ticker Reference."
            )

            aggregates_processing = (
                self._process_massive_dataset(
                    "massive",
                    "aggregates",
                    execution_start_time,
                    batch_id,
                )
            )

            dividends_processing = (
                self._process_massive_dataset(
                    "massive",
                    "dividends",
                    execution_start_time,
                    batch_id,
                )
            )

            stocks_list_processing = (
                self._process_finnhub_dataset(
                    "finnhub",
                    "ticker_reference",
                    execution_start_time,
                    batch_id,
                )
            )

            logger.info(
                "[PIPELINE][STEP 5/6] Silver processing completed | "
                "aggregates=%s | dividends=%s | ticker_reference=%s",
                aggregates_processing,
                dividends_processing,
                stocks_list_processing,
            )

            logger.info(
                "[PIPELINE][STEP 6/6] Silver → Gold."
            )

            self._build_gold_layer(
                execution_start_time
            )

            logger.info(
                "[PIPELINE][STEP 6/6] Gold processing completed."
            )

            pipeline_duration = (
                datetime.now(timezone.utc)
                - pipeline_start_time
            ).total_seconds()

            logger.info(
                "[PIPELINE] ETL execution completed successfully | "
                "batch_id=%s | duration_seconds=%.2f | "
                "silver_exchanges=%s | "
                "silver_overview=%s | "
                "silver_aggregates=%s | "
                "silver_dividends=%s | "
                "silver_ticker_reference=%s",
                batch_id,
                pipeline_duration,
                massive_exchange_processing,
                stock_overview_processing,
                aggregates_processing,
                dividends_processing,
                stocks_list_processing,
            )

            return "success"

        except Exception:
            pipeline_duration = (
                datetime.now(timezone.utc)
                - pipeline_start_time
            ).total_seconds()

            logger.exception(
                "[PIPELINE] ETL execution failed | "
                "batch_id=%s | duration_seconds=%.2f",
                batch_id,
                pipeline_duration,
            )

            raise