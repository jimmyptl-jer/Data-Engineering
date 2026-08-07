"""
Extract Stage Module — Reads raw Bronze data and intermediate Silver data from AWS S3 into PySpark DataFrames.

Key Responsibilities:
  1. Explicit Schema Enforcement: Defines explicit `StructType` Spark schemas for Bronze JSON datasets
     (Weekly Time Series, Daily Time Series, Company Overview) to ensure type safety and avoid runtime schema inference overhead.
  2. REST API Extraction: Executes GET requests to the Alpha Vantage API endpoints with error/timeout handling.
  3. S3 Partition Path Generation: Constructs S3 bucket keys for Bronze (raw JSON) and Silver (Parquet/CSV) storage formats.
  4. Layer Data Extraction: Reads JSON, Parquet, and CSV files from S3 using PySpark Dataframe Readers.
"""

import logging
from datetime import datetime

import requests
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    MapType,
    StringType,
    StructField,
    StructType,
)

from . import config

logger = logging.getLogger(__name__)

# Base REST API URL for Alpha Vantage queries
ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"


# ============================================================
# SPARK EXPLICIT SCHEMAS — Bronze JSON Payloads
# ============================================================

# Shared OHLCV struct definition for time series data points
_ohlcv_fields = StructType([
    StructField("1. open", StringType(), True),
    StructField("2. high", StringType(), True),
    StructField("3. low", StringType(), True),
    StructField("4. close", StringType(), True),
    StructField("5. volume", StringType(), True),
])

# Explicit schema for Weekly Time Series raw JSON
stock_schema_weekly = StructType([
    StructField("Meta Data", StructType([
        StructField("2. Symbol", StringType(), True),
        StructField("3. Last Refreshed", StringType(), True),
    ]), True),
    StructField("Weekly Time Series", MapType(StringType(), _ohlcv_fields), True),
])

# Explicit schema for Daily Time Series raw JSON
stock_schema_daily = StructType([
    StructField("Meta Data", StructType([
        StructField("2. Symbol", StringType(), True),
        StructField("3. Last Refreshed", StringType(), True),
    ]), True),
    StructField("Time Series (Daily)", MapType(StringType(), _ohlcv_fields), True),
])

# Explicit schema for Company Overview reference JSON (50+ business columns)
stock_overview_schema = StructType([
    StructField("Symbol", StringType(), True),
    StructField("AssetType", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("CIK", StringType(), True),
    StructField("Exchange", StringType(), True),
    StructField("Currency", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Sector", StringType(), True),
    StructField("Industry", StringType(), True),
    StructField("OfficialSite", StringType(), True),
    StructField("FiscalYearEnd", StringType(), True),
    StructField("LatestQuarter", StringType(), True),
    StructField("MarketCapitalization", StringType(), True),
    StructField("EBITDA", StringType(), True),
    StructField("PERatio", StringType(), True),
    StructField("PEGRatio", StringType(), True),
    StructField("BookValue", StringType(), True),
    StructField("DividendPerShare", StringType(), True),
    StructField("DividendYield", StringType(), True),
    StructField("EPS", StringType(), True),
    StructField("RevenuePerShareTTM", StringType(), True),
    StructField("ProfitMargin", StringType(), True),
    StructField("OperatingMarginTTM", StringType(), True),
    StructField("ReturnOnAssetsTTM", StringType(), True),
    StructField("ReturnOnEquityTTM", StringType(), True),
    StructField("RevenueTTM", StringType(), True),
    StructField("GrossProfitTTM", StringType(), True),
    StructField("DilutedEPSTTM", StringType(), True),
    StructField("QuarterlyEarningsGrowthYOY", StringType(), True),
    StructField("QuarterlyRevenueGrowthYOY", StringType(), True),
    StructField("AnalystTargetPrice", StringType(), True),
    StructField("AnalystRatingStrongBuy", StringType(), True),
    StructField("AnalystRatingBuy", StringType(), True),
    StructField("AnalystRatingHold", StringType(), True),
    StructField("AnalystRatingSell", StringType(), True),
    StructField("AnalystRatingStrongSell", StringType(), True),
    StructField("TrailingPE", StringType(), True),
    StructField("ForwardPE", StringType(), True),
    StructField("PriceToSalesRatioTTM", StringType(), True),
    StructField("PriceToBookRatio", StringType(), True),
    StructField("EVToRevenue", StringType(), True),
    StructField("EVToEBITDA", StringType(), True),
    StructField("Beta", StringType(), True),
    StructField("52WeekHigh", StringType(), True),
    StructField("52WeekLow", StringType(), True),
    StructField("50DayMovingAverage", StringType(), True),
    StructField("200DayMovingAverage", StringType(), True),
    StructField("SharesOutstanding", StringType(), True),
    StructField("SharesFloat", StringType(), True),
    StructField("PercentInsiders", StringType(), True),
    StructField("PercentInstitutions", StringType(), True),
    StructField("DividendDate", StringType(), True),
    StructField("ExDividendDate", StringType(), True),
])


# ============================================================
# STOCK DATA EXTRACTOR CLASS
# ============================================================

class StockDataExtractor:
    """
    Handles data extraction from external APIs and S3 Bronze/Silver data lake partitions.
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize Extractor with active SparkSession.

        Args:
            spark: Active SparkSession instance.
        """
        self.spark = spark
        self.bucket_name = config.S3_BUCKET_NAME

    # ============================================================
    # S3 PATH BUILDERS
    # ============================================================

    def _bronze_bucket_key(self, dataset: str, execution_start_time: datetime) -> str:
        """
        Build fully qualified S3 URI prefix for Bronze layer partitions.

        Args:
            dataset: Dataset folder name (e.g. 'daily_time_series').
            execution_start_time: Execution start timestamp for partitioning.

        Returns:
            str: S3 URI string formatted as `s3a://{bucket}/stock/bronze/...`
        """
        return (
            f"s3a://{self.bucket_name}/"
            f"stock/bronze/"
            f"source=alphavantage/"
            f"dataset={dataset}/"
            f"year={execution_start_time.year}/"
            f"month={execution_start_time.month:02d}/"
            f"day={execution_start_time.day:02d}/"
            f"hour={execution_start_time.hour:02d}/"
            f"minute={execution_start_time.minute:02d}/"
        )

    def _silver_bucket_key(
        self, dataset: str, data_format: str, execution_start_time: datetime,
    ) -> str:
        """
        Build fully qualified S3 URI prefix for Silver layer partitions.

        Args:
            dataset: Dataset folder name (e.g. 'daily_time_series').
            data_format: Storage format extension ('csv' or 'parquet').
            execution_start_time: Execution start timestamp for partitioning.

        Returns:
            str: S3 URI string formatted as `s3a://{bucket}/stock/silver/...`
        """
        return (
            f"s3a://{self.bucket_name}/"
            f"stock/silver/"
            f"source=alphavantage/"
            f"dataset={dataset}/"
            f"year={execution_start_time.year}/"
            f"month={execution_start_time.month:02d}/"
            f"day={execution_start_time.day:02d}/"
            f"hour={execution_start_time.hour:02d}/"
            f"minute={execution_start_time.minute:02d}/"
            f"format={data_format}/"
        )

    # ============================================================
    # REST API CALLS
    # ============================================================

    def fetch_alpha_vantage_api_data(self, params: dict = None) -> dict:
        """
        Execute HTTP GET request against the Alpha Vantage REST API.

        Args:
            params: Dictionary containing query parameters ('function', 'symbol', 'apikey').

        Returns:
            dict: Decoded JSON response payload from Alpha Vantage.

        Raises:
            requests.exceptions.RequestException: If HTTP connection or status fails.
            ValueError: If JSON decoding fails.
        """
        symbol = params.get("symbol") if params else None
        function = params.get("function") if params else None

        logger.info(
            "[EXTRACT][API_CALL] Executing HTTP GET to Alpha Vantage: symbol=%s, function=%s",
            symbol,
            function,
        )

        try:
            response = requests.get(
                ALPHA_VANTAGE_BASE_URL,
                params=params,
                timeout=10,
            )

            logger.info(
                "[EXTRACT][API_RESPONSE] Received response: symbol=%s, HTTP Status=%s",
                symbol,
                response.status_code,
            )

            response.raise_for_status()
            response_data = response.json()

            logger.debug(
                "[EXTRACT][API_KEYS] Top-level keys returned for symbol=%s: %s",
                symbol,
                list(response_data.keys()),
            )

            return response_data

        except requests.exceptions.HTTPError as e:
            logger.exception("[EXTRACT][API_FAIL] HTTP Error for symbol=%s: %s", symbol, e)
            raise

        except requests.exceptions.Timeout as e:
            logger.exception("[EXTRACT][API_FAIL] Timeout Error for symbol=%s: %s", symbol, e)
            raise

        except requests.exceptions.ConnectionError as e:
            logger.exception("[EXTRACT][API_FAIL] Connection Error for symbol=%s: %s", symbol, e)
            raise

        except requests.exceptions.RequestException as e:
            logger.exception("[EXTRACT][API_FAIL] Request Error for symbol=%s: %s", symbol, e)
            raise

        except ValueError as e:
            logger.exception("[EXTRACT][API_FAIL] JSON Decode Error for symbol=%s: %s", symbol, e)
            raise

    # ============================================================
    # BRONZE LAYER EXTRACTORS
    # ============================================================

    def extract_bronze_weekly_data(
        self, dataset: str, execution_start_time: datetime,
    ) -> DataFrame:
        """
        Extract raw weekly time-series JSON files from Bronze S3 partition into a Spark DataFrame.

        Args:
            dataset: Dataset partition folder name.
            execution_start_time: Execution start timestamp.

        Returns:
            DataFrame: Spark DataFrame matching `stock_schema_weekly`.
        """
        bucket_key = self._bronze_bucket_key(dataset, execution_start_time)
        logger.info("[EXTRACT][BRONZE_WEEKLY] Reading JSON files from: %s", bucket_key)

        try:
            df = (
                self.spark.read
                .format("json")
                .option("multiLine", "true")
                .schema(stock_schema_weekly)
                .option("recursiveFileLookup", "true")
                .load(bucket_key)
            )
            logger.info("[EXTRACT][BRONZE_WEEKLY_OK] Extraction completed.")
            return df

        except Exception as e:
            logger.exception("[EXTRACT][BRONZE_WEEKLY_FAIL] Failed reading weekly data: %s", e)
            raise

    def extract_bronze_daily_data(
        self, dataset: str, execution_start_time: datetime,
    ) -> DataFrame:
        """
        Extract raw daily time-series JSON files from Bronze S3 partition into a Spark DataFrame.

        Args:
            dataset: Dataset partition folder name.
            execution_start_time: Execution start timestamp.

        Returns:
            DataFrame: Spark DataFrame matching `stock_schema_daily`.
        """
        bucket_key = self._bronze_bucket_key(dataset, execution_start_time)
        logger.info("[EXTRACT][BRONZE_DAILY] Reading JSON files from: %s", bucket_key)

        try:
            df = (
                self.spark.read
                .format("json")
                .option("multiLine", "true")
                .schema(stock_schema_daily)
                .option("recursiveFileLookup", "true")
                .load(bucket_key)
            )
            logger.info("[EXTRACT][BRONZE_DAILY_OK] Extraction completed.")
            return df

        except Exception as e:
            logger.exception("[EXTRACT][BRONZE_DAILY_FAIL] Failed reading daily data: %s", e)
            raise

    def extract_bronze_overview_data(
        self, dataset: str, execution_start_time: datetime,
    ) -> DataFrame:
        """
        Extract raw company overview JSON files from Bronze S3 partition into a Spark DataFrame.

        Args:
            dataset: Dataset partition folder name.
            execution_start_time: Execution start timestamp.

        Returns:
            DataFrame: Spark DataFrame matching `stock_overview_schema`.
        """
        bucket_key = self._bronze_bucket_key(dataset, execution_start_time)
        logger.info("[EXTRACT][BRONZE_OVERVIEW] Reading JSON files from: %s", bucket_key)

        try:
            df = (
                self.spark.read
                .format("json")
                .option("multiLine", "true")
                .schema(stock_overview_schema)
                .option("recursiveFileLookup", "true")
                .load(bucket_key)
            )
            logger.info("[EXTRACT][BRONZE_OVERVIEW_OK] Extraction completed.")
            return df

        except Exception as e:
            logger.exception("[EXTRACT][BRONZE_OVERVIEW_FAIL] Failed reading overview data: %s", e)
            raise

    # ============================================================
    # SILVER LAYER EXTRACTORS
    # ============================================================

    def extract_silver_daily_data_parquet(
        self, dataset: str, execution_start_time: datetime,
    ) -> DataFrame:
        """
        Extract processed daily time-series Parquet files from Silver S3 layer.

        Args:
            dataset: Dataset partition folder name.
            execution_start_time: Execution start timestamp.

        Returns:
            DataFrame: Spark DataFrame loaded from Parquet.
        """
        bucket_key = self._silver_bucket_key(dataset, "parquet", execution_start_time)
        logger.info("[EXTRACT][SILVER_DAILY_PARQUET] Reading Parquet files from: %s", bucket_key)

        try:
            df = self.spark.read.format("parquet").load(bucket_key)
            logger.info("[EXTRACT][SILVER_DAILY_PARQUET_OK] Extraction completed.")
            return df

        except Exception as e:
            logger.exception("[EXTRACT][SILVER_DAILY_PARQUET_FAIL] Failed reading daily Parquet: %s", e)
            raise

    def extract_silver_daily_data_csv(
        self, dataset: str, data_format: str, execution_start_time: datetime,
    ) -> DataFrame:
        """
        Extract processed daily time-series CSV files from Silver S3 layer.

        Args:
            dataset: Dataset partition folder name.
            data_format: Storage format string ('csv').
            execution_start_time: Execution start timestamp.

        Returns:
            DataFrame: Spark DataFrame loaded from CSV.
        """
        bucket_key = self._silver_bucket_key(dataset, data_format, execution_start_time)
        logger.info("[EXTRACT][SILVER_DAILY_CSV] Reading CSV files from: %s", bucket_key)

        try:
            df = (
                self.spark.read
                .option("header", "true")
                .format(data_format)
                .load(bucket_key)
            )
            logger.info("[EXTRACT][SILVER_DAILY_CSV_OK] Extraction completed.")
            return df

        except Exception as e:
            logger.exception("[EXTRACT][SILVER_DAILY_CSV_FAIL] Failed reading daily CSV: %s", e)
            raise

    def extract_silver_overview_data_parquet(
        self, dataset: str, execution_start_time: datetime,
    ) -> DataFrame:
        """
        Extract processed company overview Parquet files from Silver S3 layer.

        Args:
            dataset: Dataset partition folder name.
            execution_start_time: Execution start timestamp.

        Returns:
            DataFrame: Spark DataFrame loaded from Parquet.
        """
        bucket_key = self._silver_bucket_key(dataset, "parquet", execution_start_time)
        logger.info("[EXTRACT][SILVER_OVERVIEW_PARQUET] Reading Parquet files from: %s", bucket_key)

        try:
            df = self.spark.read.format("parquet").load(bucket_key)
            logger.info("[EXTRACT][SILVER_OVERVIEW_PARQUET_OK] Extraction completed.")
            return df

        except Exception as e:
            logger.exception("[EXTRACT][SILVER_OVERVIEW_PARQUET_FAIL] Failed reading overview Parquet: %s", e)
            raise

    def extract_silver_overview_data_csv(
        self, dataset: str, data_format: str, execution_start_time: datetime,
    ) -> DataFrame:
        """
        Extract processed company overview CSV files from Silver S3 layer.

        Args:
            dataset: Dataset partition folder name.
            data_format: Storage format string ('csv').
            execution_start_time: Execution start timestamp.

        Returns:
            DataFrame: Spark DataFrame loaded from CSV.
        """
        bucket_key = self._silver_bucket_key(dataset, data_format, execution_start_time)
        logger.info("[EXTRACT][SILVER_OVERVIEW_CSV] Reading CSV files from: %s", bucket_key)

        try:
            df = (
                self.spark.read
                .option("header", "true")
                .format(data_format)
                .load(bucket_key)
            )
            logger.info("[EXTRACT][SILVER_OVERVIEW_CSV_OK] Extraction completed.")
            return df

        except Exception as e:
            logger.exception("[EXTRACT][SILVER_OVERVIEW_CSV_FAIL] Failed reading overview CSV: %s", e)
            raise
