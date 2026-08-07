"""
Extract stage — reads raw data from the Bronze S3 layer and Silver S3
layer into Spark DataFrames.
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

ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"


# ============================================================
# SPARK SCHEMAS — Bronze JSON
# ============================================================

_ohlcv_fields = StructType([
    StructField("1. open", StringType(), True),
    StructField("2. high", StringType(), True),
    StructField("3. low", StringType(), True),
    StructField("4. close", StringType(), True),
    StructField("5. volume", StringType(), True),
])

stock_schema_weekly = StructType([
    StructField("Meta Data", StructType([
        StructField("2. Symbol", StringType(), True),
        StructField("3. Last Refreshed", StringType(), True),
    ]), True),
    StructField("Weekly Time Series", MapType(StringType(), _ohlcv_fields), True),
])

stock_schema_daily = StructType([
    StructField("Meta Data", StructType([
        StructField("2. Symbol", StringType(), True),
        StructField("3. Last Refreshed", StringType(), True),
    ]), True),
    StructField("Time Series (Daily)", MapType(StringType(), _ohlcv_fields), True),
])

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
# EXTRACTOR
# ============================================================

class StockDataExtractor:
    """
    Handles extraction of stock data from Bronze and Silver S3 layers.

    Supported formats: JSON (Bronze), CSV / Parquet (Silver).
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.bucket_name = config.S3_BUCKET_NAME

    # ============================================================
    # PATH BUILDERS
    # ============================================================

    def _bronze_bucket_key(self, dataset: str, execution_start_time: datetime) -> str:
        """Build the S3 path prefix for a Bronze dataset partition."""
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
        """Build the S3 path prefix for a Silver dataset partition."""
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
    # ALPHA VANTAGE API
    # ============================================================

    def fetch_alpha_vantage_api_data(self, params=None):
        """
        Make a GET request to the Alpha Vantage API and return the
        decoded JSON response.
        """
        symbol = params.get("symbol") if params else None
        function = params.get("function") if params else None

        logger.info(
            "[EXTRACT][API] Requesting Alpha Vantage: symbol=%s, function=%s",
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
                "[EXTRACT][API] Response received: symbol=%s, status=%s",
                symbol,
                response.status_code,
            )

            response.raise_for_status()

            response_data = response.json()

            logger.debug(
                "[EXTRACT][API] Response top-level keys for %s: %s",
                symbol,
                list(response_data.keys()),
            )

            return response_data

        except requests.exceptions.HTTPError as e:
            logger.exception("[EXTRACT][API_FAIL] HTTP error for %s: %s", symbol, e)
            raise

        except requests.exceptions.Timeout as e:
            logger.exception("[EXTRACT][API_FAIL] Timeout for %s: %s", symbol, e)
            raise

        except requests.exceptions.ConnectionError as e:
            logger.exception("[EXTRACT][API_FAIL] Connection error for %s: %s", symbol, e)
            raise

        except requests.exceptions.RequestException as e:
            logger.exception("[EXTRACT][API_FAIL] Request error for %s: %s", symbol, e)
            raise

        except ValueError as e:
            logger.exception("[EXTRACT][API_FAIL] JSON decode error for %s: %s", symbol, e)
            raise

    # ============================================================
    # BRONZE EXTRACTORS
    # ============================================================

    def extract_bronze_weekly_data(
        self, dataset: str, execution_start_time: datetime,
    ) -> DataFrame:
        """Extract weekly time-series JSON from the Bronze layer."""
        bucket_key = self._bronze_bucket_key(dataset, execution_start_time)
        logger.info("[EXTRACT][BRONZE_WEEKLY] Reading from: %s", bucket_key)

        try:
            df = (
                self.spark.read
                .format("json")
                .option("multiLine", "true")
                .schema(stock_schema_weekly)
                .option("recursiveFileLookup", "true")
                .load(bucket_key)
            )
            logger.info("[EXTRACT][BRONZE_WEEKLY] Extraction completed.")
            return df

        except Exception as e:
            logger.exception("[EXTRACT][BRONZE_WEEKLY_FAIL] %s", e)
            raise

    def extract_bronze_daily_data(
        self, dataset: str, execution_start_time: datetime,
    ) -> DataFrame:
        """Extract daily time-series JSON from the Bronze layer."""
        bucket_key = self._bronze_bucket_key(dataset, execution_start_time)
        logger.info("[EXTRACT][BRONZE_DAILY] Reading from: %s", bucket_key)

        try:
            df = (
                self.spark.read
                .format("json")
                .option("multiLine", "true")
                .schema(stock_schema_daily)
                .option("recursiveFileLookup", "true")
                .load(bucket_key)
            )
            logger.info("[EXTRACT][BRONZE_DAILY] Extraction completed.")
            return df

        except Exception as e:
            logger.exception("[EXTRACT][BRONZE_DAILY_FAIL] %s", e)
            raise

    def extract_bronze_overview_data(
        self, dataset: str, execution_start_time: datetime,
    ) -> DataFrame:
        """Extract company overview JSON from the Bronze layer."""
        bucket_key = self._bronze_bucket_key(dataset, execution_start_time)
        logger.info("[EXTRACT][BRONZE_OVERVIEW] Reading from: %s", bucket_key)

        try:
            df = (
                self.spark.read
                .format("json")
                .option("multiLine", "true")
                .schema(stock_overview_schema)
                .option("recursiveFileLookup", "true")
                .load(bucket_key)
            )
            logger.info("[EXTRACT][BRONZE_OVERVIEW] Extraction completed.")
            return df

        except Exception as e:
            logger.exception("[EXTRACT][BRONZE_OVERVIEW_FAIL] %s", e)
            raise

    # ============================================================
    # SILVER EXTRACTORS
    # ============================================================

    def extract_silver_daily_data_parquet(
        self, dataset: str, execution_start_time: datetime,
    ) -> DataFrame:
        """Extract daily time-series Parquet from the Silver layer."""
        bucket_key = self._silver_bucket_key(dataset, "parquet", execution_start_time)
        logger.info("[EXTRACT][SILVER_DAILY_PARQUET] Reading from: %s", bucket_key)

        try:
            df = self.spark.read.format("parquet").load(bucket_key)
            logger.info("[EXTRACT][SILVER_DAILY_PARQUET] Extraction completed.")
            return df

        except Exception as e:
            logger.exception("[EXTRACT][SILVER_DAILY_PARQUET_FAIL] %s", e)
            raise

    def extract_silver_daily_data_csv(
        self, dataset: str, data_format: str, execution_start_time: datetime,
    ) -> DataFrame:
        """Extract daily time-series CSV from the Silver layer."""
        bucket_key = self._silver_bucket_key(dataset, data_format, execution_start_time)
        logger.info("[EXTRACT][SILVER_DAILY_CSV] Reading from: %s", bucket_key)

        try:
            df = (
                self.spark.read
                .option("header", "true")
                .format(data_format)
                .load(bucket_key)
            )
            logger.info("[EXTRACT][SILVER_DAILY_CSV] Extraction completed.")
            return df

        except Exception as e:
            logger.exception("[EXTRACT][SILVER_DAILY_CSV_FAIL] %s", e)
            raise

    def extract_silver_overview_data_parquet(
        self, dataset: str, execution_start_time: datetime,
    ) -> DataFrame:
        """Extract company overview Parquet from the Silver layer."""
        bucket_key = self._silver_bucket_key(dataset, "parquet", execution_start_time)
        logger.info("[EXTRACT][SILVER_OVERVIEW_PARQUET] Reading from: %s", bucket_key)

        try:
            df = self.spark.read.format("parquet").load(bucket_key)
            logger.info("[EXTRACT][SILVER_OVERVIEW_PARQUET] Extraction completed.")
            return df

        except Exception as e:
            logger.exception("[EXTRACT][SILVER_OVERVIEW_PARQUET_FAIL] %s", e)
            raise

    def extract_silver_overview_data_csv(
        self, dataset: str, data_format: str, execution_start_time: datetime,
    ) -> DataFrame:
        """Extract company overview CSV from the Silver layer."""
        bucket_key = self._silver_bucket_key(dataset, data_format, execution_start_time)
        logger.info("[EXTRACT][SILVER_OVERVIEW_CSV] Reading from: %s", bucket_key)

        try:
            df = (
                self.spark.read
                .option("header", "true")
                .format(data_format)
                .load(bucket_key)
            )
            logger.info("[EXTRACT][SILVER_OVERVIEW_CSV] Extraction completed.")
            return df

        except Exception as e:
            logger.exception("[EXTRACT][SILVER_OVERVIEW_CSV_FAIL] %s", e)
            raise
