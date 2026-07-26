import logging

import requests
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    MapType,
)

from . import config

logger = logging.getLogger(__name__)

ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"

# ============================================================
# Stock JSON SCHEMA
# ============================================================

weekly_stock_schema = StructType([
    StructField("1. open", StringType(), True),
    StructField("2. high", StringType(), True),
    StructField("3. low", StringType(), True),
    StructField("4. close", StringType(), True),
    StructField("5. volume", StringType(), True),
])

stock_schema = StructType([
    StructField(
        "Meta Data",
        StructType([
            StructField("2. Symbol", StringType(), True),
            StructField("3. Last Refreshed", StringType(), True),
        ]),
        True,
    ),

    StructField(
        "Weekly Time Series",
        MapType(
            StringType(),
            weekly_stock_schema,
        ),
        True,
    ),
])


class StockDataExtractor:
    """
    Handles extraction of stock data from the Bronze S3 layer.

    Supported formats:
    - JSON
    - CSV

    CSV extraction also demonstrates Spark malformed-data modes:
    - PERMISSIVE
    - DROPMALFORMED
    - FAILFAST
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.bucket_name = config.S3_BUCKET_NAME

    # ============================================================
    # ALPHA VANTAGE API FETCH
    # ============================================================

    def fetch_from_api(self, params=None):
        """
        Make a GET request to the Alpha Vantage API and return the
        decoded JSON response.
        """

        symbol = params.get("symbol") if params else None
        function = params.get("function") if params else None

        logger.info(
            "Preparing Alpha Vantage API request. "
            "Symbol: %s, Function: %s",
            symbol,
            function,
        )

        try:
            logger.info(
                "Sending GET request to Alpha Vantage. Symbol: %s",
                symbol,
            )

            response = requests.get(
                ALPHA_VANTAGE_BASE_URL,
                params=params,
                timeout=10,
            )

            logger.info(
                "Alpha Vantage response received. "
                "Symbol: %s, Status code: %s",
                symbol,
                response.status_code,
            )

            response.raise_for_status()

            logger.info(
                "Alpha Vantage HTTP request successful. Symbol: %s",
                symbol,
            )

            response_data = response.json()

            logger.info(
                "Alpha Vantage JSON response decoded successfully. "
                "Symbol: %s",
                symbol,
            )

            logger.debug(
                "Alpha Vantage response top-level keys for %s: %s",
                symbol,
                list(response_data.keys()),
            )

            return response_data

        except requests.exceptions.HTTPError as e:
            logger.exception(
                "HTTP error while calling Alpha Vantage. "
                "Symbol: %s, Error: %s",
                symbol,
                e,
            )
            raise

        except requests.exceptions.Timeout as e:
            logger.exception(
                "Alpha Vantage request timed out. "
                "Symbol: %s, Error: %s",
                symbol,
                e,
            )
            raise

        except requests.exceptions.ConnectionError as e:
            logger.exception(
                "Connection error while calling Alpha Vantage. "
                "Symbol: %s, Error: %s",
                symbol,
                e,
            )
            raise

        except requests.exceptions.RequestException as e:
            logger.exception(
                "Request error while calling Alpha Vantage. "
                "Symbol: %s, Error: %s",
                symbol,
                e,
            )
            raise

        except ValueError as e:
            logger.exception(
                "Failed to decode Alpha Vantage JSON response. "
                "Symbol: %s, Error: %s",
                symbol,
                e,
            )
            raise

    def _bucket_key(self) -> str:
        """Shared source path for both JSON and CSV extraction."""
        return f"s3a://{self.bucket_name}/stock/source=alphavantage/"

    # ============================================================
    # JSON EXTRACTION
    # ============================================================

    def extract_data(self) -> DataFrame:
        """
        Extract JSON stock data from the Bronze S3 bucket
        and return it as a Spark DataFrame.

        This currently uses an explicit schema (stock_schema) rather
        than inferSchema, for the data-understanding and learning phase.
        """

        bucket_key = self._bucket_key()

        logger.info("Extracting stock JSON data from: %s", bucket_key)

        try:
            stock_data_df = (
                self.spark.read
                .format("json")
                .option("multiLine", "true")
                .schema(stock_schema)
                .option("recursiveFileLookup", "true")
                .load(bucket_key)
            )

            logger.info("Stock JSON extraction completed successfully.")

            # ====================================================
            # DATA UNDERSTANDING
            # ====================================================

            print("\n========== JSON SCHEMA ==========")
            stock_data_df.printSchema()

            return stock_data_df

        except Exception as e:
            logger.exception(
                "Error extracting stock JSON data: %s",
                e,
            )
            raise

    # ============================================================
    # CSV EXTRACTION
    # ============================================================

    def extract_data_csv(self) -> DataFrame:
        """
        Extract CSV stock data from the Bronze S3 bucket.

        Demonstrates Spark's three malformed-data modes:

        1. PERMISSIVE
           Keeps malformed records where possible.

        2. DROPMALFORMED
           Drops malformed records.

        3. FAILFAST
           Raises an exception when malformed data is encountered.

        PERMISSIVE is used as the primary DataFrame returned
        by this method.
        """

        bucket_key = self._bucket_key()

        logger.info("Extracting stock CSV data from: %s", bucket_key)

        try:
            # ====================================================
            # 1. PERMISSIVE MODE (primary DataFrame)
            # ====================================================

            logger.info("Reading CSV data using PERMISSIVE mode.")

            stock_data_df_permissive = (
                self.spark.read
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("mode", "PERMISSIVE")
                .option("recursiveFileLookup", "true")
                .load(bucket_key)
            )

            print("\n========== CSV SCHEMA ==========")
            stock_data_df_permissive.printSchema()

            # ====================================================
            # SAVE INFERRED SCHEMA
            # ====================================================
            # NOTE: named inferred_schema_json (not stock_schema) to
            # avoid shadowing the module-level explicit JSON schema.

            inferred_schema_json = stock_data_df_permissive.schema.json()
            schema_file_path = "stock_schema.json"

            with open(schema_file_path, "w", encoding="utf-8") as file:
                file.write(inferred_schema_json)

            logger.info(
                "Inferred stock schema saved to: %s",
                schema_file_path,
            )

            # ====================================================
            # 2. DROPMALFORMED MODE
            # ====================================================

            logger.info("Reading CSV data using DROPMALFORMED mode.")

            stock_data_df_dropmalformed = (
                self.spark.read
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("mode", "DROPMALFORMED")
                .option("recursiveFileLookup", "true")
                .load(bucket_key)
            )

            # Trigger Spark action so malformed rows are actually
            # evaluated during the learning/testing phase.
            dropmalformed_count = stock_data_df_dropmalformed.count()

            logger.info(
                "DROPMALFORMED record count: %s",
                dropmalformed_count,
            )

            # ====================================================
            # 3. FAILFAST MODE
            # ====================================================

            logger.info("Reading CSV data using FAILFAST mode.")

            stock_data_df_failfast = (
                self.spark.read
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("mode", "FAILFAST")
                .option("recursiveFileLookup", "true")
                .load(bucket_key)
            )

            # Spark uses lazy evaluation.
            # count() forces Spark to process the CSV and therefore
            # actually test FAILFAST behavior.
            try:
                failfast_count = stock_data_df_failfast.count()

                logger.info(
                    "FAILFAST validation passed. Record count: %s",
                    failfast_count,
                )

            except Exception as failfast_error:
                logger.warning(
                    "FAILFAST detected malformed CSV data: %s",
                    failfast_error,
                )

            logger.info("Stock CSV extraction completed successfully.")

            # ====================================================
            # PRIMARY DATAFRAME
            # ====================================================
            #
            # PERMISSIVE is currently our primary DataFrame.
            #
            # Later, once the schema is understood and stabilized,
            # inferSchema should be replaced with an explicit
            # StructType schema.
            # ====================================================

            return stock_data_df_permissive

        except Exception as e:
            logger.exception(
                "Error extracting stock CSV data: %s",
                e,
            )
            raise