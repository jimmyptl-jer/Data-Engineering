import json
import logging
import os
import random
from datetime import datetime

import boto3
import requests


# ============================================================
# LOGGER CONFIGURATION
# ============================================================

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ============================================================
# AWS CONFIGURATION
# ============================================================

S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]

logger.info(
    "[INGESTION][ALPHAVANTAGE][CONFIG] "
    "S3 bucket name loaded from environment | bucket=%s",
    S3_BUCKET_NAME,
)

s3_client = boto3.client("s3")

logger.info(
    "[INGESTION][ALPHAVANTAGE][CONFIG] "
    "boto3 S3 client initialized."
)


# ============================================================
# ALPHA VANTAGE CONFIGURATION
# ============================================================

ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"


ALPHA_VANTAGE_ENDPOINTS = [
    {
        "function": "TIME_SERIES_DAILY",
        "dataset": "daily_time_series",
        "description": (
            "Daily OHLCV stock prices, volume, and trading metrics"
        ),
    },
    {
        "function": "OVERVIEW",
        "dataset": "company_overview",
        "description": (
            "Company fundamental attributes, financials, ratios, and metadata"
        ),
    },
]

logger.info(
    "[INGESTION][ALPHAVANTAGE][CONFIG] "
    "Loaded %d Alpha Vantage endpoint definition(s): %s",
    len(ALPHA_VANTAGE_ENDPOINTS),
    [ep["function"] for ep in ALPHA_VANTAGE_ENDPOINTS],
)


# ============================================================
# ALPHA VANTAGE API KEY POOL
# ============================================================

API_KEYS = [
    "W1SN8PZWD266H4IT",
    "OYL3KTJ9QI7HIWU9",
    "UDVADFDQN9A9S1ML",
    "T8ZQ39FX5PK58MFT",
    "2V4GYOW0RPSWETZU",
    "4GF66MAAPH7K4VYU",
    "GZ9RIOC5GXQZ1P8R",
    "39PNGODQKLK0ERS8",
    "7KQ8ZZA0FLT36RAU",
    "PVR986OBGGI5J5TX",
    "YGPPJPFZB1OTANF8",
    "PFLGH5VQKF77KMV6",
    "8RNRI5PF9DBEKW88",
    "J6H1HXFS1VTF6A5J",
    "EGTTUO3YAWJD2F40",
    "GD2BDDZOCD9MX8LV",
]



# Filter out unset environment variable slots
API_KEYS = [key for key in API_KEYS if key]

logger.info(
    "[INGESTION][ALPHAVANTAGE][KEY_POOL] "
    "Filtered API key pool | remaining_keys=%d",
    len(API_KEYS),
)


if not API_KEYS:
    logger.error(
        "[INGESTION][ALPHAVANTAGE][KEY_POOL_ERROR] "
        "No valid Alpha Vantage API keys found."
    )

    raise ValueError(
        "No valid Alpha Vantage API keys found. Please check that at least "
        "one ALPHA_VANTAGE_API_KEY environment variable is configured."
    )


if len(API_KEYS) < len(API_KEYS):
    logger.warning(
        "[INGESTION][ALPHAVANTAGE][KEY_POOL] "
        "%d of %d API key slots unconfigured.",
        len(API_KEYS) - len(API_KEYS),
        len(API_KEYS),
    )


logger.info(
    "[INGESTION][ALPHAVANTAGE][KEY_POOL_READY] "
    "Loaded %d valid API key(s) into pool.",
    len(API_KEYS),
)


# ============================================================
# API KEY HELPER
# ============================================================


def get_key() -> str:
    """
    Return a random API key from the configured key pool.
    """

    logger.debug(
        "[INGESTION][ALPHAVANTAGE][API_KEY_SELECT_START] "
        "Selecting API key from pool | pool_size=%d",
        len(API_KEYS),
    )

    key = random.choice(API_KEYS)

    logger.info(
        "[INGESTION][ALPHAVANTAGE][API_KEY_SELECTED] "
        "API key selected from key pool."
    )

    return key


# ============================================================
# ALPHA VANTAGE INGESTION CLASS
# ============================================================


class AlphaVantageIngestion:
    """
    Ingests market data from Alpha Vantage API
    and lands raw JSON into S3 Bronze.
    """

    def __init__(self, extractor, loader, bucket_name: str):
        self.extractor = extractor
        self.loader = loader
        self.bucket_name = bucket_name

        logger.info(
            "[INGESTION][ALPHAVANTAGE][INITIALIZED] "
            "AlphaVantageIngestion initialized | bucket=%s",
            bucket_name,
        )

    # ========================================================
    # INGEST ALL SYMBOLS / ENDPOINTS
    # ========================================================

    def _ingest_from_api(
        self,
        stock_symbols: list[str],
        execution_start_time: datetime,
        full_load: bool = False,
        datasource: str = "alphavantage",
        run_id: str | None = None,
    ) -> list[dict]:
        """
        Fetch raw market data from Alpha Vantage API for all
        symbols and configured endpoints, then write the raw
        JSON files to the S3 Bronze layer.

        Full Load vs Incremental Strategy:

        - Initial run / full_load=True:
          Uses outputsize='full' for historical daily data.

        - Subsequent runs:
          Uses outputsize='compact' for the latest daily data.
        """

        logger.info(
            "[INGESTION][ALPHAVANTAGE][CYCLE_START] "
            "Starting API ingestion cycle | "
            "symbols=%d | full_load=%s | datasource=%s",
            len(stock_symbols),
            full_load,
            datasource,
        )

        logger.debug(
            "[INGESTION][ALPHAVANTAGE][SYMBOL_LIST] "
            "Symbols scheduled for ingestion: %s",
            stock_symbols,
        )

        results = []

        # Generate a run ID if one was not supplied
        if not run_id:
            run_id = execution_start_time.strftime(
                "%Y%m%d_%H%M%S"
            )

            logger.info(
                "[INGESTION][ALPHAVANTAGE][RUN_ID_GENERATED] "
                "No run_id supplied, generated new run_id | run_id=%s",
                run_id,
            )

        logger.info(
            "[INGESTION][ALPHAVANTAGE][RUN_ID] "
            "Using ingestion run ID | run_id=%s",
            run_id,
        )

        # ====================================================
        # LOOP THROUGH SYMBOLS
        # ====================================================

        for symbol_index, symbol in enumerate(stock_symbols, start=1):

            logger.info(
                "[INGESTION][ALPHAVANTAGE][SYMBOL_START] "
                "Starting symbol ingestion | symbol=%s | progress=%d/%d",
                symbol,
                symbol_index,
                len(stock_symbols),
            )

            # =================================================
            # LOOP THROUGH ENDPOINTS
            # =================================================

            for endpoint in ALPHA_VANTAGE_ENDPOINTS:

                function = endpoint["function"]
                dataset = endpoint["dataset"]

                logger.info(
                    "[INGESTION][ALPHAVANTAGE][ENDPOINT_START] "
                    "Processing endpoint | "
                    "symbol=%s | function=%s | dataset=%s",
                    symbol,
                    function,
                    dataset,
                )

                try:

                    # =================================================
                    # BUILD ADDITIONAL PARAMETERS
                    # =================================================

                    additional_params = {}

                    if (
                        function == "TIME_SERIES_DAILY"
                        and full_load
                    ):
                        additional_params["outputsize"] = "full"

                        logger.info(
                            "[INGESTION][ALPHAVANTAGE][LOAD_MODE] "
                            "Full load selected | "
                            "symbol=%s | outputsize=full",
                            symbol,
                        )

                    elif function == "TIME_SERIES_DAILY":

                        additional_params["outputsize"] = "compact"

                        logger.info(
                            "[INGESTION][ALPHAVANTAGE][LOAD_MODE] "
                            "Incremental load selected | "
                            "symbol=%s | outputsize=compact",
                            symbol,
                        )

                    # =================================================
                    # INGEST
                    # =================================================

                    response = self.ingest(
                        symbol=symbol,
                        function=function,
                        dataset=dataset,
                        datasource=datasource,
                        execution_start_time=execution_start_time,
                        run_id=run_id,
                        additional_params=additional_params,
                    )

                    results.append(
                        {
                            "symbol": symbol,
                            "function": function,
                            "dataset": dataset,
                            "response": response,
                        }
                    )

                    logger.info(
                        "[INGESTION][ALPHAVANTAGE][ENDPOINT_SUCCESS] "
                        "Endpoint ingestion completed | "
                        "symbol=%s | function=%s | dataset=%s",
                        symbol,
                        function,
                        dataset,
                    )

                except Exception as e:

                    logger.exception(
                        "[INGESTION][ALPHAVANTAGE][ENDPOINT_ERROR] "
                        "Error ingesting endpoint | "
                        "symbol=%s | function=%s | dataset=%s | error=%s",
                        symbol,
                        function,
                        dataset,
                        e,
                    )

                    results.append(
                        {
                            "symbol": symbol,
                            "function": function,
                            "dataset": dataset,
                            "error": str(e),
                        }
                    )

                    logger.warning(
                        "[INGESTION][ALPHAVANTAGE][ENDPOINT_FAILURE_RECORDED] "
                        "Failure recorded in results list | "
                        "symbol=%s | function=%s",
                        symbol,
                        function,
                    )

            logger.info(
                "[INGESTION][ALPHAVANTAGE][SYMBOL_COMPLETE] "
                "Completed symbol ingestion | symbol=%s",
                symbol,
            )

        success_count = len([r for r in results if "error" not in r])
        error_count = len([r for r in results if "error" in r])

        logger.info(
            "[INGESTION][ALPHAVANTAGE][CYCLE_SUMMARY] "
            "Cycle summary | successes=%d | errors=%d",
            success_count,
            error_count,
        )

        logger.info(
            "[INGESTION][ALPHAVANTAGE][CYCLE_COMPLETE] "
            "Ingestion cycle completed | "
            "total_requests_processed=%d | run_id=%s",
            len(results),
            run_id,
        )

        return results

    # ========================================================
    # FETCH ALPHA VANTAGE API DATA
    # ========================================================

    def fetch_alpha_vantage_api_data(
        self,
        params: dict | None = None,
    ) -> dict:
        """
        Execute HTTP GET request against the Alpha Vantage REST API.

        Args:
            params:
                Dictionary containing query parameters:
                function, symbol, apikey, and optional parameters.

        Returns:
            dict:
                Decoded JSON response payload from Alpha Vantage.
        """

        symbol = params.get("symbol") if params else None
        function = params.get("function") if params else None

        logger.info(
            "[EXTRACT][API_CALL] "
            "Executing HTTP GET to Alpha Vantage | "
            "symbol=%s | function=%s",
            symbol,
            function,
        )

        logger.debug(
            "[EXTRACT][API_CALL_PARAMS] "
            "Request query params (excluding apikey) | symbol=%s | params=%s",
            symbol,
            {k: v for k, v in (params or {}).items() if k != "apikey"},
        )

        try:

            response = requests.get(
                ALPHA_VANTAGE_BASE_URL,
                params=params,
                timeout=10,
            )

            logger.info(
                "[EXTRACT][API_RESPONSE] "
                "Received response | "
                "symbol=%s | HTTP status=%s",
                symbol,
                response.status_code,
            )

            logger.debug(
                "[EXTRACT][API_RESPONSE_SIZE] "
                "Raw response payload size | "
                "symbol=%s | size_bytes=%s",
                symbol,
                len(response.content) if response.content else 0,
            )

            response.raise_for_status()

            response_data = response.json()

            logger.info(
                "[EXTRACT][API_RESPONSE_JSON] "
                "Successfully decoded JSON response | "
                "symbol=%s | data_type=%s",
                symbol,
                type(response_data).__name__,
            )

            if isinstance(response_data, dict):

                logger.debug(
                    "[EXTRACT][API_KEYS] "
                    "Top-level keys returned for symbol=%s: %s",
                    symbol,
                    list(response_data.keys()),
                )

            return response_data

        except requests.exceptions.HTTPError as e:

            logger.exception(
                "[EXTRACT][API_FAIL] "
                "HTTP Error | symbol=%s | error=%s",
                symbol,
                e,
            )

            raise

        except requests.exceptions.Timeout as e:

            logger.exception(
                "[EXTRACT][API_FAIL] "
                "Timeout Error | symbol=%s | error=%s",
                symbol,
                e,
            )

            raise

        except requests.exceptions.ConnectionError as e:

            logger.exception(
                "[EXTRACT][API_FAIL] "
                "Connection Error | symbol=%s | error=%s",
                symbol,
                e,
            )

            raise

        except requests.exceptions.RequestException as e:

            logger.exception(
                "[EXTRACT][API_FAIL] "
                "Request Error | symbol=%s | error=%s",
                symbol,
                e,
            )

            raise

        except ValueError as e:

            logger.exception(
                "[EXTRACT][API_FAIL] "
                "JSON Decode Error | symbol=%s | error=%s",
                symbol,
                e,
            )

            raise

    # ========================================================
    # UPLOAD TO S3
    # ========================================================

    def upload_to_s3(
        self,
        data,
        dataset: str,
        run_id: str,
        execution_time: datetime,
        symbol: str | None = None,
        datasource: str = "alphavantage",
    ) -> str:
        """
        Upload raw data to S3 Bronze using a standard envelope.
        """

        logger.info(
            "[INGESTION][ALPHAVANTAGE][S3_UPLOAD_START] "
            "Starting S3 upload | "
            "dataset=%s | symbol=%s | run_id=%s",
            dataset,
            symbol,
            run_id,
        )

        ingestion_date = execution_time.strftime("%Y-%m-%d")

        logger.info(
            "[INGESTION][ALPHAVANTAGE][INGESTION_DATE] "
            "Calculated ingestion date | "
            "ingestion_date=%s",
            ingestion_date,
        )

        # ====================================================
        # BUILD S3 KEY
        # ====================================================

        if symbol:

            s3_key = (
                "stock/"
                "bronze/"
                f"source={datasource}/"
                f"dataset={dataset}/"
                f"symbol={symbol.upper()}/"
                f"ingestion_date={ingestion_date}/"
                f"run_id={run_id}/"
                "data.json"
            )

        else:

            s3_key = (
                "stock/"
                "bronze/"
                f"source={datasource}/"
                f"dataset={dataset}/"
                f"ingestion_date={ingestion_date}/"
                f"run_id={run_id}/"
                "data.json"
            )

        logger.info(
            "[INGESTION][ALPHAVANTAGE][S3_KEY] "
            "Generated S3 key | "
            "bucket=%s | key=%s",
            self.bucket_name,
            s3_key,
        )

        # ====================================================
        # DATA INFORMATION
        # ====================================================

        if isinstance(data, list):

            record_count = len(data)

        elif isinstance(data, dict):

            results = data.get("results")

            if isinstance(results, list):

                record_count = len(results)

            elif isinstance(results, dict):

                record_count = len(results)

            else:

                record_count = 1

        else:

            record_count = 1

        logger.info(
            "[INGESTION][ALPHAVANTAGE][DATA] "
            "Preparing data for Bronze | "
            "dataset=%s | record_count=%s | data_type=%s",
            dataset,
            record_count,
            type(data).__name__,
        )

        # ====================================================
        # STANDARD BRONZE ENVELOPE
        # ====================================================

        payload = {
            "source": datasource,
            "dataset": dataset,
            "ingestion_date": ingestion_date,
            "run_id": run_id,
            "record_count": record_count,
            "results": data,
        }

        logger.info(
            "[INGESTION][ALPHAVANTAGE][BRONZE_ENVELOPE] "
            "Bronze envelope created | "
            "source=%s | dataset=%s | "
            "ingestion_date=%s | run_id=%s | "
            "record_count=%s",
            payload["source"],
            payload["dataset"],
            payload["ingestion_date"],
            payload["run_id"],
            payload["record_count"],
        )

        # ====================================================
        # SERIALIZE JSON
        # ====================================================

        payload_json = json.dumps(
            payload,
            ensure_ascii=False,
        )

        payload_bytes = payload_json.encode("utf-8")

        logger.info(
            "[INGESTION][ALPHAVANTAGE][JSON_SERIALIZED] "
            "Payload serialized to JSON | "
            "size_bytes=%s",
            len(payload_bytes),
        )

        # ====================================================
        # UPLOAD TO S3
        # ====================================================

        logger.info(
            "[INGESTION][ALPHAVANTAGE][S3_UPLOAD] "
            "Uploading payload to S3 | "
            "bucket=%s | key=%s",
            self.bucket_name,
            s3_key,
        )

        s3_client.put_object(
            Bucket=self.bucket_name,
            Key=s3_key,
            Body=payload_bytes,
            ContentType="application/json",
        )

        logger.info(
            "[INGESTION][ALPHAVANTAGE][S3_UPLOAD_SUCCESS] "
            "Upload completed successfully | "
            "dataset=%s | symbol=%s | "
            "record_count=%s | key=%s",
            dataset,
            symbol,
            payload["record_count"],
            s3_key,
        )

        return s3_key

    # ========================================================
    # INGEST SINGLE SYMBOL / ENDPOINT
    # ========================================================

    def ingest(
        self,
        symbol: str,
        function: str,
        dataset: str,
        datasource: str,
        execution_start_time: datetime,
        run_id: str,
        additional_params: dict | None = None,
    ) -> str:
        """
        Fetch API response for a given stock symbol and API
        function, validate payload, and write raw JSON to
        S3 Bronze layer.
        """

        logger.info(
            "[INGESTION][ALPHAVANTAGE][INGEST_START] "
            "Starting ingestion | "
            "symbol=%s | function=%s | "
            "dataset=%s | datasource=%s | run_id=%s",
            symbol,
            function,
            dataset,
            datasource,
            run_id,
        )

        # ====================================================
        # SELECT API KEY
        # ====================================================

        api_key = get_key()

        logger.info(
            "[INGESTION][ALPHAVANTAGE][API_KEY_READY] "
            "API key selected successfully | symbol=%s",
            symbol,
        )

        # ====================================================
        # BUILD API PARAMETERS
        # ====================================================

        params = {
            "function": function,
            "symbol": symbol,
            "apikey": api_key,
        }

        if additional_params:

            params.update(additional_params)

            logger.info(
                "[INGESTION][ALPHAVANTAGE][ADDITIONAL_PARAMS] "
                "Additional API parameters added | "
                "symbol=%s | params=%s",
                symbol,
                {
                    key: value
                    for key, value in additional_params.items()
                },
            )

        logger.info(
            "[INGESTION][ALPHAVANTAGE][API_PARAMS] "
            "API parameters prepared | "
            "function=%s | symbol=%s",
            function,
            symbol,
        )

        # ====================================================
        # FETCH DATA
        # ====================================================

        logger.info(
            "[INGESTION][ALPHAVANTAGE][API_REQUEST_START] "
            "Calling Alpha Vantage API | "
            "symbol=%s | function=%s",
            symbol,
            function,
        )

        data = self.fetch_alpha_vantage_api_data(
            params=params
        )

        logger.info(
            "[INGESTION][ALPHAVANTAGE][API_REQUEST_SUCCESS] "
            "Alpha Vantage API response received | "
            "symbol=%s | data_type=%s",
            symbol,
            type(data).__name__,
        )

        # ====================================================
        # BASIC RESPONSE VALIDATION
        # ====================================================

        logger.debug(
            "[INGESTION][ALPHAVANTAGE][VALIDATION_START] "
            "Starting basic response validation | symbol=%s",
            symbol,
        )

        if not data:

            logger.warning(
                "[INGESTION][ALPHAVANTAGE][EMPTY_RESPONSE] "
                "Alpha Vantage returned an empty response | "
                "symbol=%s | function=%s",
                symbol,
                function,
            )

        if isinstance(data, dict):

            if "Error Message" in data:

                logger.error(
                    "[INGESTION][ALPHAVANTAGE][API_ERROR] "
                    "Alpha Vantage returned an error message | "
                    "symbol=%s | function=%s",
                    symbol,
                    function,
                )

                raise ValueError(
                    data["Error Message"]
                )

            if "Note" in data:

                logger.warning(
                    "[INGESTION][ALPHAVANTAGE][API_NOTE] "
                    "Alpha Vantage returned a note/rate-limit message | "
                    "symbol=%s | function=%s",
                    symbol,
                    function,
                )

        logger.debug(
            "[INGESTION][ALPHAVANTAGE][VALIDATION_COMPLETE] "
            "Response validation completed | symbol=%s",
            symbol,
        )

        # ====================================================
        # INGESTION DATE
        # ====================================================

        ingestion_date = execution_start_time.strftime(
            "%Y-%m-%d"
        )

        logger.info(
            "[INGESTION][ALPHAVANTAGE][INGESTION_DATE] "
            "Ingestion date calculated | "
            "symbol=%s | ingestion_date=%s",
            symbol,
            ingestion_date,
        )

        # ====================================================
        # BUILD S3 BRONZE KEY
        # ====================================================

        bucket_key = (
            "stock/"
            "bronze/"
            f"source={datasource}/"
            f"dataset={dataset}/"
            f"ingestion_date={ingestion_date}/"
            f"run_id={run_id}/"
            f"{symbol.upper()}.json"
        )

        logger.info(
            "[INGESTION][ALPHAVANTAGE][S3_KEY] "
            "Generated Bronze S3 key | "
            "bucket=%s | key=%s",
            self.bucket_name,
            bucket_key,
        )

        # ====================================================
        # UPLOAD RAW DATA
        # ====================================================

        logger.info(
            "[INGESTION][ALPHAVANTAGE][RAW_UPLOAD_START] "
            "Uploading raw API response to Bronze | "
            "symbol=%s | dataset=%s",
            symbol,
            dataset,
        )

        result = self.loader.upload_raw_to_s3(
            data,
            self.bucket_name,
            bucket_key=bucket_key,
        )

        logger.info(
            "[INGESTION][ALPHAVANTAGE][RAW_UPLOAD_SUCCESS] "
            "Raw data uploaded to Bronze successfully | "
            "symbol=%s | dataset=%s | result=%s",
            symbol,
            dataset,
            result,
        )

        logger.info(
            "[INGESTION][ALPHAVANTAGE][INGEST_SUCCESS] "
            "Ingestion completed successfully | "
            "symbol=%s | dataset=%s | "
            "run_id=%s | s3_key=%s",
            symbol,
            dataset,
            run_id,
            bucket_key,
        )

        return result