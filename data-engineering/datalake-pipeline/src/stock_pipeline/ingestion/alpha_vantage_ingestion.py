"""
Alpha Vantage API Ingestion Module.

Handles data acquisition from the Alpha Vantage REST API into the raw S3 Bronze layer.

Key Responsibilities:
  1. API Key Pool Rotation: Manages a pool of up to 16 API keys, rotating them randomly per request
     to avoid hitting Alpha Vantage's 5-calls/minute free-tier rate limits.
  2. API Response Validation: Inspects JSON responses for Alpha Vantage error/rate-limit keys
     (`Information`, `Note`, `Error Message`) to prevent rate-limit notices from polluting the Bronze layer.
  3. S3 Bronze Landing: Constructs hierarchical S3 partition keys and uploads raw JSON payloads immutably.
"""

import logging
import os
import random
from datetime import datetime

logger = logging.getLogger(__name__)

# Keys returned by Alpha Vantage when rate limits are exceeded or errors occur
ALPHA_VANTAGE_ERROR_KEYS = ("Information", "Note", "Error Message")

# ============================================================
# API KEY POOL ROTATION
# ============================================================

# Read key slots from environment variables
_RAW_API_KEYS = [
    os.getenv("ALPHA_VANTAGE_API_KEY"),
    os.getenv("ALPHA_VANTAGE_API_KEY_1"),
    os.getenv("ALPHA_VANTAGE_API_KEY_2"),
    os.getenv("ALPHA_VANTAGE_API_KEY_3"),
    os.getenv("ALPHA_VANTAGE_API_KEY_4"),
    os.getenv("ALPHA_VANTAGE_API_KEY_5"),
    os.getenv("ALPHA_VANTAGE_API_KEY_6"),
    os.getenv("ALPHA_VANTAGE_API_KEY_7"),
    os.getenv("ALPHA_VANTAGE_API_KEY_8"),
    os.getenv("ALPHA_VANTAGE_API_KEY_9"),
    os.getenv("ALPHA_VANTAGE_API_KEY_10"),
    os.getenv("ALPHA_VANTAGE_API_KEY_11"),
    os.getenv("ALPHA_VANTAGE_API_KEY_12"),
    os.getenv("ALPHA_VANTAGE_API_KEY_13"),
    os.getenv("ALPHA_VANTAGE_API_KEY_14"),
    os.getenv("ALPHA_VANTAGE_API_KEY_15"),
]

# Filter out unset environment variable slots (`None` or empty strings)
# This prevents passing `None` as the apikey param, which would trigger API error responses.
API_KEYS = [key for key in _RAW_API_KEYS if key]

if not API_KEYS:
    raise ValueError(
        "No valid Alpha Vantage API keys found. Please check that at least "
        "one ALPHA_VANTAGE_API_KEY environment variable is configured."
    )

if len(API_KEYS) < len(_RAW_API_KEYS):
    logger.warning(
        "[INGEST][KEY_POOL] %d of %d API key slots were unconfigured and excluded from rotation.",
        len(_RAW_API_KEYS) - len(API_KEYS),
        len(_RAW_API_KEYS),
    )

logger.info("[INGEST][KEY_POOL_READY] Loaded %d valid Alpha Vantage API key(s) into rotation pool.", len(API_KEYS))


def get_key() -> str:
    """
    Randomly select and return a valid API key from the active rotation pool.

    Returns:
        str: Alpha Vantage API key.
    """
    key = random.choice(API_KEYS)
    logger.debug(
        "[INGEST][KEY_ISSUED] Issued key ending in: ...%s",
        key[-4:] if len(key) > 4 else "****",
    )
    return key


# ============================================================
# ALPHA VANTAGE INGESTION CLASS
# ============================================================

class AlphaVantageIngestion:
    """
    Ingests market data from Alpha Vantage API and lands raw JSON into S3 Bronze.
    """

    def __init__(self, extractor, loader, bucket_name: str):
        """
        Initialize the Ingestion component.

        Args:
            extractor: Extractor object instance (`StockDataExtractor`) for REST API calls.
            loader: Loader object instance (`StockDataLoader`) for boto3 S3 writes.
            bucket_name: Target S3 bucket name.
        """
        self.extractor = extractor
        self.loader = loader
        self.bucket_name = bucket_name

    def ingest(
        self,
        symbol: str,
        function: str,
        dataset: str,
        execution_start_time: datetime,
    ) -> dict:
        """
        Fetch API response for a given stock symbol and API function,
        validate payload, and write raw JSON to S3 Bronze layer.

        Args:
            symbol: Stock ticker symbol (e.g., 'IBM').
            function: Alpha Vantage API function name (e.g., 'TIME_SERIES_DAILY', 'OVERVIEW').
            dataset: Target dataset partition folder name (e.g., 'daily_time_series').
            execution_start_time: Pipeline start timestamp for deterministic partitioning.

        Returns:
            dict: S3 upload result metadata payload from the loader.

        Raises:
            ValueError: If Alpha Vantage returns an error payload or rate-limit notification.
        """
        # Step 1: Obtain random key from key pool
        api_key = get_key()

        params = {
            "function": function,
            "symbol": symbol,
            "apikey": api_key,
        }

        logger.info(
            "[INGEST][API_REQUEST] Ingesting symbol=%s, function=%s, dataset=%s",
            symbol, function, dataset,
        )

        # Step 2: Call REST API via extractor
        data = self.extractor.fetch_alpha_vantage_api_data(params=params)

        # Step 3: Guard against rate-limit / error payloads
        error_key = next((k for k in ALPHA_VANTAGE_ERROR_KEYS if k in data), None)
        if error_key:
            logger.error(
                "[INGEST_ERR][ALPHA_VANTAGE_ERROR] API returned error response instead of data. "
                "Symbol: %s, Function: %s, Error Key: %s, Message: %s",
                symbol,
                function,
                error_key,
                data[error_key],
            )
            raise ValueError(
                f"Alpha Vantage API error for symbol={symbol}/function={function}: "
                f"{data[error_key]}"
            )

        # Step 4: Construct hierarchical S3 Bronze object key
        bucket_key = (
            f"stock/"
            f"bronze/"
            f"source=alphavantage/"
            f"dataset={dataset}/"
            f"year={execution_start_time.year}/"
            f"month={execution_start_time.month:02d}/"
            f"day={execution_start_time.day:02d}/"
            f"hour={execution_start_time.hour:02d}/"
            f"minute={execution_start_time.minute:02d}/"
            f"{symbol.upper()}.json"
        )

        # Step 5: Upload raw JSON payload to S3 Bronze layer
        return self.loader.upload_raw_to_s3(
            data,
            self.bucket_name,
            stock_symbol=symbol,
            bucket_key=bucket_key,
        )