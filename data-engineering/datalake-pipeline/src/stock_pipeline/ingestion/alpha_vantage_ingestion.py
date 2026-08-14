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
API_KEYS = [key for key in _RAW_API_KEYS if key]

if not API_KEYS:
    raise ValueError(
        "No valid Alpha Vantage API keys found. Please check that at least "
        "one ALPHA_VANTAGE_API_KEY environment variable is configured."
    )

if len(API_KEYS) < len(_RAW_API_KEYS):
    logger.warning(
        "[INGESTION][ALPHAVANTAGE][KEY_POOL] %d of %d API key slots unconfigured.",
        len(_RAW_API_KEYS) - len(API_KEYS),
        len(_RAW_API_KEYS),
    )

logger.info(
    "[INGESTION][ALPHAVANTAGE][KEY_POOL_READY] Loaded %d valid API key(s) into pool.",
    len(API_KEYS),
)


def get_key() -> str:
    """
    Randomly select and return a valid API key from the active rotation pool.
    """
    key = random.choice(API_KEYS)
    logger.debug(
        "[INGESTION][ALPHAVANTAGE][KEY_ISSUED] Issued key ending in: ...%s",
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
        self.extractor = extractor
        self.loader = loader
        self.bucket_name = bucket_name

    def ingest(
        self,
        symbol: str,
        function: str,
        dataset: str,
        datasource: str,
        execution_start_time: datetime,
        run_id: str,
    ) -> dict:
        """
        Fetch API response for a given stock symbol and API function,
        validate payload, and write raw JSON to S3 Bronze layer.
        """
        api_key = get_key()

        params = {
            "function": function,
            "symbol": symbol,
            "apikey": api_key,
        }

        logger.info(
            "[INGESTION][ALPHAVANTAGE] Fetching symbol=%s function=%s dataset=%s",
            symbol,
            function,
            dataset,
        )

        data = self.extractor.fetch_alpha_vantage_api_data(params=params)

        error_key = next((k for k in ALPHA_VANTAGE_ERROR_KEYS if k in data), None)
        if error_key:
            logger.error(
                "[INGESTION][ALPHAVANTAGE][ERROR] API error for symbol=%s function=%s key=%s message=%s",
                symbol,
                function,
                error_key,
                data[error_key],
            )
            raise ValueError(
                f"Alpha Vantage API error for symbol={symbol}/function={function}: "
                f"{data[error_key]}"
            )

        ingestion_date = execution_start_time.strftime("%Y-%m-%d")

        bucket_key = (
            f"stock/"
            f"bronze/"
            f"source={datasource}/"
            f"dataset={dataset}/"
            f"ingestion_date={ingestion_date}/"
            f"run_id={run_id}/"
            f"{symbol.upper()}.json"
        )

        return self.loader.upload_raw_to_s3(
            data,
            self.bucket_name,
            bucket_key=bucket_key,
        )