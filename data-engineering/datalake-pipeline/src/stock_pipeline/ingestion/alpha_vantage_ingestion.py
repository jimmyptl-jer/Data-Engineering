"""
Handles ingestion of stock data from the Alpha Vantage API into the
Bronze S3 layer.
"""

import logging
import os
import random
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

ALPHA_VANTAGE_ERROR_KEYS = ("Information", "Note", "Error Message")

# ============================================================
# API KEY POOL
# ============================================================

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

# Filter out any unset env vars (None) so a random pick can never
# return a missing key — this was the root cause of the empty gold
# data: a None key silently dropped the "apikey" query param and
# Alpha Vantage returned a short error payload instead of real data.
API_KEYS = [key for key in _RAW_API_KEYS if key]

if not API_KEYS:
    raise ValueError(
        "No valid Alpha Vantage API keys found. Check that at least "
        "one ALPHA_VANTAGE_API_KEY* environment variable is set."
    )

if len(API_KEYS) < len(_RAW_API_KEYS):
    logger.warning(
        "%d of %d configured Alpha Vantage API key slots were empty "
        "and have been excluded from rotation.",
        len(_RAW_API_KEYS) - len(API_KEYS),
        len(_RAW_API_KEYS),
    )

logger.info("[STEP INGEST][APIKEYS_LOADED] Loaded %d valid Alpha Vantage API key(s).", len(API_KEYS))


def get_key() -> str:
    """
    Return a random API key from the valid key pool.
    """
    key = random.choice(API_KEYS)
    logger.debug(
        "Issued API key ending in: ...%s",
        key[-4:] if len(key) > 4 else "****",
    )
    return key


# ============================================================
# INGESTION
# ============================================================

class AlphaVantageIngestion:

    def __init__(self, extractor, loader, bucket_name):
        self.extractor = extractor
        self.loader = loader
        self.bucket_name = bucket_name

    def ingest(self, symbol, function, dataset, execution_start_time):
        api_key = get_key()

        params = {
            "function": function,
            "symbol": symbol,
            "apikey": api_key,
        }

        data = self.extractor.fetch_alpha_vantage_api_data(params=params)

        # Guard against silently uploading a rate-limit/error response
        # as if it were real data.
        error_key = next((k for k in ALPHA_VANTAGE_ERROR_KEYS if k in data), None)
        if error_key:
            logger.error(
                "[STEP INGEST_ERR][ALPHA_VANTAGE_ERROR] Alpha Vantage returned an error/rate-limit response instead "
                "of data. Symbol: %s, Function: %s, Key: %s, Message: %s",
                symbol,
                function,
                error_key,
                data[error_key],
            )
            raise ValueError(
                f"Alpha Vantage API error for {symbol}/{function}: "
                f"{data[error_key]}"
            )

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

        return self.loader.upload_raw_to_s3(
            data,
            self.bucket_name,
            stock_symbol=symbol,
            bucket_key=bucket_key
        )