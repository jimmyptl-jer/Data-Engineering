"""
Alpha Vantage Ingestion — API Key Pool.

Each request uses a random key from the pool to spread Alpha Vantage's
per-key rate limit.
"""

import logging
import random


logger = logging.getLogger(__name__)


# ============================================================
# ALPHA VANTAGE API KEY POOL
# ============================================================

_RAW_API_KEYS = [
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

# Filter out empty key slots
API_KEYS = [key for key in _RAW_API_KEYS if key]

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


if len(API_KEYS) < len(_RAW_API_KEYS):
    logger.warning(
        "[INGESTION][ALPHAVANTAGE][KEY_POOL] "
        "%d of %d API key slots unconfigured.",
        len(_RAW_API_KEYS) - len(API_KEYS),
        len(_RAW_API_KEYS),
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
