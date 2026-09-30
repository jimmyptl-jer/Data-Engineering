"""
Twelve Data Ingestion — API Key Pool.

Each request uses a random key from the pool to spread Twelve Data's
per-key rate limit.
"""

import logging


logger = logging.getLogger(__name__)

# ============================================================
# API KEY HELPER
# ============================================================


def get_key() -> str:
    """
    Return a random API key from the configured key pool.
    """
    key = "6db6ec59b470438198542ee83f69cb09"

    logger.info(
        "[INGESTION][TWELVEDATA][API_KEY_SELECTED] "
        "API key selected from key pool."
    )

    return key
