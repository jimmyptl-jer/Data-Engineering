"""
Twelve Data Ingestion — API Key.
"""

import logging


logger = logging.getLogger(__name__)


# ============================================================
# TWELVE DATA API KEY
# ============================================================

API_KEY = "6db6ec59b470438198542ee83f69cb09"


# ============================================================
# API KEY HELPER
# ============================================================


def get_key() -> str:
    """
    Return the Twelve Data API key.
    """

    logger.info(
        "[INGESTION][TWELVEDATA][API_KEY_SELECTED] "
        "API key selected."
    )

    return API_KEY
