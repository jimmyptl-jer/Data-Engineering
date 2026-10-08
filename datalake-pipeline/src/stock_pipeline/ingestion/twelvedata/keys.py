"""
Twelve Data Ingestion — API Key.

The key comes from the TWELVEDATA_API_KEY environment variable (set on the
Lambda by template.yaml, or in a local .env / shell for `python app.py`).
"""

import logging
import os


logger = logging.getLogger(__name__)


# ============================================================
# API KEY HELPER
# ============================================================


def get_key() -> str:
    """
    Return the Twelve Data API key from the environment.

    Raises:
        RuntimeError: If TWELVEDATA_API_KEY is not set.
    """

    api_key = os.environ.get("TWELVEDATA_API_KEY")

    if not api_key:
        raise RuntimeError("TWELVEDATA_API_KEY environment variable is not set")

    logger.info(
        "[INGESTION][TWELVEDATA][API_KEY_SELECTED] "
        "API key selected."
    )

    return api_key
