"""
Finnhub Ingestion — API Client.
"""

import logging

import requests

from config import FINNHUB_API_KEY, FINNHUB_BASE_URL, REQUEST_TIMEOUT_SECONDS


logger = logging.getLogger(__name__)


# ============================================================
# FETCH FINNHUB DATA
# ============================================================

def fetch_stock_symbols(exchange: str) -> list:
    """
    Fetch stock ticker reference data from Finnhub.

    Endpoint:
        GET /stock/symbol

    Args:
        exchange: Finnhub exchange code, e.g. US.

    Returns:
        List of ticker reference records.
    """

    url = f"{FINNHUB_BASE_URL}/stock/symbol"

    params = {
        "exchange": exchange,
        "token": FINNHUB_API_KEY,
    }

    logger.info(
        "[FINNHUB] Fetching stock symbols | exchange=%s",
        exchange,
    )

    response = requests.get(
        url,
        params=params,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    response.raise_for_status()

    data = response.json()

    logger.info(
        "[FINNHUB] Received %s records | exchange=%s",
        len(data) if isinstance(data, list) else 0,
        exchange,
    )

    return data
