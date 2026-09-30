"""
Massive Stock Overview Ingestion.

Fetches stock ticker overview/reference data from the Massive API
and returns a serialized record for the Bronze layer.
"""

import logging

from massive import RESTClient

from serialization import serialize_record


logger = logging.getLogger(__name__)


# ============================================================
# FETCH STOCK OVERVIEW
# ============================================================

def ingest_stock_overview(
    client: RESTClient,
    symbol: str,
) -> dict:
    """
    Fetch stock overview/reference data from Massive.

    Endpoint:
        get_ticker_details(symbol)

    Args:
        client: Massive REST client.
        symbol: Stock ticker symbol.

    Returns:
        dict: Serialized stock overview record.

    Raises:
        Exception: If the Massive API request fails.
    """

    logger.info(
        "[MASSIVE_STOCK_OVERVIEW] "
        "Starting ingestion | symbol=%s",
        symbol,
    )

    # --------------------------------------------------------
    # FETCH
    # --------------------------------------------------------

    try:

        response = client.get_ticker_details(
            symbol,
        )

    except Exception:

        logger.exception(
            "[MASSIVE_STOCK_OVERVIEW] "
            "Failed to fetch stock overview | symbol=%s",
            symbol,
        )

        raise

    logger.info(
        "[MASSIVE_STOCK_OVERVIEW] "
        "Fetch completed | symbol=%s",
        symbol,
    )

    # --------------------------------------------------------
    # VALIDATE
    # --------------------------------------------------------

    if not response:

        logger.warning(
            "[MASSIVE_STOCK_OVERVIEW] "
            "API returned an empty response | symbol=%s",
            symbol,
        )

        return {}

    # --------------------------------------------------------
    # SERIALIZE
    # --------------------------------------------------------

    data = serialize_record(response)

    logger.info(
        "[MASSIVE_STOCK_OVERVIEW] "
        "Stock overview serialized | symbol=%s",
        symbol,
    )

    logger.debug(
        "[MASSIVE_STOCK_OVERVIEW] "
        "Serialized record: %s",
        data,
    )

    return data