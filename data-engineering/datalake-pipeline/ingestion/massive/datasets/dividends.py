"""
Massive Stock Dividends Ingestion.

Fetches stock dividend data from the Massive API
and returns serialized records for the Bronze layer.
"""

import logging

from massive import RESTClient

from serialization import serialize_records


logger = logging.getLogger(__name__)


# ============================================================
# FETCH STOCK DIVIDENDS
# ============================================================

def ingest_dividends(
    client: RESTClient,
    symbol: str,
    limit: str = "500",
    sort: str = "ticker.asc",
) -> list[dict]:
    """
    Fetch stock dividend data from Massive.

    Args:
        client: Massive REST client.
        symbol: Stock ticker symbol.
        limit: Maximum number of records.
        sort: Sort order.

    Returns:
        list[dict]: Serialized dividend records.

    Raises:
        Exception: If the Massive API request fails.
    """

    logger.info(
        "[MASSIVE_DIVIDENDS] "
        "Starting ingestion | symbol=%s",
        symbol,
    )

    # --------------------------------------------------------
    # FETCH
    # --------------------------------------------------------

    response = []

    try:

        for dividend in client.list_stocks_dividends(
            ticker=symbol,
            limit=limit,
            sort=sort,
        ):
            response.append(dividend)

    except Exception:

        logger.exception(
            "[MASSIVE_DIVIDENDS] "
            "Failed while fetching dividends | "
            "symbol=%s | records=%d",
            symbol,
            len(response),
        )

        # If nothing was retrieved, propagate the error.
        if not response:
            raise

        # Otherwise keep the partial response.
        logger.warning(
            "[MASSIVE_DIVIDENDS] "
            "Returning partial response | "
            "symbol=%s | records=%d",
            symbol,
            len(response),
        )

    logger.info(
        "[MASSIVE_DIVIDENDS] "
        "Fetch completed | symbol=%s | records=%d",
        symbol,
        len(response),
    )

    # --------------------------------------------------------
    # VALIDATE
    # --------------------------------------------------------

    if not response:

        logger.warning(
            "[MASSIVE_DIVIDENDS] "
            "API returned an empty response | symbol=%s",
            symbol,
        )

        return []

    # --------------------------------------------------------
    # SERIALIZE
    # --------------------------------------------------------

    data = serialize_records(response)

    logger.info(
        "[MASSIVE_DIVIDENDS] "
        "Serialized %d dividend records | symbol=%s",
        len(data),
        symbol,
    )

    if data:

        logger.debug(
            "[MASSIVE_DIVIDENDS] "
            "First dividend record: %s",
            data[0],
        )

    return data