"""
Massive Stock Splits Ingestion.

Fetches stock split data from the Massive API
and returns serialized records for the Bronze layer.
"""

import logging

from massive import RESTClient

from serialization import serialize_records


logger = logging.getLogger(__name__)


# ============================================================
# FETCH STOCK SPLITS
# ============================================================

def ingest_splits(
    client: RESTClient,
    symbol: str,
    limit: str = "100",
    sort: str = "execution_date.desc",
) -> list[dict]:
    """
    Fetch stock split data from Massive.

    Args:
        client: Massive REST client.
        symbol: Stock ticker symbol.
        limit: Maximum number of records.
        sort: Sort order.

    Returns:
        list[dict]: Serialized stock split records.

    Raises:
        Exception: If the Massive API request fails.
    """

    logger.info(
        "[MASSIVE_SPLITS] "
        "Starting ingestion | symbol=%s",
        symbol,
    )

    # --------------------------------------------------------
    # FETCH
    # --------------------------------------------------------

    response = []

    try:

        for split in client.list_stocks_splits(
            ticker=symbol,
            limit=limit,
            sort=sort,
        ):
            response.append(split)

    except Exception:

        logger.exception(
            "[MASSIVE_SPLITS] "
            "Failed while fetching splits | "
            "symbol=%s | records=%d",
            symbol,
            len(response),
        )

        # If nothing was retrieved, propagate the error.
        if not response:
            raise

        # Otherwise keep the partial response.
        logger.warning(
            "[MASSIVE_SPLITS] "
            "Returning partial response | "
            "symbol=%s | records=%d",
            symbol,
            len(response),
        )

    logger.info(
        "[MASSIVE_SPLITS] "
        "Fetch completed | symbol=%s | records=%d",
        symbol,
        len(response),
    )

    # --------------------------------------------------------
    # VALIDATE
    # --------------------------------------------------------

    if not response:

        logger.warning(
            "[MASSIVE_SPLITS] "
            "API returned an empty response | symbol=%s",
            symbol,
        )

        return []

    # --------------------------------------------------------
    # SERIALIZE
    # --------------------------------------------------------

    data = serialize_records(response)

    logger.info(
        "[MASSIVE_SPLITS] "
        "Serialized %d split records | symbol=%s",
        len(data),
        symbol,
    )

    if data:

        logger.debug(
            "[MASSIVE_SPLITS] "
            "First split record: %s",
            data[0],
        )

    return data