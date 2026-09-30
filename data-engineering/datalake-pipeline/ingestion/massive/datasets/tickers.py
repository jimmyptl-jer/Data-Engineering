"""
Massive Tickers Ingestion.

Fetches ticker reference data from the Massive API
and returns serialized records for the Bronze layer.
"""

import logging

from massive import RESTClient

from serialization import serialize_records


logger = logging.getLogger(__name__)


# ============================================================
# FETCH TICKERS
# ============================================================

def ingest_tickers(
    client: RESTClient,
    market: str = "stocks",
    active: bool = True,
    order: str = "asc",
    limit: int = 1000,
    sort: str = "ticker",
) -> list[dict]:
    """
    Fetch ticker reference data from Massive.

    Endpoint:
        list_tickers()

    Args:
        client: Massive REST client.
        market: Market type. Defaults to stocks.
        active: Whether to fetch active tickers only.
        order: Sort order.
        limit: Maximum records per API request.
        sort: Field used for sorting.

    Returns:
        list[dict]: Serialized ticker records.

    Raises:
        Exception: If the Massive API request fails.
    """

    logger.info(
        "[MASSIVE_TICKERS] Starting ticker ingestion | "
        "market=%s | active=%s | limit=%s | sort=%s",
        market,
        active,
        limit,
        sort,
    )

    response = []

    # --------------------------------------------------------
    # FETCH
    # --------------------------------------------------------

    try:

        for ticker in client.list_tickers(
            market=market,
            active=active,
            order=order,
            limit=limit,
            sort=sort,
        ):
            response.append(ticker)

    except Exception:

        logger.exception(
            "[MASSIVE_TICKERS] "
            "API request failed after %d records",
            len(response),
        )

        # If pagination already returned records,
        # keep the partial response.
        if not response:

            raise

        logger.warning(
            "[MASSIVE_TICKERS] "
            "Returning partial response | records=%d",
            len(response),
        )

    logger.info(
        "[MASSIVE_TICKERS] "
        "Fetch completed | records=%d",
        len(response),
    )

    # --------------------------------------------------------
    # VALIDATE
    # --------------------------------------------------------

    if not response:

        logger.warning(
            "[MASSIVE_TICKERS] "
            "API returned an empty response"
        )

        return []

    # --------------------------------------------------------
    # SERIALIZE
    # --------------------------------------------------------

    data = serialize_records(response)

    logger.info(
        "[MASSIVE_TICKERS] "
        "Serialized %d ticker records",
        len(data),
    )

    if data:

        logger.debug(
            "[MASSIVE_TICKERS] "
            "First ticker record: %s",
            data[0],
        )

    return data