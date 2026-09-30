"""
Massive Aggregates Ingestion.

Fetches grouped daily OHLCV aggregate data from the Massive API
and returns serialized records for the Bronze layer.
"""

import logging

from massive import RESTClient

from serialization import serialize_records


logger = logging.getLogger(__name__)


# ============================================================
# FETCH AGGREGATES
# ============================================================

def ingest_aggregates(
    client: RESTClient,
    from_date: str,
    to_date: str | None = None,
    multiplier: int = 1,
    timespan: str = "day",
    adjusted: bool = True,
) -> list[dict]:
    """
    Fetch aggregate OHLCV data from Massive.

    Uses the grouped daily aggregates endpoint.

    Args:
        client: Massive REST client.
        from_date: Start date in YYYY-MM-DD format.
        to_date: Optional end date.
        multiplier: Aggregate multiplier.
        timespan: Aggregate timespan.
        adjusted: Whether to use adjusted prices.

    Returns:
        list[dict]: Serialized aggregate records.
    """

    logger.info(
        "[MASSIVE_AGGREGATES] Starting ingestion | "
        "from_date=%s | to_date=%s | "
        "multiplier=%s | timespan=%s | adjusted=%s",
        from_date,
        to_date,
        multiplier,
        timespan,
        adjusted,
    )

    # --------------------------------------------------------
    # FETCH
    # --------------------------------------------------------

    try:

        response = client.get_grouped_daily_aggs(
            from_date,
            adjusted=str(adjusted).lower(),
        )

    except Exception:

        logger.exception(
            "[MASSIVE_AGGREGATES] "
            "Failed to fetch aggregate data | "
            "from_date=%s",
            from_date,
        )

        raise

    logger.info(
        "[MASSIVE_AGGREGATES] "
        "Fetch completed"
    )

    # --------------------------------------------------------
    # VALIDATE
    # --------------------------------------------------------

    if not response:

        logger.warning(
            "[MASSIVE_AGGREGATES] "
            "API returned an empty response"
        )

        return []

    logger.info(
        "[MASSIVE_AGGREGATES] "
        "Received %d aggregate records",
        len(response),
    )

    # --------------------------------------------------------
    # SERIALIZE
    # --------------------------------------------------------

    data = serialize_records(response)

    logger.info(
        "[MASSIVE_AGGREGATES] "
        "Serialized %d aggregate records",
        len(data),
    )

    if data:

        logger.debug(
            "[MASSIVE_AGGREGATES] "
            "First aggregate record: %s",
            data[0],
        )

    return data