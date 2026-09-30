"""
Massive Exchanges Ingestion.

Fetches stock exchange reference data from the Massive API
and returns the serialized records for the Bronze ingestion layer.
"""

import logging

from massive import RESTClient

from serialization import serialize_records


logger = logging.getLogger(__name__)


# ============================================================
# FETCH EXCHANGES
# ============================================================

def ingest_exchanges(
    client: RESTClient,
) -> list[dict]:
    """
    Fetch stock exchange reference data from Massive.

    Endpoint:
        get_exchanges(
            asset_class="stocks",
            locale="us"
        )

    Returns:
        list[dict]: Serialized exchange records.

    Raises:
        Exception: If the Massive API request fails.
    """

    logger.info(
        "[MASSIVE_EXCHANGES] Starting exchange ingestion"
    )

    # --------------------------------------------------------
    # 1. FETCH
    # --------------------------------------------------------

    logger.info(
        "[MASSIVE_EXCHANGES] "
        "Fetching stock exchange data from Massive API"
    )

    response = client.get_exchanges(
        asset_class="stocks",
        locale="us",
    )

    logger.info(
        "[MASSIVE_EXCHANGES] "
        "API fetch completed"
    )

    # --------------------------------------------------------
    # 2. VALIDATE
    # --------------------------------------------------------

    if not response:

        logger.warning(
            "[MASSIVE_EXCHANGES] "
            "API returned an empty response"
        )

        return []

    logger.info(
        "[MASSIVE_EXCHANGES] "
        "Received %d exchange records",
        len(response),
    )

    # --------------------------------------------------------
    # 3. SERIALIZE
    # --------------------------------------------------------

    logger.info(
        "[MASSIVE_EXCHANGES] "
        "Serializing API response"
    )

    data = serialize_records(response)

    logger.info(
        "[MASSIVE_EXCHANGES] "
        "Serialized %d exchange records",
        len(data),
    )

    if data:

        logger.debug(
            "[MASSIVE_EXCHANGES] "
            "First exchange record: %s",
            data[0],
        )

    return data