"""
Finnhub API Ingestion Module.

Handles data acquisition from the Finnhub REST API
into the raw S3 Bronze layer.

Finnhub provides free access to real-time stock data, including:
    - Stock symbols list per exchange (US, etc.)
    - Company profiles
    - Market news

Data Flow:
    Finnhub REST API  -->  JSON Response  -->  Serialize  -->  S3 Bronze (raw JSON)

Bronze S3 Key Format:
    stock/bronze/source=finnhub/dataset={dataset}/ingestion_date=YYYY-MM-DD/run_id={batch_id}/data.json
"""

import json
import logging
import os

import finnhub
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Optional, Any


logger = logging.getLogger(__name__)


def get_key() -> str:
    """
    Retrieve the Finnhub API key from the FINNHUB_API_KEY environment variable.

    Returns:
        str: The Finnhub API key.

    Raises:
        ValueError: If the environment variable is not set.
    """
    api_key = os.getenv("FINNHUB_API_KEY")

    if not api_key:
        raise ValueError(
            "FINNHUB_API_KEY environment variable is not set."
        )

    return api_key


class FinnHubIngestion:
    """
    Handles ingestion of raw data from the Finnhub API
    into the S3 Bronze layer.

    This class is responsible for:
        1. Fetching stock symbol lists from Finnhub's REST API.
        2. Serializing API response objects into JSON-safe dictionaries.
        3. Uploading the serialized data to S3 as raw JSON files.

    Attributes:
        loader: StockDataLoader instance for uploading data to S3.
        bucket_name: Target S3 bucket for Bronze layer storage.
        finnhub_client: Authenticated Finnhub API client.
    """

    def __init__(
        self,
        loader,
        bucket_name: str,
    ):
        """
        Initialize the FinnHubIngestion with an S3 loader and bucket name.

        Args:
            loader: StockDataLoader instance for S3 upload operations.
            bucket_name: Name of the target S3 bucket (e.g., 'graywolf--data--lake').
        """
        self.loader = loader
        self.bucket_name = bucket_name

        # Initialize the Finnhub client with API key from environment
        self.finnhub_client = finnhub.Client(get_key())

        logger.info(
            "[INGEST][FINNHUB_INIT] FinnHubIngestion initialized | bucket=%s",
            bucket_name,
        )

    # =========================================================
    # SERIALIZATION HELPERS
    # =========================================================

    @staticmethod
    def _serialize_record(record: Any) -> dict:
        """
        Convert a single Finnhub API response object into a plain dictionary.

        Finnhub returns different object types depending on the endpoint.
        This method handles three cases:
            - Already a dict  → return as-is
            - A dataclass     → use dataclasses.asdict()
            - Has __dict__    → convert via __dict__ attribute

        Args:
            record: A single API response object from Finnhub.

        Returns:
            dict: A JSON-serializable Python dictionary.

        Raises:
            TypeError: If the record type is not supported.
        """
        # Case 1: Already a dictionary — no conversion needed
        if isinstance(record, dict):
            return record

        # Case 2: Python dataclass — use stdlib asdict()
        if is_dataclass(record):
            return asdict(record)

        # Case 3: Regular object with attributes — extract __dict__
        if hasattr(record, "__dict__"):
            return dict(record.__dict__)

        raise TypeError(
            f"Unsupported Finnhub API record type: {type(record)}"
        )

    @classmethod
    def _serialize_records(cls, records: list[Any]) -> list[dict]:
        """
        Convert a list of Finnhub API response objects into dictionaries.

        Args:
            records: List of API response objects.

        Returns:
            list[dict]: List of JSON-serializable dictionaries.
        """
        return [
            cls._serialize_record(record)
            for record in records
        ]

    # =========================================================
    # 1. STOCK SYMBOLS LIST (TICKER REFERENCE)
    # =========================================================

    def ingest_stocks_list(
        self,
        exchange: str,
        execution_start_time: datetime,
        run_id: str,
    ) -> Optional[Any]:
        """
        Fetch all stock symbols for a given exchange from Finnhub
        and upload the raw data to S3 Bronze layer.

        This method:
            1. Calls Finnhub's stock_symbols() endpoint for the given exchange.
            2. Validates the API response is non-empty.
            3. Serializes the response into JSON format.
            4. Builds an S3 key following the Bronze partition convention.
            5. Uploads the raw JSON payload to S3.

        Args:
            exchange: Exchange code to query (e.g., 'US' for US exchanges).
            execution_start_time: Pipeline start timestamp for partition key generation.
            run_id: Unique batch identifier (e.g., 'batch_20260814_053000').

        Returns:
            dict: S3 upload response metadata if successful.
            list: Empty list if API returned no data.
            None: If the ingestion failed entirely.
        """
        logger.info(
            "[INGEST][STOCKS_LIST] Starting ingestion | "
            "exchange=%s | run_id=%s",
            exchange,
            run_id,
        )

        try:
            # -------------------------------------------------
            # Step 1: Fetch stock symbols from Finnhub API
            # -------------------------------------------------
            logger.info(
                "[INGEST][STOCKS_LIST] Fetching stock symbols | "
                "exchange=%s",
                exchange,
            )

            response = []

            try:
                # Finnhub returns a list of dicts with keys:
                # currency, description, displaySymbol, figi, mic, symbol, type
                response = self.finnhub_client.stock_symbols(
                    exchange
                )

            except Exception:
                logger.exception(
                    "[INGEST][STOCKS_LIST] Failed to fetch stock symbols | "
                    "exchange=%s | records=%d | run_id=%s",
                    exchange,
                    len(response),
                    run_id,
                )

                # If we got partial data before the error, continue with it
                if response:
                    logger.warning(
                        "[INGEST][STOCKS_LIST] Continuing with partial response | "
                        "records=%d | run_id=%s",
                        len(response),
                        run_id,
                    )
                else:
                    return None

            logger.info(
                "[INGEST][STOCKS_LIST] Fetch completed | "
                "records=%d | exchange=%s | run_id=%s",
                len(response),
                exchange,
                run_id,
            )

            # -------------------------------------------------
            # Step 2: Validate — ensure we got data back
            # -------------------------------------------------
            if not response:
                logger.warning(
                    "[INGEST][STOCKS_LIST] Empty API response | "
                    "exchange=%s | run_id=%s",
                    exchange,
                    run_id,
                )
                return []

            logger.info(
                "[INGEST][STOCKS_LIST] Response validation completed | "
                "records=%d",
                len(response),
            )

            # -------------------------------------------------
            # Step 3: Serialize response to JSON string
            #
            # Finnhub returns native Python dicts/lists, so we
            # can serialize directly with json.dumps(). We also
            # handle edge cases where the response might be a
            # dataclass or object with __dict__.
            # -------------------------------------------------

            # Handle non-list response types (edge cases)
            if isinstance(response, dict):
                return response

            if is_dataclass(response):
                return asdict(response)

            if hasattr(response, "__dict__"):
                return dict(response.__dict__)

            # Standard case: serialize the list of dicts to a JSON string
            data = json.dumps(response)

            logger.info(
                "[INGEST][STOCKS_LIST] Data serialized | bytes=%d",
                len(data),
            )

            logger.debug(
                "[INGEST][STOCKS_LIST] First record preview: %s",
                data[0],
            )

            # -------------------------------------------------
            # Step 4: Build the S3 Bronze partition key
            #
            # Key format:
            #   stock/bronze/source=finnhub/dataset=ticker_reference/
            #   ingestion_date=YYYY-MM-DD/run_id=batch_xxx/data.json
            # -------------------------------------------------
            ingestion_date = execution_start_time.strftime("%Y-%m-%d")

            bucket_key = (
                f"stock/"
                f"bronze/"
                f"source=finnhub/"
                f"dataset=ticker_reference/"
                f"ingestion_date={ingestion_date}/"
                f"run_id={run_id}/"
                f"data.json"
            )

            logger.debug(
                "[INGEST][STOCKS_LIST] Bronze key=%s",
                bucket_key,
            )

            # -------------------------------------------------
            # Step 5: Upload raw JSON to S3 Bronze layer
            # -------------------------------------------------
            logger.info(
                "[INGEST][STOCKS_LIST] Uploading data to Bronze | "
                "bytes=%d | run_id=%s",
                len(data),
                run_id,
            )

            result = self.loader.upload_raw_to_s3(
                data=data,
                bucket_name=self.bucket_name,
                bucket_key=bucket_key,
            )

            logger.info(
                "[INGEST][STOCKS_LIST] Ingestion completed successfully | "
                "exchange=%s | run_id=%s",
                exchange,
                run_id,
            )

            return result

        except Exception:
            logger.exception(
                "[INGEST][STOCKS_LIST] Ingestion failed | "
                "exchange=%s | run_id=%s",
                exchange,
                run_id,
            )
            return None