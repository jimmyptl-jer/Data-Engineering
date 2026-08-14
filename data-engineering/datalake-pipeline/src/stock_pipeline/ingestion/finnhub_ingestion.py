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
            "[INGESTION][FINNHUB][INIT] FinnHubIngestion initialized | bucket=%s",
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
        """

        # ============================================================
        # STEP 1: START INGESTION
        # ============================================================

        print("\n" + "=" * 80)
        print("STEP 1: START FINNHUB INGESTION")
        print("=" * 80)

        logger.info(
            "[INGESTION][FINNHUB] Starting ingestion | exchange=%s | run_id=%s",
            exchange,
            run_id,
        )

        try:

            # ========================================================
            # STEP 2: FETCH DATA FROM FINNHUB
            # ========================================================

            print("\n" + "=" * 80)
            print("STEP 2: FETCH STOCK SYMBOLS FROM FINNHUB")
            print("=" * 80)

            logger.info(
                "[INGESTION][FINNHUB] Fetching stock symbols | exchange=%s",
                exchange,
            )

            response = []

            try:
                response = self.finnhub_client.stock_symbols(
                    exchange
                )

            except Exception:

                print("\n" + "!" * 80)
                print("FINNHUB API FETCH FAILED")
                print("!" * 80)

                logger.exception(
                    "[INGESTION][FINNHUB][ERROR] Failed to fetch stock symbols | exchange=%s | run_id=%s",
                    exchange,
                    run_id,
                )

                if response:
                    logger.warning(
                        "[INGESTION][FINNHUB][WARNING] Continuing with partial response | records=%d | run_id=%s",
                        len(response),
                        run_id,
                    )
                else:
                    return None

            print("\n" + "-" * 80)
            print("FINNHUB API FETCH COMPLETED")
            print(f"Records received: {len(response)}")
            print("-" * 80)

            logger.info(
                "[INGESTION][FINNHUB] Fetch completed | records=%d | exchange=%s | run_id=%s",
                len(response),
                exchange,
                run_id,
            )

            # ========================================================
            # STEP 3: VALIDATE RESPONSE
            # ========================================================

            print("\n" + "=" * 80)
            print("STEP 3: VALIDATE FINNHUB RESPONSE")
            print("=" * 80)

            if not response:
                logger.warning(
                    "[INGESTION][FINNHUB][WARNING] Empty API response | exchange=%s | run_id=%s",
                    exchange,
                    run_id,
                )
                return []

            print("\n" + "-" * 80)
            print("RESPONSE VALIDATION PASSED")
            print(f"Records validated: {len(response)}")
            print("-" * 80)

            logger.info(
                "[INGESTION][FINNHUB] Response validation completed | records=%d",
                len(response),
            )

            # ========================================================
            # STEP 4: PREPARE DATA
            # ========================================================

            print("\n" + "=" * 80)
            print("STEP 4: PREPARE DATA FOR S3")
            print("=" * 80)

            if isinstance(response, dict):
                return response

            if is_dataclass(response):
                return asdict(response)

            if hasattr(response, "__dict__"):
                return dict(response.__dict__)

            data = response

            print("\n" + "-" * 80)
            print("DATA PREPARATION COMPLETED")
            print(f"Records prepared: {len(data)}")
            print("-" * 80)

            logger.info(
                "[INGESTION][FINNHUB] Data prepared | records=%d",
                len(data),
            )

            if data:
                logger.debug(
                    "[INGESTION][FINNHUB] First record preview: %s",
                    data[0],
                )

            # ========================================================
            # STEP 5: BUILD S3 BRONZE KEY
            # ========================================================

            print("\n" + "=" * 80)
            print("STEP 5: BUILD BRONZE S3 KEY")
            print("=" * 80)

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
                "[INGESTION][FINNHUB] Bronze key=%s",
                bucket_key,
            )

            # ========================================================
            # STEP 6: UPLOAD DATA TO S3
            # ========================================================

            print("\n" + "=" * 80)
            print("STEP 6: UPLOAD DATA TO S3 BRONZE")
            print("=" * 80)

            logger.info(
                "[INGESTION][FINNHUB] Uploading data to Bronze | records=%d | run_id=%s",
                len(data),
                run_id,
            )

            result = self.loader.upload_raw_to_s3(
                data=data,
                bucket_name=self.bucket_name,
                bucket_key=bucket_key,
            )

            # ========================================================
            # STEP 7: INGESTION COMPLETED
            # ========================================================

            print("\n" + "=" * 80)
            print("FINNHUB INGESTION COMPLETED SUCCESSFULLY")
            print("=" * 80)

            logger.info(
                "[INGESTION][FINNHUB] Ingestion completed successfully | exchange=%s | run_id=%s",
                exchange,
                run_id,
            )

            return result

        except Exception:

            print("\n" + "!" * 80)
            print("FINNHUB INGESTION FAILED")
            print("!" * 80)

            logger.exception(
                "[INGESTION][FINNHUB][ERROR] Ingestion failed | exchange=%s | run_id=%s",
                exchange,
                run_id,
            )

            return None