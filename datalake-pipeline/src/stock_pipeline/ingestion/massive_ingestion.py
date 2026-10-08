"""
Massive API Ingestion Module.

Handles data acquisition from the Massive (Polygon.io) REST API
into the raw S3 Bronze layer.

Supported Datasets:
    - Exchanges (reference data)
    - Aggregates / OHLCV (grouped daily)
    - Splits (stock split history)
    - Dividends (dividend history)
    - Ticker Reference (active tickers list)
"""

import logging
import os
import time

from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Optional, Any

from massive import RESTClient


logger = logging.getLogger(__name__)


def get_key() -> str:
    """
    Get Massive API key from environment variables.
    """

    api_key = os.getenv("MASSIVE_API_KEY")

    if not api_key:
        raise ValueError(
            "MASSIVE_API_KEY environment variable is not set."
        )

    return api_key


class MassiveIngestion:
    """
    Handles ingestion of raw data from Massive
    into the Bronze layer.
    """

    def __init__(
        self,
        loader,
        bucket_name: str,
    ):
        self.loader = loader
        self.bucket_name = bucket_name

        self.client = RESTClient(get_key())

        logger.info(
            "[INGESTION][MASSIVE][INIT] MassiveIngestion initialized | bucket=%s",
            bucket_name,
        )

    # =========================================================
    # SERIALIZATION
    # =========================================================

    @staticmethod
    def _serialize_record(record: Any) -> dict:
        """
        Convert a Massive SDK model object into a dictionary.

        Massive REST API responses are returned as SDK model objects,
        not native Python dictionaries.

        Args:
            record: Massive API response object.

        Returns:
            dict: Serializable Python dictionary.

        Raises:
            TypeError: If the record cannot be converted to a dictionary.
        """

        
        if isinstance(record, dict):
            return record

        if is_dataclass(record):
            return asdict(record)

        if hasattr(record, "__dict__"):
            return dict(record.__dict__)

        raise TypeError(
            f"Unsupported Massive API record type: {type(record)}"
        )

    @classmethod
    def _serialize_records(cls, records: list[Any]) -> list[dict]:
        """
        Convert a list of Massive SDK objects into dictionaries.
        """

        return [
            cls._serialize_record(record)
            for record in records
        ]

    # =========================================================
    # 1. TICKER REFERENCE
    # =========================================================

    def ingest_list_tickers(
        self,
        datasource: str,
        market: str,
        active: bool,
        order: str,
        limit: int,
        sort: str,
        execution_start_time: datetime,
        run_id: str,
    ) -> Optional[Any]:

        logger.info(
            "[INGEST][MASSIVE_TICKERS] Starting | run_id=%s",
            run_id,
        )

        try:

            # -------------------------------------------------
            # Step 1: Fetch
            # -------------------------------------------------

            logger.info(
                "[INGEST][MASSIVE_TICKERS] "
                "Fetching ticker reference data | "
                "market=%s active=%s limit=%s",
                market,
                active,
                limit,
            )

            response = []

            try:
                for ticker in self.client.list_tickers(
                    market=market,
                    active=active,
                    order=order,
                    limit=limit,
                    sort=sort,
                ):
                    response.append(ticker)

            except Exception:
                logger.exception(
                    "[INGEST][MASSIVE_TICKERS] "
                    "Failed mid-pagination after %d records | run_id=%s",
                    len(response),
                    run_id,
                )

                if response:
                    logger.warning(
                        "[INGEST][MASSIVE_TICKERS] "
                        "Uploading partial results (%d records) instead of discarding",
                        len(response),
                    )
                else:
                    return None

            logger.info(
                "[INGEST][MASSIVE_TICKERS] "
                "Fetch completed | records=%d | run_id=%s",
                len(response),
                run_id,
            )

            # -------------------------------------------------
            # Step 2: Validate
            # -------------------------------------------------

            if not response:
                logger.warning(
                    "[INGEST][MASSIVE_TICKERS] "
                    "API returned empty response | run_id=%s",
                    run_id,
                )
                return []

            logger.info(
                "[INGEST][MASSIVE_TICKERS] "
                "Response validated | ticker_count=%d",
                len(response),
            )

            # -------------------------------------------------
            # Step 3: Serialize
            # -------------------------------------------------

            data = self._serialize_records(response)

            logger.info(
                "[INGEST][MASSIVE_TICKERS] "
                "Serialized %d ticker records.",
                len(data),
            )

            logger.debug(
                "[INGEST][MASSIVE_TICKERS] "
                "First serialized record: %s",
                data[0],
            )

            # -------------------------------------------------
            # Step 4: Bronze key
            # -------------------------------------------------

            ingestion_date = execution_start_time.strftime("%Y-%m-%d")

            bucket_key = (
                f"stock/"
                f"bronze/"
                f"source={datasource}/"
                f"dataset=ticker_reference/"
                f"ingestion_date={ingestion_date}/"
                f"run_id={run_id}/"
                f"data.json"
            )

            logger.info(
                "[INGEST][MASSIVE_TICKERS] Bronze key: %s",
                bucket_key,
            )

            # -------------------------------------------------
            # Step 5: Upload
            # -------------------------------------------------

            logger.info(
                "[INGEST][MASSIVE_TICKERS] "
                "Uploading to S3 Bronze | records=%d",
                len(data),
            )

            result = self.loader.upload_raw_to_s3(
                data=data,
                bucket_name=self.bucket_name,
                bucket_key=bucket_key,
            )

            logger.info(
                "[INGEST][MASSIVE_TICKERS] "
                "Upload completed | run_id=%s | ticker_count=%d",
                run_id,
                len(data),
            )

            return result

        except Exception:
            logger.exception(
                "[INGEST][MASSIVE_TICKERS] Failed | run_id=%s",
                run_id,
            )
            return None

    # =========================================================
    # 2. EXCHANGES
    # =========================================================

    def ingest_exchanges(
        self,
        datasource: str,
        execution_start_time: datetime,
        run_id: str,
    ) -> Optional[Any]:
        """
        Fetch stock exchange reference data from Massive
        and upload it to Bronze.
        """

        logger.info(
            "[INGEST][MASSIVE_EXCHANGES] "
            "Starting | datasource=%s | run_id=%s",
            datasource,
            run_id,
        )

        try:

            # -------------------------------------------------
            # Step 1: Fetch
            # -------------------------------------------------

            logger.info(
                "[INGEST][MASSIVE_EXCHANGES] "
                "Fetching exchange data from API."
            )

            response = self.client.get_exchanges(
                asset_class="stocks",
                locale="us",
            )

            logger.info(
                "[INGEST][MASSIVE_EXCHANGES] "
                "Fetch completed."
            )

            # -------------------------------------------------
            # Step 2: Validate
            # -------------------------------------------------

            if not response:
                logger.warning(
                    "[INGEST][MASSIVE_EXCHANGES] "
                    "API returned empty response | run_id=%s",
                    run_id,
                )
                return []

            logger.info(
                "[INGEST][MASSIVE_EXCHANGES] "
                "Response is non-empty | records=%d",
                len(response),
            )

            # -------------------------------------------------
            # Step 3: Serialize
            # -------------------------------------------------

            logger.info(
                "[INGEST][MASSIVE_EXCHANGES] "
                "Response type: %s",
                type(response),
            )

            logger.info(
                "[INGEST][MASSIVE_EXCHANGES] "
                "First item type: %s",
                type(response[0]),
            )

            data = self._serialize_records(response)

            logger.info(
                "[INGEST][MASSIVE_EXCHANGES] "
                "Serialized %d exchange records.",
                len(data),
            )

            logger.debug(
                "[INGEST][MASSIVE_EXCHANGES] "
                "First serialized record: %s",
                data[0],
            )

            # -------------------------------------------------
            # Step 4: Bronze key
            # -------------------------------------------------

            ingestion_date = execution_start_time.strftime("%Y-%m-%d")

            bucket_key = (
                f"stock/"
                f"bronze/"
                f"source={datasource}/"
                f"dataset=exchanges/"
                f"ingestion_date={ingestion_date}/"
                f"run_id={run_id}/"
                f"data.json"
            )

            logger.info(
                "[INGEST][MASSIVE_EXCHANGES] "
                "Bronze key: %s",
                bucket_key,
            )

            # -------------------------------------------------
            # Step 5: Upload
            # -------------------------------------------------

            logger.info(
                "[INGEST][MASSIVE_EXCHANGES] "
                "Uploading to S3 Bronze."
            )

            result = self.loader.upload_raw_to_s3(
                data=data,
                bucket_name=self.bucket_name,
                bucket_key=bucket_key,
            )

            logger.info(
                "[INGEST][MASSIVE_EXCHANGES] "
                "Upload completed | run_id=%s",
                run_id,
            )

            return result

        except Exception:
            logger.exception(
                "[INGEST][MASSIVE_EXCHANGES] "
                "Failed | datasource=%s | run_id=%s",
                datasource,
                run_id,
            )
            return None

    # =========================================================
    # 3. AGGREGATES / OHLC
    # =========================================================

    def ingest_aggregates(
        self,
        datasource: str,
        symbol: str,
        execution_start_time: datetime,
        run_id: str,
        multiplier: int,
        timespan: str,
        from_date: str,
        to_date: str,
    ) -> Optional[Any]:
        """
        Fetch aggregate OHLCV data from Massive
        and upload it to Bronze.
        """

        logger.info(
            "[INGEST][MASSIVE_AGGREGATES] "
            "Starting | symbol=%s | from_date=%s | "
            "to_date=%s | run_id=%s",
            symbol,
            from_date,
            to_date,
            run_id,
        )

        try:

            # -------------------------------------------------
            # Step 1: Fetch
            # -------------------------------------------------

            logger.info(
                "[INGEST][MASSIVE_AGGREGATES] "
                "Fetching grouped daily aggs | from_date=%s",
                from_date,
            )

            response = self.client.get_grouped_daily_aggs(
                from_date,
                adjusted="true",
            )

            logger.info(
                "[INGEST][MASSIVE_AGGREGATES] "
                "Fetch completed."
            )

            # -------------------------------------------------
            # Step 2: Validate
            # -------------------------------------------------

            if not response:
                logger.warning(
                    "[INGEST][MASSIVE_AGGREGATES] "
                    "API returned empty response | "
                    "symbol=%s | run_id=%s",
                    symbol,
                    run_id,
                )
                return []

            logger.info(
                "[INGEST][MASSIVE_AGGREGATES] "
                "Response is non-empty."
            )

            logger.info(
                "[INGEST][MASSIVE_AGGREGATES] "
                "Response type: %s",
                type(response),
            )

            logger.info(
                "[INGEST][MASSIVE_AGGREGATES] "
                "First item type: %s",
                type(response[0]),
            )

            # -------------------------------------------------
            # Step 3: Serialize
            # -------------------------------------------------

            data = self._serialize_records(response)

            logger.info(
                "[INGEST][MASSIVE_AGGREGATES] "
                "Serialized %d aggregate records.",
                len(data),
            )

            logger.debug(
                "[INGEST][MASSIVE_AGGREGATES] "
                "First serialized record: %s",
                data[0],
            )

            # -------------------------------------------------
            # Step 4: Bronze key
            # -------------------------------------------------

            ingestion_date = execution_start_time.strftime("%Y-%m-%d")

            bucket_key = (
                f"stock/"
                f"bronze/"
                f"source={datasource}/"
                f"dataset=aggregates/"
                f"ingestion_date={ingestion_date}/"
                f"run_id={run_id}/"
                f"{symbol}.json"
            )

            logger.info(
                "[INGEST][MASSIVE_AGGREGATES] "
                "Bronze key: %s",
                bucket_key,
            )

            # -------------------------------------------------
            # Step 5: Upload
            # -------------------------------------------------

            logger.info(
                "[INGEST][MASSIVE_AGGREGATES] "
                "Uploading to S3 Bronze."
            )

            result = self.loader.upload_raw_to_s3(
                data=data,
                bucket_name=self.bucket_name,
                bucket_key=bucket_key,
            )

            logger.info(
                "[INGEST][MASSIVE_AGGREGATES] "
                "Upload completed | symbol=%s | run_id=%s",
                symbol,
                run_id,
            )

            return result

        except Exception:
            logger.exception(
                "[INGEST][MASSIVE_AGGREGATES] "
                "Failed | symbol=%s | run_id=%s",
                symbol,
                run_id,
            )
            return None

    # =========================================================
    # 4. SPLITS
    # =========================================================

    def ingest_splits(
        self,
        datasource: str,
        symbol: str,
        execution_start_time: datetime,
        run_id: str,
    ) -> Optional[Any]:
        """
        Fetch stock split data from Massive
        and upload it to Bronze.
        """

        logger.info(
            "[INGEST][MASSIVE_SPLITS] "
            "Starting | symbol=%s | run_id=%s",
            symbol,
            run_id,
        )

        try:

            # -------------------------------------------------
            # Step 1: Fetch
            # -------------------------------------------------

            logger.info(
                "[INGEST][MASSIVE_SPLITS] "
                "Fetching splits | symbol=%s",
                symbol,
            )

            splits = []

            for split in self.client.list_stocks_splits(
                ticker=symbol,
                limit="100",
                sort="execution_date.desc",
            ):
                splits.append(split)

            response = splits

            logger.info(
                "[INGEST][MASSIVE_SPLITS] "
                "Fetch completed | records=%d",
                len(response),
            )

            # -------------------------------------------------
            # Step 2: Validate
            # -------------------------------------------------

            if not response:
                logger.warning(
                    "[INGEST][MASSIVE_SPLITS] "
                    "API returned empty response | "
                    "symbol=%s | run_id=%s",
                    symbol,
                    run_id,
                )
                return []

            logger.info(
                "[INGEST][MASSIVE_SPLITS] "
                "Response is non-empty | record_count=%d",
                len(response),
            )

            # -------------------------------------------------
            # Step 3: Serialize
            # -------------------------------------------------

            data = self._serialize_records(response)

            logger.info(
                "[INGEST][MASSIVE_SPLITS] "
                "Serialized %d split records.",
                len(data),
            )

            # -------------------------------------------------
            # Step 4: Bronze key
            # -------------------------------------------------

            ingestion_date = execution_start_time.strftime("%Y-%m-%d")

            bucket_key = (
                f"stock/"
                f"bronze/"
                f"source={datasource}/"
                f"dataset=splits/"
                f"symbol={symbol}/"
                f"ingestion_date={ingestion_date}/"
                f"run_id={run_id}/"
                f"data.json"
            )

            logger.info(
                "[INGEST][MASSIVE_SPLITS] "
                "Bronze key: %s",
                bucket_key,
            )

            # -------------------------------------------------
            # Step 5: Upload
            # -------------------------------------------------

            logger.info(
                "[INGEST][MASSIVE_SPLITS] "
                "Uploading to S3 Bronze."
            )

            result = self.loader.upload_raw_to_s3(
                data=data,
                bucket_name=self.bucket_name,
                bucket_key=bucket_key,
            )

            logger.info(
                "[INGEST][MASSIVE_SPLITS] "
                "Upload completed | symbol=%s | run_id=%s",
                symbol,
                run_id,
            )

            return result

        except Exception:
            logger.exception(
                "[INGEST][MASSIVE_SPLITS] "
                "Failed | symbol=%s | run_id=%s",
                symbol,
                run_id,
            )
            return None

    # =========================================================
    # 5. DIVIDENDS
    # =========================================================

    def ingest_dividends(
        self,
        datasource: str,
        symbol: str,
        execution_start_time: datetime,
        run_id: str,
    ) -> Optional[Any]:
        """
        Fetch dividend data from Massive
        and upload it to Bronze.
        """

        logger.info(
            "[INGEST][MASSIVE_DIVIDENDS] "
            "Starting | symbol=%s | run_id=%s",
            symbol,
            run_id,
        )

        try:

            # -------------------------------------------------
            # Step 1: Fetch
            # -------------------------------------------------

            logger.info(
                "[INGEST][MASSIVE_DIVIDENDS] "
                "Fetching dividends | symbol=%s",
                symbol,
            )

            dividends = []

            for dividend in self.client.list_stocks_dividends(
                ticker=symbol,
                limit="500",
                sort="ticker.asc",
            ):
                dividends.append(dividend)

            response = dividends

            logger.info(
                "[INGEST][MASSIVE_DIVIDENDS] "
                "Fetch completed | records=%d",
                len(response),
            )

            # -------------------------------------------------
            # Step 2: Validate
            # -------------------------------------------------

            if not response:
                logger.warning(
                    "[INGEST][MASSIVE_DIVIDENDS] "
                    "API returned empty response | "
                    "symbol=%s | run_id=%s",
                    symbol,
                    run_id,
                )
                return []

            logger.info(
                "[INGEST][MASSIVE_DIVIDENDS] "
                "Response is non-empty | record_count=%d",
                len(response),
            )

            # -------------------------------------------------
            # Step 3: Serialize
            # -------------------------------------------------

            data = self._serialize_records(response)

            logger.info(
                "[INGEST][MASSIVE_DIVIDENDS] "
                "Serialized %d dividend records.",
                len(data),
            )

            logger.debug(
                "[INGEST][MASSIVE_DIVIDENDS] "
                "First serialized record: %s",
                data[0],
            )

            # -------------------------------------------------
            # Step 4: Bronze key
            # -------------------------------------------------

            ingestion_date = execution_start_time.strftime("%Y-%m-%d")

            bucket_key = (
                f"stock/"
                f"bronze/"
                f"source={datasource}/"
                f"dataset=dividends/"
                f"ingestion_date={ingestion_date}/"
                f"run_id={run_id}/"
                f"{symbol}.json"
            )

            logger.info(
                "[INGEST][MASSIVE_DIVIDENDS] "
                "Bronze key: %s",
                bucket_key,
            )

            # -------------------------------------------------
            # Step 5: Upload
            # -------------------------------------------------

            logger.info(
                "[INGEST][MASSIVE_DIVIDENDS] "
                "Uploading to S3 Bronze."
            )

            result = self.loader.upload_raw_to_s3(
                data=data,
                bucket_name=self.bucket_name,
                bucket_key=bucket_key,
            )

            logger.info(
                "[INGEST][MASSIVE_DIVIDENDS] "
                "Upload completed | symbol=%s | run_id=%s",
                symbol,
                run_id,
            )

            return result

        except Exception:
            logger.exception(
                "[INGEST][MASSIVE_DIVIDENDS] "
                "Failed | symbol=%s | run_id=%s",
                symbol,
                run_id,
            )
            return None
        
    def ingest_stock_overview(
        self,
        datasource: str,
        symbol: str,
        execution_start_time: datetime,
        run_id: str,
    ) -> Optional[Any]:
        """
        Fetch stock ticker overview/reference data from Massive
        and upload it to Bronze.
        """

        logger.info(
            "[INGEST][MASSIVE_STOCK_OVERVIEW] "
            "Starting | symbol=%s | run_id=%s",
            symbol,
            run_id,
        )

        try:

            # -------------------------------------------------
            # Step 1: Fetch
            # -------------------------------------------------

            logger.info(
                "[INGEST][MASSIVE_STOCK_OVERVIEW] "
                "Fetching stock overview | symbol=%s",
                symbol,
            )

            stock_overview = self.client.get_ticker_details(
                symbol,
            )

            logger.info(
                "[INGEST][MASSIVE_STOCK_OVERVIEW] "
                "Fetch completed | symbol=%s",
                symbol,
            )


            # -------------------------------------------------
            # Step 2: Validate
            # -------------------------------------------------

            if not stock_overview:
                logger.warning(
                    "[INGEST][MASSIVE_STOCK_OVERVIEW] "
                    "API returned empty response | "
                    "symbol=%s | run_id=%s",
                    symbol,
                    run_id,
                )
                return []

            logger.info(
                "[INGEST][MASSIVE_STOCK_OVERVIEW] "
                "Response is non-empty | symbol=%s",
                symbol,
            )

            # -------------------------------------------------
            # Step 3: Serialize
            # -------------------------------------------------

            data = asdict(stock_overview)

            logger.info(
                "[INGEST][MASSIVE_STOCK_OVERVIEW] "
                "Stock overview serialized | symbol=%s",
                symbol,
            )

            logger.debug(
                "[INGEST][MASSIVE_STOCK_OVERVIEW] "
                "Serialized record: %s",
                data,
            )

            # -------------------------------------------------
            # Step 4: Bronze key
            # -------------------------------------------------

            ingestion_date = execution_start_time.strftime("%Y-%m-%d")

            bucket_key = (
                f"stock/"
                f"bronze/"
                f"source={datasource}/"
                f"dataset=stock_overview/"
                f"ingestion_date={ingestion_date}/"
                f"run_id={run_id}/"
                f"{symbol}.json"
            )

            logger.info(
                "[INGEST][MASSIVE_STOCK_OVERVIEW] "
                "Bronze key: %s",
                bucket_key,
            )

            # -------------------------------------------------
            # Step 5: Upload
            # -------------------------------------------------

            logger.info(
                "[INGEST][MASSIVE_STOCK_OVERVIEW] "
                "Uploading stock overview to S3 Bronze | symbol=%s",
                symbol,
            )

            result = self.loader.upload_raw_to_s3(
                data=data,
                bucket_name=self.bucket_name,
                bucket_key=bucket_key,
            )

            logger.info(
                "[INGEST][MASSIVE_STOCK_OVERVIEW] "
                "Upload completed | symbol=%s | run_id=%s",
                symbol,
                run_id,
            )

            return result

        except Exception:
            logger.exception(
                "[INGEST][MASSIVE_STOCK_OVERVIEW] "
                "Failed | symbol=%s | run_id=%s",
                symbol,
                run_id,
            )
            return None