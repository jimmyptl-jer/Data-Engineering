import logging
import os
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
            "MassiveIngestion initialized",
            extra={
                "bucket_name": bucket_name,
            },
        )

    # =========================================================
    # 1. EXCHANGES
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
            "Starting Massive exchanges ingestion",
            extra={
                "datasource": datasource,
                "run_id": run_id,
            },
        )

        try:

            # -------------------------------------------------
            # Step 1: Fetch
            # -------------------------------------------------

            response = self.client.get_exchanges(
                asset_class="stocks",
                locale="us",
            )

            # -------------------------------------------------
            # Step 2: Validate
            # -------------------------------------------------

            if not response:
                logger.warning(
                    "Massive returned no exchange data",
                    extra={
                        "datasource": datasource,
                        "run_id": run_id,
                    },
                )

                return []

            # -------------------------------------------------
            # Step 3: Store response
            # -------------------------------------------------

            data = response

            # -------------------------------------------------
            # Step 4: Bronze key
            # -------------------------------------------------

            ingestion_date = execution_start_time.strftime(
                "%Y-%m-%d"
            )

            bucket_key = (
                f"stock/"
                f"bronze/"
                f"source={datasource}/"
                f"dataset=exchanges/"
                f"ingestion_date={ingestion_date}/"
                f"run_id={run_id}/"
                f"data.json"
            )

            # -------------------------------------------------
            # Step 5: Upload
            # -------------------------------------------------

            return self.loader.upload_raw_to_s3(
                data=data,
                bucket_name=self.bucket_name,
                bucket_key=bucket_key,
            )

        except Exception:
            logger.exception(
                "Failed to ingest Massive exchanges",
                extra={
                    "datasource": datasource,
                    "run_id": run_id,
                },
            )

            return None

    # =========================================================
    # 2. AGGREGATES / OHLC
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
            "Starting Massive aggregates ingestion",
            extra={
                "symbol": symbol,
                "multiplier": multiplier,
                "timespan": timespan,
                "from_date": from_date,
                "to_date": to_date,
                "run_id": run_id,
            },
        )

        try:

            # -------------------------------------------------
            # Step 1: Fetch
            # -------------------------------------------------

            response = self.client.get_grouped_daily_aggs(
                from_date,
                adjusted="true",
            )

            # -------------------------------------------------
            # Step 2: Validate
            # -------------------------------------------------

            if not response:
                logger.warning(
                    "Massive returned no aggregate data",
                    extra={
                        "symbol": symbol,
                        "run_id": run_id,
                    },
                )

                return []

            # -------------------------------------------------
            # Step 3: Store response
            # -------------------------------------------------

            data = response

            # -------------------------------------------------
            # Step 4: Bronze key
            # -------------------------------------------------

            ingestion_date = execution_start_time.strftime(
                "%Y-%m-%d"
            )

            bucket_key = (
                f"stock/"
                f"bronze/"
                f"source={datasource}/"
                f"dataset=aggregates/"
                f"symbol={symbol}/"
                f"ingestion_date={ingestion_date}/"
                f"run_id={run_id}/"
                f"data.json"
            )

            # -------------------------------------------------
            # Step 5: Upload
            # -------------------------------------------------

            return self.loader.upload_raw_to_s3(
                data=data,
                bucket_name=self.bucket_name,
                bucket_key=bucket_key,
            )

        except Exception:
            logger.exception(
                "Failed to ingest Massive aggregates",
                extra={
                    "symbol": symbol,
                    "run_id": run_id,
                },
            )

            return None


    # =========================================================
    # 6. SPLITS
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
            "Starting Massive splits ingestion",
            extra={
                "symbol": symbol,
                "run_id": run_id,
            },
        )

        try:

            # -------------------------------------------------
            # Step 1: Fetch
            # -------------------------------------------------

            splits = []

            for split in self.client.list_stocks_splits(
                ticker=symbol,
                limit="100",
                sort="execution_date.desc",
            ):
                splits.append(split)

            response = splits

            # -------------------------------------------------
            # Step 2: Validate
            # -------------------------------------------------

            if not response:
                logger.warning(
                    "Massive returned no split data",
                    extra={
                        "symbol": symbol,
                        "run_id": run_id,
                    },
                )

                return []

            # -------------------------------------------------
            # Step 3: Store response
            # -------------------------------------------------

            data = response

            # -------------------------------------------------
            # Step 4: Bronze key
            # -------------------------------------------------

            ingestion_date = execution_start_time.strftime(
                "%Y-%m-%d"
            )

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

            # -------------------------------------------------
            # Step 5: Upload
            # -------------------------------------------------

            return self.loader.upload_raw_to_s3(
                data=data,
                bucket_name=self.bucket_name,
                bucket_key=bucket_key,
            )

        except Exception:
            logger.exception(
                "Failed to ingest Massive splits",
                extra={
                    "symbol": symbol,
                    "run_id": run_id,
                },
            )

            return None

    # =========================================================
    # 7. DIVIDENDS
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
            "Starting Massive dividends ingestion",
            extra={
                "symbol": symbol,
                "run_id": run_id,
            },
        )

        try:

            # -------------------------------------------------
            # Step 1: Fetch
            # -------------------------------------------------

            dividends = []

            for dividend in self.client.list_stocks_dividends(
                ticker=symbol,
                limit="500",
                sort="ticker.asc",
            ):
                dividends.append(dividend)

            response = dividends

            # -------------------------------------------------
            # Step 2: Validate
            # -------------------------------------------------

            if not response:
                logger.warning(
                    "Massive returned no dividend data",
                    extra={
                        "symbol": symbol,
                        "run_id": run_id,
                    },
                )

                return []

            # -------------------------------------------------
            # Step 3: Store response
            # -------------------------------------------------

            data = response

            # -------------------------------------------------
            # Step 4: Bronze key
            # -------------------------------------------------

            ingestion_date = execution_start_time.strftime(
                "%Y-%m-%d"
            )

            bucket_key = (
                f"stock/"
                f"bronze/"
                f"source={datasource}/"
                f"dataset=dividends/"
                f"symbol={symbol}/"
                f"ingestion_date={ingestion_date}/"
                f"run_id={run_id}/"
                f"data.json"
            )

            # -------------------------------------------------
            # Step 5: Upload
            # -------------------------------------------------

            return self.loader.upload_raw_to_s3(
                data=data,
                bucket_name=self.bucket_name,
                bucket_key=bucket_key,
            )

        except Exception:
            logger.exception(
                "Failed to ingest Massive dividends",
                extra={
                    "symbol": symbol,
                    "run_id": run_id,
                },
            )

            return None

    # =========================================================
    # 10. TICKER REFERENCE
    # =========================================================

    def ingest_list_tickers(
        self,
        datasource: str,
        execution_start_time: datetime,
        run_id: str,
    ) -> Optional[Any]:
        """
        Fetch ticker/reference data from Massive
        and upload it to Bronze.
        """

        logger.info(
            "Starting Massive ticker reference ingestion",
            extra={
                "run_id": run_id,
            },
        )

        try:

            # -------------------------------------------------
            # Step 1: Fetch
            # -------------------------------------------------

            response = []

            for ticker in self.client.list_tickers(
                market="stocks",
                active=True,
                order="asc",
                limit=1000,
                sort="ticker",
            ):
                response.append(ticker)
                
            print(response)

            # -------------------------------------------------
            # Step 2: Validate
            # -------------------------------------------------

            if not response:
                logger.warning(
                    "Massive returned no ticker reference data",
                    extra={
                        "run_id": run_id,
                    },
                )

                return []

            logger.info(
                "Successfully fetched ticker reference data",
                extra={
                    "run_id": run_id,
                    "ticker_count": len(response),
                },
            )

            # -------------------------------------------------
            # Step 3: Store response
            # -------------------------------------------------

            data = response

            # -------------------------------------------------
            # Step 4: Bronze key
            # -------------------------------------------------

            ingestion_date = execution_start_time.strftime(
                "%Y-%m-%d"
            )

            bucket_key = (
                f"stock/"
                f"bronze/"
                f"source={datasource}/"
                f"dataset=ticker_reference/"
                f"ingestion_date={ingestion_date}/"
                f"run_id={run_id}/"
                f"data.json"
            )

            # -------------------------------------------------
            # Step 5: Upload
            # -------------------------------------------------

            return self.loader.upload_raw_to_s3(
                data=data,
                bucket_name=self.bucket_name,
                bucket_key=bucket_key,
            )

        except Exception:
            logger.exception(
                "Failed to ingest Massive ticker reference data",
                extra={
                    "run_id": run_id,
                },
            )

            return None
 