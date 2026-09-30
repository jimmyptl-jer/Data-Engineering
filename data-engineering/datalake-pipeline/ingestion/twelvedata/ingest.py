"""
Alpha Vantage Ingestion — Symbol × Endpoint Loop.

`AlphaVantageIngestion.ingest_symbols()` fetches every endpoint in
ALPHA_VANTAGE_ENDPOINTS for every symbol and writes each raw response to
S3 Bronze as .../dataset=<dataset>/.../<SYMBOL>.json. A failing endpoint
is recorded in the results and does not stop the others.
"""

import logging
from datetime import datetime

from bronze import write_bronze_json
from client import fetch_endpoint
from config import DEFAULT_DATASOURCE, TwelveData_Endpoints


logger = logging.getLogger(__name__)


# ============================================================
# ALPHA VANTAGE INGESTION CLASS
# ============================================================


class TwelveDataIngestion:
    """
    Ingests market data from Twelve Data API
    and lands raw JSON into S3 Bronze.
    """

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name

        logger.info(
            "[INGESTION][TWELVEDATA][INITIALIZED] "
            "TwelveDataIngestion initialized | bucket=%s",
            bucket_name,
        )

    # ========================================================
    # INGEST ALL SYMBOLS / ENDPOINTS
    # ========================================================

    def ingest_symbols(
        self,
        stock_symbols: list[str],
        execution_start_time: datetime,
        full_load: bool = False,
        datasource: str = DEFAULT_DATASOURCE,
        run_id: str | None = None,
    ) -> list[dict]:
        """
        Fetch raw market data from Alpha Vantage API for all
        symbols and configured endpoints, then write the raw
        JSON files to the S3 Bronze layer.

        Full Load vs Incremental Strategy:

        - Initial run / full_load=True:
          Uses outputsize='full' for historical daily data.

        - Subsequent runs:
          Uses outputsize='compact' for the latest daily data.

        Returns:
            list[dict]: One entry per symbol/endpoint, with "response"
            (the Bronze key) on success or "error" on failure.
        """
        
        results = []

        # Generate a run ID if one was not supplied
        if not run_id:
            run_id = execution_start_time.strftime(
                "%Y%m%d_%H%M%S"
            )

            logger.info(
                "[INGESTION][TWELVEDATA][RUN_ID_GENERATED] "
                "No run_id supplied, generated new run_id | run_id=%s",
                run_id,
            )

        # ====================================================
        # LOOP THROUGH SYMBOLS
        # ====================================================

        for symbol_index, symbol in enumerate(stock_symbols, start=1):

            # =================================================
            # LOOP THROUGH ENDPOINTS
            # =================================================

            for endpoint in TwelveData_Endpoints:

                function = endpoint["function"]
                dataset = endpoint["dataset"]

                try:

                    response = self.ingest(
                        symbol=symbol,
                        function=function,
                        dataset=dataset,
                        datasource=datasource,
                        execution_start_time=execution_start_time,
                        run_id=run_id,
                        additional_params=self._load_mode_params(
                            symbol=symbol,
                            function=function,
                            full_load=full_load,
                        ),
                    )

                    results.append(
                        {
                            "symbol": symbol,
                            "function": function,
                            "dataset": dataset,
                            "response": response,
                        }
                    )

                except Exception as e:

                    logger.exception(
                        "[INGESTION][TWELVEDATA][ENDPOINT_ERROR] "
                        "Error ingesting endpoint | "
                        "symbol=%s | function=%s | dataset=%s | error=%s",
                        symbol,
                        function,
                        dataset,
                        e,
                    )

                    results.append(
                        {
                            "symbol": symbol,
                            "function": function,
                            "dataset": dataset,
                            "error": str(e),
                        }
                    )

                    logger.warning(
                        "[INGESTION][TWELVEDATA][ENDPOINT_FAILURE_RECORDED] "
                        "Failure recorded in results list | "
                        "symbol=%s | function=%s",
                        symbol,
                        function,
                    )

            logger.info(
                "[INGESTION][TWELVEDATA][SYMBOL_COMPLETE] "
                "Completed symbol ingestion | symbol=%s",
                symbol,
            )

        success_count = len([r for r in results if "error" not in r])
        error_count = len([r for r in results if "error" in r])

        logger.info(
            "[INGESTION][TWELVEDATA][CYCLE_SUMMARY] "
            "Cycle summary | successes=%d | errors=%d",
            success_count,
            error_count,
        )

        logger.info(
            "[INGESTION][TWELVEDATA][CYCLE_COMPLETE] "
            "Ingestion cycle completed | "
            "total_requests_processed=%d | run_id=%s",
            len(results),
            run_id,
        )

        return results

    # ========================================================
    # LOAD MODE (daily time series outputsize)
    # ========================================================

    @staticmethod
    def _load_mode_params(
        symbol: str,
        function: str,
        full_load: bool,
    ) -> dict:
        """
        Extra query parameters for the endpoint.

        TIME_SERIES requires an interval. outputsize is a bar count
        (Twelve Data max 5000, default 30): the maximum on a full load
        (history), 100 otherwise (latest days only).
        """

        additional_params = {}

        if function != "TIME_SERIES":
            return additional_params

        additional_params["interval"] = "1day"

        if full_load:

            additional_params["outputsize"] = 5000

            logger.info(
                "[INGESTION][TWELVEDATA][LOAD_MODE] "
                "Full load selected | "
                "symbol=%s | outputsize=5000",
                symbol,
            )

        else:

            additional_params["outputsize"] = 100

            logger.info(
                "[INGESTION][TWELVEDATA][LOAD_MODE] "
                "Incremental load selected | "
                "symbol=%s | outputsize=100",
                symbol,
            )

        return additional_params

    # ========================================================
    # INGEST SINGLE SYMBOL / ENDPOINT
    # ========================================================

    def ingest(
        self,
        symbol: str,
        function: str,
        dataset: str,
        datasource: str,
        execution_start_time: datetime,
        run_id: str,
        additional_params: dict | None = None,
    ) -> str:
        """
        Fetch API response for a given stock symbol and API
        function, validate payload, and write raw JSON to
        S3 Bronze layer.

        Returns:
            str: The Bronze S3 key written.
        """

        

        data = fetch_endpoint(
            symbol=symbol,
            function=function,
            additional_params=additional_params,
        )

        s3_key = write_bronze_json(
            data,
            bucket_name=self.bucket_name,
            source=datasource,
            dataset=dataset,
            run_id=run_id,
            execution_time=execution_start_time,
            file_name=symbol.upper(),
        )

        logger.info(
            "[INGESTION][ALPHAVANTAGE][INGEST_SUCCESS] "
            "Ingestion completed successfully | "
            "symbol=%s | dataset=%s | "
            "run_id=%s | s3_key=%s",
            symbol,
            dataset,
            run_id,
            s3_key,
        )

        return s3_key
