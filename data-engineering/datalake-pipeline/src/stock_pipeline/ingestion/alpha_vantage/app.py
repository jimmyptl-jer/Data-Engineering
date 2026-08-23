"""
Alpha Vantage Ingestion Lambda — Entry Point.

Standalone Lambda that fetches raw market data from the Alpha Vantage REST API
and lands it in the S3 Bronze layer.

Ingests the following endpoints for every symbol in the event payload:
    - TIME_SERIES_DAILY  → dataset=daily_time_series
    - OVERVIEW           → dataset=company_overview

Event Payload (optional):
    {
        "stock_symbols": ["IBM", "AAPL"],
        "run_id": "batch_custom_001"          # optional — auto-generated if omitted
    }

SAM Handler:
    src.stock_pipeline.ingestion.alpha_vantage.app.lambda_handler
"""

import logging
import os
import uuid
from datetime import datetime, timezone

import requests

from ...load import StockDataLoader
from ..alpha_vantage_ingestion import AlphaVantageIngestion
from ... import config


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# LIGHTWEIGHT EXTRACTOR
# Thin HTTP wrapper so AlphaVantageIngestion can call
# fetch_alpha_vantage_api_data() without a PySpark session.
# ============================================================

_ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"


class _LightweightExtractor:
    """
    Minimal extractor for standalone Lambda use.
    Replaces StockDataExtractor (PySpark) with a plain requests.get call.
    """

    def fetch_alpha_vantage_api_data(self, params: dict) -> dict:
        symbol = params.get("symbol")
        function = params.get("function")

        logger.info(
            "[EXTRACT][AV_HTTP] GET symbol=%s function=%s",
            symbol,
            function,
        )

        response = requests.get(
            _ALPHA_VANTAGE_BASE_URL,
            params=params,
            timeout=10,
        )
        response.raise_for_status()
        return response.json()


# ============================================================
# LAMBDA HANDLER
# ============================================================

def lambda_handler(event: dict, context) -> dict:
    """
    AWS Lambda entry point for Alpha Vantage ingestion.

    Args:
        event (dict): Optional payload with keys:
            - stock_symbols (list[str]): Symbols to ingest. Defaults to ["IBM"].
            - run_id (str): Batch run identifier. Auto-generated UUID if omitted.
        context: AWS Lambda runtime context (unused).

    Returns:
        dict: statusCode, summary, and per-symbol/endpoint results.
    """

    execution_start_time = datetime.now(timezone.utc)

    logger.info(
        "[LAMBDA][AV_INGEST] Triggered at %s",
        execution_start_time.isoformat(),
    )

    # ----------------------------------------------------------
    # Parse event
    # ----------------------------------------------------------

    is_dict = isinstance(event, dict)
    stock_symbols: list[str] = (
        event.get("stock_symbols", ["IBM"]) if is_dict else ["IBM"]
    )
    run_id: str = (
        event.get("run_id", f"batch_{uuid.uuid4().hex[:8]}") if is_dict
        else f"batch_{uuid.uuid4().hex[:8]}"
    )

    logger.info(
        "[LAMBDA][AV_INGEST] symbols=%s | run_id=%s",
        stock_symbols,
        run_id,
    )

    # ----------------------------------------------------------
    # Initialise dependencies (no PySpark required)
    # ----------------------------------------------------------

    loader = StockDataLoader(
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
    )

    ingestion = AlphaVantageIngestion(
        extractor=_LightweightExtractor(),
        loader=loader,
        bucket_name=config.S3_BUCKET_NAME,
    )

    # ----------------------------------------------------------
    # Ingest every endpoint for every symbol
    # ----------------------------------------------------------

    results = []

    for symbol in stock_symbols:
        for endpoint in config.ALPHA_VANTAGE_ENDPOINTS:
            function = endpoint["function"]
            dataset = endpoint["dataset"]

            try:
                response = ingestion.ingest(
                    symbol=symbol,
                    function=function,
                    dataset=dataset,
                    datasource="alphavantage",
                    execution_start_time=execution_start_time,
                    run_id=run_id,
                )

                logger.info(
                    "[LAMBDA][AV_INGEST] SUCCESS | symbol=%s function=%s",
                    symbol,
                    function,
                )

                results.append(
                    {
                        "symbol": symbol,
                        "function": function,
                        "dataset": dataset,
                        "status": "SUCCESS",
                        "response": str(response),
                    }
                )

            except Exception as e:
                logger.exception(
                    "[LAMBDA][AV_INGEST] FAILED | symbol=%s function=%s | error=%s",
                    symbol,
                    function,
                    e,
                )

                results.append(
                    {
                        "symbol": symbol,
                        "function": function,
                        "dataset": dataset,
                        "status": "FAILED",
                        "error": str(e),
                    }
                )

    # ----------------------------------------------------------
    # Summary
    # ----------------------------------------------------------

    total = len(results)
    succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
    failed = total - succeeded

    logger.info(
        "[LAMBDA][AV_INGEST] Completed | run_id=%s | total=%d succeeded=%d failed=%d",
        run_id,
        total,
        succeeded,
        failed,
    )

    return {
        "statusCode": 200 if failed == 0 else 207,
        "run_id": run_id,
        "summary": {
            "total": total,
            "succeeded": succeeded,
            "failed": failed,
        },
        "results": results,
    }


# ============================================================
# LOCAL EXECUTION
# ============================================================

if __name__ == "__main__":
    logger.info("[LOCAL] Running Alpha Vantage ingestion locally.")
    result = lambda_handler({"stock_symbols": ["IBM"]}, {})
    logger.info("[LOCAL] Done: %s", result)
