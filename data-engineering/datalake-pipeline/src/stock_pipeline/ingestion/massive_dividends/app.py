"""
Massive Dividends Ingestion Lambda — Entry Point.

Standalone Lambda that fetches dividend history per stock symbol from the
Massive (Polygon.io) REST API and lands it in the S3 Bronze layer.

Ingests:
    - list_stocks_dividends(ticker=symbol, ...)  → dataset=dividends

Event Payload (optional):
    {
        "stock_symbols": ["IBM", "AAPL"],   # defaults to ["IBM"]
        "run_id":        "batch_custom_001" # auto-generated if omitted
    }

SAM Handler:
    src.stock_pipeline.ingestion.massive_dividends.app.lambda_handler
"""

import logging
import uuid
from datetime import datetime, timezone

from ...load import StockDataLoader
from ..massive_ingestion import MassiveIngestion
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
# LAMBDA HANDLER
# ============================================================

def lambda_handler(event: dict, context) -> dict:
    """
    AWS Lambda entry point for Massive dividends ingestion.

    Args:
        event (dict): Optional payload with keys:
            - stock_symbols (list[str]): Symbols to ingest. Defaults to ["IBM"].
            - run_id (str): Batch run identifier. Auto-generated UUID if omitted.
        context: AWS Lambda runtime context (unused).

    Returns:
        dict: statusCode, summary, and per-symbol results.
    """

    execution_start_time = datetime.now(timezone.utc)

    logger.info(
        "[LAMBDA][MASSIVE_DIVIDENDS_INGEST] Triggered at %s",
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
        "[LAMBDA][MASSIVE_DIVIDENDS_INGEST] symbols=%s | run_id=%s",
        stock_symbols,
        run_id,
    )

    # ----------------------------------------------------------
    # Initialise dependencies
    # ----------------------------------------------------------

    loader = StockDataLoader(
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
    )

    ingestion = MassiveIngestion(
        loader=loader,
        bucket_name=config.S3_BUCKET_NAME,
    )

    # ----------------------------------------------------------
    # Ingest dividends for each symbol
    # ----------------------------------------------------------

    results = []

    for symbol in stock_symbols:
        try:
            response = ingestion.ingest_dividends(
                datasource="massive",
                symbol=symbol,
                execution_start_time=execution_start_time,
                run_id=run_id,
            )

            status = "SUCCESS" if response is not None else "FAILED"

            logger.info(
                "[LAMBDA][MASSIVE_DIVIDENDS_INGEST] %s | symbol=%s | run_id=%s",
                status,
                symbol,
                run_id,
            )

            results.append(
                {
                    "symbol": symbol,
                    "dataset": "dividends",
                    "status": status,
                    "response": str(response),
                }
            )

        except Exception as e:
            logger.exception(
                "[LAMBDA][MASSIVE_DIVIDENDS_INGEST] FAILED | symbol=%s | run_id=%s | error=%s",
                symbol,
                run_id,
                e,
            )

            results.append(
                {
                    "symbol": symbol,
                    "dataset": "dividends",
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
        "[LAMBDA][MASSIVE_DIVIDENDS_INGEST] Completed | run_id=%s | total=%d succeeded=%d failed=%d",
        run_id,
        total,
        succeeded,
        failed,
    )

    return {
        "statusCode": 200 if failed == 0 else 207,
        "run_id": run_id,
        "summary": {
            "source": "massive",
            "dataset": "dividends",
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
    logger.info("[LOCAL] Running Massive dividends ingestion locally.")
    result = lambda_handler({"stock_symbols": ["IBM"]}, {})
    logger.info("[LOCAL] Done: %s", result)
