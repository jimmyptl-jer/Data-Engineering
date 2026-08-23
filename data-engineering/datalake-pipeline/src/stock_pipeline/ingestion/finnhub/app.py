"""
Finnhub Ingestion Lambda — Entry Point.

Standalone Lambda that fetches raw stock ticker reference data from the
Finnhub REST API and lands it in the S3 Bronze layer.

Ingests the following endpoint:
    - stock_symbols(exchange="US")  → dataset=ticker_reference

Event Payload (optional):
    {
        "exchange": "US",                     # defaults to "US"
        "run_id": "batch_custom_001"          # optional — auto-generated if omitted
    }

SAM Handler:
    src.stock_pipeline.ingestion.finnhub.app.lambda_handler
"""

import logging
import uuid
from datetime import datetime, timezone

from ...load import StockDataLoader
from ..finnhub_ingestion import FinnHubIngestion
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
    AWS Lambda entry point for Finnhub ingestion.

    Args:
        event (dict): Optional payload with keys:
            - exchange (str): Exchange code to fetch. Defaults to "US".
            - run_id (str): Batch run identifier. Auto-generated UUID if omitted.
        context: AWS Lambda runtime context (unused).

    Returns:
        dict: statusCode, summary, and ingestion result.
    """

    execution_start_time = datetime.now(timezone.utc)

    logger.info(
        "[LAMBDA][FINNHUB_INGEST] Triggered at %s",
        execution_start_time.isoformat(),
    )

    # ----------------------------------------------------------
    # Parse event
    # ----------------------------------------------------------

    is_dict = isinstance(event, dict)
    exchange: str = event.get("exchange", "US") if is_dict else "US"
    run_id: str = (
        event.get("run_id", f"batch_{uuid.uuid4().hex[:8]}") if is_dict
        else f"batch_{uuid.uuid4().hex[:8]}"
    )

    logger.info(
        "[LAMBDA][FINNHUB_INGEST] exchange=%s | run_id=%s",
        exchange,
        run_id,
    )

    # ----------------------------------------------------------
    # Initialise dependencies
    # ----------------------------------------------------------

    loader = StockDataLoader(
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
    )

    ingestion = FinnHubIngestion(
        loader=loader,
        bucket_name=config.S3_BUCKET_NAME,
    )

    # ----------------------------------------------------------
    # Ingest ticker reference for the requested exchange
    # ----------------------------------------------------------

    try:
        response = ingestion.ingest_stocks_list(
            exchange=exchange,
            execution_start_time=execution_start_time,
            run_id=run_id,
        )

        status = "SUCCESS" if response is not None else "FAILED"

        logger.info(
            "[LAMBDA][FINNHUB_INGEST] %s | exchange=%s | run_id=%s",
            status,
            exchange,
            run_id,
        )

        return {
            "statusCode": 200,
            "run_id": run_id,
            "summary": {
                "source": "finnhub",
                "dataset": "ticker_reference",
                "exchange": exchange,
                "status": status,
            },
            "response": str(response),
        }

    except Exception as e:
        logger.exception(
            "[LAMBDA][FINNHUB_INGEST] FAILED | exchange=%s | run_id=%s | error=%s",
            exchange,
            run_id,
            e,
        )

        return {
            "statusCode": 500,
            "run_id": run_id,
            "summary": {
                "source": "finnhub",
                "dataset": "ticker_reference",
                "exchange": exchange,
                "status": "FAILED",
                "error": str(e),
            },
        }


# ============================================================
# LOCAL EXECUTION
# ============================================================

if __name__ == "__main__":
    logger.info("[LOCAL] Running Finnhub ingestion locally.")
    result = lambda_handler({}, {})
    logger.info("[LOCAL] Done: %s", result)
