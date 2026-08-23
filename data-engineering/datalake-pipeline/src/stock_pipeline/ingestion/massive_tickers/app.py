"""
Massive Tickers Ingestion Lambda — Entry Point.

Standalone Lambda that fetches the active stock ticker reference list from the
Massive (Polygon.io) REST API and lands it in the S3 Bronze layer.

Ingests:
    - list_tickers(market="stocks", active=True, ...)  → dataset=ticker_reference

Event Payload (optional):
    {
        "market":  "stocks",             # defaults to "stocks"
        "active":  true,                 # defaults to true
        "order":   "asc",               # defaults to "asc"
        "limit":   1000,                # defaults to 1000
        "sort":    "ticker",             # defaults to "ticker"
        "run_id":  "batch_custom_001"   # auto-generated if omitted
    }

SAM Handler:
    src.stock_pipeline.ingestion.massive_tickers.app.lambda_handler
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
    AWS Lambda entry point for Massive ticker reference list ingestion.

    Args:
        event (dict): Optional payload — see module docstring for keys.
        context: AWS Lambda runtime context (unused).

    Returns:
        dict: statusCode, summary, and ingestion result.
    """

    execution_start_time = datetime.now(timezone.utc)

    logger.info(
        "[LAMBDA][MASSIVE_TICKERS_INGEST] Triggered at %s",
        execution_start_time.isoformat(),
    )

    # ----------------------------------------------------------
    # Parse event
    # ----------------------------------------------------------

    is_dict = isinstance(event, dict)
    market: str  = event.get("market", "stocks") if is_dict else "stocks"
    active: bool = bool(event.get("active", True)) if is_dict else True
    order: str   = event.get("order", "asc") if is_dict else "asc"
    limit: int   = int(event.get("limit", 1000)) if is_dict else 1000
    sort: str    = event.get("sort", "ticker") if is_dict else "ticker"
    run_id: str  = (
        event.get("run_id", f"batch_{uuid.uuid4().hex[:8]}") if is_dict
        else f"batch_{uuid.uuid4().hex[:8]}"
    )

    logger.info(
        "[LAMBDA][MASSIVE_TICKERS_INGEST] market=%s active=%s limit=%d | run_id=%s",
        market,
        active,
        limit,
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
    # Ingest ticker list
    # ----------------------------------------------------------

    try:
        response = ingestion.ingest_list_tickers(
            datasource="massive",
            market=market,
            active=active,
            order=order,
            limit=limit,
            sort=sort,
            execution_start_time=execution_start_time,
            run_id=run_id,
        )

        status = "SUCCESS" if response is not None else "FAILED"

        logger.info(
            "[LAMBDA][MASSIVE_TICKERS_INGEST] %s | run_id=%s",
            status,
            run_id,
        )

        return {
            "statusCode": 200,
            "run_id": run_id,
            "summary": {
                "source": "massive",
                "dataset": "ticker_reference",
                "market": market,
                "status": status,
            },
            "response": str(response),
        }

    except Exception as e:
        logger.exception(
            "[LAMBDA][MASSIVE_TICKERS_INGEST] FAILED | run_id=%s | error=%s",
            run_id,
            e,
        )

        return {
            "statusCode": 500,
            "run_id": run_id,
            "summary": {
                "source": "massive",
                "dataset": "ticker_reference",
                "market": market,
                "status": "FAILED",
                "error": str(e),
            },
        }


# ============================================================
# LOCAL EXECUTION
# ============================================================

if __name__ == "__main__":
    logger.info("[LOCAL] Running Massive tickers ingestion locally.")
    result = lambda_handler({}, {})
    logger.info("[LOCAL] Done: %s", result)
