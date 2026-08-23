"""
Massive Aggregates Ingestion Lambda — Entry Point.

Standalone Lambda that fetches grouped daily OHLCV aggregate data from the
Massive (Polygon.io) REST API and lands it in the S3 Bronze layer.

Ingests:
    - get_grouped_daily_aggs(from_date)  → dataset=aggregates

Event Payload (optional):
    {
        "stock_symbols": ["IBM", "AAPL"],   # defaults to ["IBM"]
        "from_date":     "2026-08-22",      # defaults to yesterday (UTC)
        "to_date":       "2026-08-22",      # defaults to yesterday (UTC)
        "multiplier":    1,                 # defaults to 1
        "timespan":      "day",             # defaults to "day"
        "run_id":        "batch_001"        # auto-generated if omitted
    }

SAM Handler:
    src.stock_pipeline.ingestion.massive_aggregates.app.lambda_handler
"""

import logging
import uuid
from datetime import datetime, timedelta, timezone

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


def _yesterday_utc() -> str:
    """Return yesterday's date in YYYY-MM-DD format (UTC)."""
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")


# ============================================================
# LAMBDA HANDLER
# ============================================================

def lambda_handler(event: dict, context) -> dict:
    """
    AWS Lambda entry point for Massive daily aggregates ingestion.

    Args:
        event (dict): Optional payload — see module docstring for keys.
        context: AWS Lambda runtime context (unused).

    Returns:
        dict: statusCode, summary, and per-symbol ingestion results.
    """

    execution_start_time = datetime.now(timezone.utc)

    logger.info(
        "[LAMBDA][MASSIVE_AGG_INGEST] Triggered at %s",
        execution_start_time.isoformat(),
    )

    # ----------------------------------------------------------
    # Parse event
    # ----------------------------------------------------------

    is_dict = isinstance(event, dict)
    stock_symbols: list[str] = (
        event.get("stock_symbols", ["IBM"]) if is_dict else ["IBM"]
    )
    from_date: str  = event.get("from_date", _yesterday_utc()) if is_dict else _yesterday_utc()
    to_date: str    = event.get("to_date", _yesterday_utc()) if is_dict else _yesterday_utc()
    multiplier: int = int(event.get("multiplier", 1)) if is_dict else 1
    timespan: str   = event.get("timespan", "day") if is_dict else "day"
    run_id: str     = (
        event.get("run_id", f"batch_{uuid.uuid4().hex[:8]}") if is_dict
        else f"batch_{uuid.uuid4().hex[:8]}"
    )

    logger.info(
        "[LAMBDA][MASSIVE_AGG_INGEST] symbols=%s | from_date=%s | to_date=%s | run_id=%s",
        stock_symbols,
        from_date,
        to_date,
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
    # Ingest aggregates for each symbol
    # ----------------------------------------------------------

    results = []

    for symbol in stock_symbols:
        try:
            response = ingestion.ingest_aggregates(
                datasource="massive",
                symbol=symbol,
                execution_start_time=execution_start_time,
                run_id=run_id,
                multiplier=multiplier,
                timespan=timespan,
                from_date=from_date,
                to_date=to_date,
            )

            status = "SUCCESS" if response is not None else "FAILED"

            logger.info(
                "[LAMBDA][MASSIVE_AGG_INGEST] %s | symbol=%s | run_id=%s",
                status,
                symbol,
                run_id,
            )

            results.append(
                {
                    "symbol": symbol,
                    "dataset": "aggregates",
                    "status": status,
                    "response": str(response),
                }
            )

        except Exception as e:
            logger.exception(
                "[LAMBDA][MASSIVE_AGG_INGEST] FAILED | symbol=%s | run_id=%s | error=%s",
                symbol,
                run_id,
                e,
            )

            results.append(
                {
                    "symbol": symbol,
                    "dataset": "aggregates",
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
        "[LAMBDA][MASSIVE_AGG_INGEST] Completed | run_id=%s | total=%d succeeded=%d failed=%d",
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
            "dataset": "aggregates",
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
    logger.info("[LOCAL] Running Massive aggregates ingestion locally.")
    result = lambda_handler({"stock_symbols": ["IBM"]}, {})
    logger.info("[LOCAL] Done: %s", result)
