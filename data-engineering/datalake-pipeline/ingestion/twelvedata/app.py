"""
Alpha Vantage Ingestion Lambda — Entry Point.

SAM Handler: app.lambda_handler

Fetches every endpoint in config.ALPHA_VANTAGE_ENDPOINTS (daily time series,
company overview) for the requested symbols and lands the raw JSON payloads
in the S3 Bronze layer.

Event (all fields optional):
    {
        "stock_symbols": ["AAPL", "MSFT"],     # else STOCK_SYMBOLS env, else ["IBM"]
        "full_load": false,                    # true → outputsize=full for daily time series
        "datasource": "alphavantage",          # Bronze source= partition
        "run_id": "batch_20260930_140000"      # Bronze run_id= partition; generated if absent
    }

Response:
    statusCode 200 when every symbol/endpoint succeeded, 207 when some
    failed, 500 when the invocation itself failed.

Files in this Lambda:
    app.py      lambda_handler: resolves the event, runs the ingestion, builds the response
    ingest.py   AlphaVantageIngestion: loops symbols × endpoints, one Bronze file per response
    client.py   fetch_endpoint(): one HTTP call to Alpha Vantage + response validation
    keys.py     API key pool and random key selection
    config.py   S3 bucket, base URL and the endpoint → dataset mapping
    bronze.py   Bronze S3 key layout + JSON writer
"""

import json
import logging
import os
from datetime import datetime, timezone

import config
from ingest import TwelveDataIngestion


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s %(message)s",
)

# The Lambda runtime leaves the root logger at WARNING; keep INFO logs.
logging.getLogger().setLevel(logging.INFO)

logger = logging.getLogger(__name__)


# ============================================================
# EVENT → STOCK SYMBOLS
# ============================================================

def resolve_stock_symbols(event: dict) -> list[str]:
    """
    Symbols from the event, else the STOCK_SYMBOLS environment variable
    (comma-separated), else config.DEFAULT_STOCK_SYMBOLS.
    """

    stock_symbols = event.get("stock_symbols")

    if not stock_symbols:

        env_symbols = os.environ.get("STOCK_SYMBOLS", "")

        stock_symbols = [
            symbol.strip()
            for symbol in env_symbols.split(",")
            if symbol.strip()
        ]

        logger.info(
            "[INGESTION][ALPHAVANTAGE][LAMBDA_SYMBOLS_FALLBACK] "
            "No stock_symbols in event, falling back to "
            "STOCK_SYMBOLS environment variable | symbols=%s",
            stock_symbols,
        )

    if not stock_symbols:

        stock_symbols = list(config.DEFAULT_STOCK_SYMBOLS)

        logger.info(
            "[INGESTION][ALPHAVANTAGE][LAMBDA_SYMBOLS_DEFAULT] "
            "No stock_symbols in event or environment, "
            "falling back to default symbol list | symbols=%s",
            stock_symbols,
        )

    return stock_symbols


# ============================================================
# LAMBDA HANDLER
# ============================================================

def lambda_handler(event, context):
    """
    AWS Lambda entrypoint for the Alpha Vantage ingestion job.
    """

    logger.info(
        "[INGESTION][ALPHAVANTAGE][LAMBDA_INVOCATION_START] "
        "Lambda handler invoked | "
        "request_id=%s",
        getattr(context, "aws_request_id", None),
    )

    logger.debug(
        "[INGESTION][ALPHAVANTAGE][LAMBDA_EVENT] "
        "Raw event payload received | event=%s",
        event,
    )

    # Naive UTC timestamp (same value as the former datetime.utcnow())
    execution_start_time = datetime.now(timezone.utc).replace(tzinfo=None)

    logger.info(
        "[INGESTION][ALPHAVANTAGE][LAMBDA_EXECUTION_TIME] "
        "Execution start time recorded | "
        "execution_start_time=%s",
        execution_start_time.isoformat(),
    )

    try:

        # ====================================================
        # RESOLVE INPUT PARAMETERS
        # ====================================================

        event = event or {}

        stock_symbols = resolve_stock_symbols(event)

        full_load = bool(event.get("full_load", False))
        datasource = event.get("datasource", config.DEFAULT_DATASOURCE)
        run_id = event.get("run_id")

        logger.info(
            "[INGESTION][ALPHAVANTAGE][LAMBDA_PARAMS] "
            "Resolved lambda parameters | "
            "symbols=%d | full_load=%s | datasource=%s | run_id=%s",
            len(stock_symbols),
            full_load,
            datasource,
            run_id,
        )

        # ====================================================
        # RUN INGESTION CYCLE
        # ====================================================

        ingestion = TwelveDataIngestion(
            bucket_name=config.S3_BUCKET_NAME,
        )

        results = ingestion.ingest_symbols(
            stock_symbols=stock_symbols,
            execution_start_time=execution_start_time,
            full_load=full_load,
            datasource=datasource,
            run_id=run_id,
        )

        error_count = len([r for r in results if "error" in r])
        success_count = len(results) - error_count

        logger.info(
            "[INGESTION][ALPHAVANTAGE][LAMBDA_INVOCATION_SUCCESS] "
            "Lambda handler completed | "
            "total=%d | successes=%d | errors=%d",
            len(results),
            success_count,
            error_count,
        )

        return {
            "statusCode": 200 if error_count == 0 else 207,
            "body": json.dumps(
                {
                    "message": "Ingestion cycle completed.",
                    "total_requests": len(results),
                    "successes": success_count,
                    "errors": error_count,
                }
            ),
        }

    except Exception as e:

        logger.exception(
            "[INGESTION][ALPHAVANTAGE][LAMBDA_INVOCATION_ERROR] "
            "Unhandled error during lambda invocation | error=%s",
            e,
        )

        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "message": "Ingestion cycle failed.",
                    "error": str(e),
                }
            ),
        }
