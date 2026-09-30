"""
Finnhub Ingestion Lambda — Entry Point.

SAM Handler: app.lambda_handler

Fetches stock ticker reference data from the Finnhub REST API
(/stock/symbol) and stores the raw JSON response in the S3 Bronze layer
as .../dataset=ticker_reference/.../<exchange>.json.

Event (both fields optional):
    {
        "exchange": "US",                    # default US
        "run_id": "batch_20260930_140000"    # Bronze run_id= partition; generated if absent
    }

Response:
    statusCode 200 on success, 500 on failure (details in "summary").

Files in this Lambda:
    app.py      lambda_handler: resolves the event, fetches, writes Bronze, builds the response
    client.py   fetch_stock_symbols(): GET /stock/symbol
    config.py   environment variables, base URL, Bronze source/dataset names
    bronze.py   Bronze S3 key layout + JSON writer
"""

import logging
import uuid
from datetime import datetime, timezone

import config
from bronze import write_bronze_json
from client import fetch_stock_symbols


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
# WRITE TO S3
# ============================================================

def write_to_s3(
    data: list,
    exchange: str,
    run_id: str,
    execution_time: datetime,
) -> str:
    """
    Write raw Finnhub response to S3 Bronze as <exchange>.json.

    Returns:
        S3 object key.
    """

    return write_bronze_json(
        data,
        bucket_name=config.S3_BUCKET_NAME,
        source=config.DATASOURCE,
        dataset=config.DATASET,
        run_id=run_id,
        execution_time=execution_time,
        file_name=exchange,
    )


# ============================================================
# LAMBDA HANDLER
# ============================================================

def lambda_handler(event, context):
    """
    AWS Lambda entry point.
    """

    execution_start_time = datetime.now(timezone.utc)

    logger.info(
        "[LAMBDA][FINNHUB] Started | timestamp=%s",
        execution_start_time.isoformat(),
    )

    # --------------------------------------------------------
    # Parse event
    # --------------------------------------------------------

    if isinstance(event, dict):

        exchange = event.get(
            "exchange",
            config.DEFAULT_EXCHANGE,
        )

        run_id = event.get(
            "run_id",
            f"batch_{uuid.uuid4().hex[:8]}",
        )

    else:

        exchange = config.DEFAULT_EXCHANGE
        run_id = f"batch_{uuid.uuid4().hex[:8]}"

    logger.info(
        "[LAMBDA][FINNHUB] exchange=%s | run_id=%s",
        exchange,
        run_id,
    )

    # --------------------------------------------------------
    # Fetch + Write
    # --------------------------------------------------------

    try:

        # Fetch from Finnhub
        data = fetch_stock_symbols(
            exchange=exchange,
        )

        # Write raw response to S3
        s3_key = write_to_s3(
            data=data,
            exchange=exchange,
            run_id=run_id,
            execution_time=execution_start_time,
        )

        logger.info(
            "[LAMBDA][FINNHUB] SUCCESS | "
            "exchange=%s | run_id=%s",
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
                "record_count": len(data),
                "status": "SUCCESS",
                "s3_bucket": config.S3_BUCKET_NAME,
                "s3_key": s3_key,
            },
        }

    except Exception as e:

        logger.exception(
            "[LAMBDA][FINNHUB] FAILED | "
            "exchange=%s | run_id=%s",
            exchange,
            run_id,
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
#
# Usage (from ingestion/finnhub/):
#   python app.py
# ============================================================

if __name__ == "__main__":

    logger.info(
        "[LOCAL] Running Finnhub ingestion locally."
    )

    result = lambda_handler(
        {
            "exchange": "US",
        },
        None,
    )

    logger.info(
        "[LOCAL] Result: %s",
        result,
    )
