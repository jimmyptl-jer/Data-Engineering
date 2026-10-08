"""
Finnhub Ingestion Lambda.

Fetches stock ticker reference data from the Finnhub REST API
and stores the raw JSON response in the S3 Bronze layer.

Endpoint:
    /stock/symbol

Event:
    {
        "exchange": "US",
        "run_id": "batch_custom_001"
    }

Both fields are optional.
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone

import boto3
import requests


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# ENVIRONMENT VARIABLES
# ============================================================

FINNHUB_API_KEY = os.environ["FINNHUB_API_KEY"]
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]

FINNHUB_BASE_URL = "https://finnhub.io/api/v1"


# ============================================================
# AWS CLIENT
# ============================================================

s3_client = boto3.client("s3")


# ============================================================
# FETCH FINNHUB DATA
# ============================================================

def fetch_stock_symbols(exchange: str) -> list:
    """
    Fetch stock ticker reference data from Finnhub.

    Endpoint:
        GET /stock/symbol

    Args:
        exchange: Finnhub exchange code, e.g. US.

    Returns:
        List of ticker reference records.
    """

    url = f"{FINNHUB_BASE_URL}/stock/symbol"

    params = {
        "exchange": exchange,
        "token": FINNHUB_API_KEY,
    }

    logger.info(
        "[FINNHUB] Fetching stock symbols | exchange=%s",
        exchange,
    )

    response = requests.get(
        url,
        params=params,
        timeout=60,
    )

    response.raise_for_status()

    data = response.json()

    logger.info(
        "[FINNHUB] Received %s records | exchange=%s",
        len(data) if isinstance(data, list) else 0,
        exchange,
    )

    return data


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
    Write raw Finnhub response to S3 Bronze.

    Returns:
        S3 object key.
    """

    execution_date = execution_time.strftime("%Y-%m-%d")
    execution_timestamp = execution_time.strftime(
        "%Y%m%dT%H%M%SZ"
    )

    s3_key = (
        f"bronze/"
        f"source=finnhub/"
        f"dataset=ticker_reference/"
        f"exchange={exchange}/"
        f"ingestion_date={execution_date}/"
        f"run_id_{run_id}/"
        f"data.json"
    )

    logger.info(
        "[S3] Writing Finnhub data | bucket=%s | key=%s",
        S3_BUCKET_NAME,
        s3_key,
    )

    payload = json.dumps(
        data,
        ensure_ascii=False,
    )

    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=payload.encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(
        "[S3] Successfully written | key=%s",
        s3_key,
    )

    return s3_key


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
            "US",
        )

        run_id = event.get(
            "run_id",
            f"batch_{uuid.uuid4().hex[:8]}",
        )

    else:

        exchange = "US"
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
                "s3_bucket": S3_BUCKET_NAME,
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