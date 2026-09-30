"""
Massive Ingestion Lambda.

Single Lambda entry point that executes all Massive ingestion
endpoints and stores the raw responses in the S3 Bronze layer.

Endpoints:
    1. Exchanges
    2. Tickers
    3. Aggregates
    4. Splits
    5. Dividends
    6. Stock Overview
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone

import boto3
from massive import RESTClient

from exchanges import ingest_exchanges
from tickers import ingest_tickers
from aggregates import ingest_aggregates
from splits import ingest_splits
from dividends import ingest_dividends
from stock_overview import ingest_stock_overview


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

MASSIVE_API_KEY = os.environ["MASSIVE_API_KEY"]

S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]


# ============================================================
# CLIENTS
# ============================================================

s3_client = boto3.client("s3")

massive_client = RESTClient(
    MASSIVE_API_KEY
)


# ============================================================
# S3 UPLOAD
# ============================================================

def upload_to_s3(
    data,
    dataset: str,
    run_id: str,
    execution_time: datetime,
    symbol: str | None = None,
) -> str:
    """
    Upload raw data to S3 Bronze.
    """

    ingestion_date = execution_time.strftime(
        "%Y-%m-%d"
    )

    if symbol:

        s3_key = (
            "stock/"
            "bronze/"
            "source=massive/"
            f"dataset={dataset}/"
            f"symbol={symbol}/"
            f"ingestion_date={ingestion_date}/"
            f"run_id={run_id}/"
            "data.json"
        )

    else:

        s3_key = (
            "stock/"
            "bronze/"
            "source=massive/"
            f"dataset={dataset}/"
            f"ingestion_date={ingestion_date}/"
            f"run_id={run_id}/"
            "data.json"
        )

    logger.info(
        "[S3] Uploading | dataset=%s | key=%s",
        dataset,
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
        "[S3] Upload completed | dataset=%s",
        dataset,
    )

    return s3_key


# ============================================================
# RUN ID
# ============================================================

def get_run_id(event) -> str:
    """
    Get run ID from the event or generate one.
    """

    if isinstance(event, dict):

        return event.get(
            "run_id",
            f"batch_{uuid.uuid4().hex[:8]}",
        )

    return f"batch_{uuid.uuid4().hex[:8]}"


# ============================================================
# MASSIVE INGESTION
# ============================================================

def lambda_handler(event, context):
    """
    Execute all Massive ingestion endpoints.
    """

    execution_start_time = datetime.now(
        timezone.utc
    )

    run_id = get_run_id(event)

    event = event if isinstance(event, dict) else {}

    logger.info(
        "=================================================="
    )

    logger.info(
        "[LAMBDA][MASSIVE] "
        "Starting ingestion | run_id=%s",
        run_id,
    )

    logger.info(
        "=================================================="
    )

    results = {}

    # ========================================================
    # 1. EXCHANGES
    # ========================================================

    try:

        logger.info(
            "[MASSIVE] Starting exchanges ingestion"
        )

        data = ingest_exchanges(
            client=massive_client
        )

        if data:

            s3_key = upload_to_s3(
                data=data,
                dataset="exchanges",
                run_id=run_id,
                execution_time=execution_start_time,
            )

            results["exchanges"] = {
                "status": "SUCCESS",
                "record_count": len(data),
                "s3_key": s3_key,
            }

        else:

            results["exchanges"] = {
                "status": "EMPTY",
                "record_count": 0,
            }

    except Exception as e:

        logger.exception(
            "[MASSIVE] Exchanges ingestion failed"
        )

        results["exchanges"] = {
            "status": "FAILED",
            "error": str(e),
        }

    # ========================================================
    # 2. TICKERS
    # ========================================================

    try:

        logger.info(
            "[MASSIVE] Starting tickers ingestion"
        )

        data = ingest_tickers(
            client=massive_client
        )

        if data:

            s3_key = upload_to_s3(
                data=data,
                dataset="ticker_reference",
                run_id=run_id,
                execution_time=execution_start_time,
            )

            results["tickers"] = {
                "status": "SUCCESS",
                "record_count": len(data),
                "s3_key": s3_key,
            }

        else:

            results["tickers"] = {
                "status": "EMPTY",
                "record_count": 0,
            }

    except Exception as e:

        logger.exception(
            "[MASSIVE] Tickers ingestion failed"
        )

        results["tickers"] = {
            "status": "FAILED",
            "error": str(e),
        }

    # ========================================================
    # 3. AGGREGATES
    # ========================================================

    try:

        logger.info(
            "[MASSIVE] Starting aggregates ingestion"
        )

        from_date = event.get(
            "from_date",
            execution_start_time.strftime("%Y-%m-%d"),
        )

        to_date = event.get(
            "to_date"
        )

        data = ingest_aggregates(
            client=massive_client,
            from_date=from_date,
            to_date=to_date,
        )

        if data:

            s3_key = upload_to_s3(
                data=data,
                dataset="aggregates",
                run_id=run_id,
                execution_time=execution_start_time,
            )

            results["aggregates"] = {
                "status": "SUCCESS",
                "record_count": len(data),
                "s3_key": s3_key,
            }

        else:

            results["aggregates"] = {
                "status": "EMPTY",
                "record_count": 0,
            }

    except Exception as e:

        logger.exception(
            "[MASSIVE] Aggregates ingestion failed"
        )

        results["aggregates"] = {
            "status": "FAILED",
            "error": str(e),
        }

    # ========================================================
    # 4. SPLITS
    # ========================================================

    try:

        logger.info(
            "[MASSIVE] Starting splits ingestion"
        )

        symbol = event.get(
            "symbol",
            "AAPL",
        )

        data = ingest_splits(
            client=massive_client,
            symbol=symbol,
        )

        if data:

            s3_key = upload_to_s3(
                data=data,
                dataset="splits",
                symbol=symbol,
                run_id=run_id,
                execution_time=execution_start_time,
            )

            results["splits"] = {
                "status": "SUCCESS",
                "symbol": symbol,
                "record_count": len(data),
                "s3_key": s3_key,
            }

        else:

            results["splits"] = {
                "status": "EMPTY",
                "symbol": symbol,
                "record_count": 0,
            }

    except Exception as e:

        logger.exception(
            "[MASSIVE] Splits ingestion failed"
        )

        results["splits"] = {
            "status": "FAILED",
            "error": str(e),
        }

    # ========================================================
    # 5. DIVIDENDS
    # ========================================================

    try:

        logger.info(
            "[MASSIVE] Starting dividends ingestion"
        )

        symbol = event.get(
            "symbol",
            "AAPL",
        )

        data = ingest_dividends(
            client=massive_client,
            symbol=symbol,
        )

        if data:

            s3_key = upload_to_s3(
                data=data,
                dataset="dividends",
                symbol=symbol,
                run_id=run_id,
                execution_time=execution_start_time,
            )

            results["dividends"] = {
                "status": "SUCCESS",
                "symbol": symbol,
                "record_count": len(data),
                "s3_key": s3_key,
            }

        else:

            results["dividends"] = {
                "status": "EMPTY",
                "symbol": symbol,
                "record_count": 0,
            }

    except Exception as e:

        logger.exception(
            "[MASSIVE] Dividends ingestion failed"
        )

        results["dividends"] = {
            "status": "FAILED",
            "error": str(e),
        }

    # ========================================================
    # 6. STOCK OVERVIEW
    # ========================================================

    try:

        logger.info(
            "[MASSIVE] Starting stock overview ingestion"
        )

        symbol = event.get(
            "symbol",
            "AAPL",
        )

        data = ingest_stock_overview(
            client=massive_client,
            symbol=symbol,
        )

        if data:

            s3_key = upload_to_s3(
                data=data,
                dataset="stock_overview",
                symbol=symbol,
                run_id=run_id,
                execution_time=execution_start_time,
            )

            results["stock_overview"] = {
                "status": "SUCCESS",
                "symbol": symbol,
                "record_count": 1,
                "s3_key": s3_key,
            }

        else:

            results["stock_overview"] = {
                "status": "EMPTY",
                "symbol": symbol,
                "record_count": 0,
            }

    except Exception as e:

        logger.exception(
            "[MASSIVE] Stock overview ingestion failed"
        )

        results["stock_overview"] = {
            "status": "FAILED",
            "error": str(e),
        }

    # ========================================================
    # FINAL SUMMARY
    # ========================================================

    successful = sum(
        1
        for result in results.values()
        if result["status"] == "SUCCESS"
    )

    failed = sum(
        1
        for result in results.values()
        if result["status"] == "FAILED"
    )

    empty = sum(
        1
        for result in results.values()
        if result["status"] == "EMPTY"
    )

    logger.info(
        "=================================================="
    )

    logger.info(
        "[LAMBDA][MASSIVE] "
        "Completed | run_id=%s | "
        "success=%s | failed=%s | empty=%s",
        run_id,
        successful,
        failed,
        empty,
    )

    logger.info(
        "=================================================="
    )

    return {
        "statusCode": 200 if failed == 0 else 500,
        "run_id": run_id,
        "summary": {
            "source": "massive",
            "status": (
                "SUCCESS"
                if failed == 0
                else "PARTIAL_FAILURE"
            ),
            "total_endpoints": len(results),
            "successful": successful,
            "failed": failed,
            "empty": empty,
        },
        "results": results,
    }