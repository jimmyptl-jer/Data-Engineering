"""
Massive Ingestion Lambda — Entry Point.

SAM Handler: app.lambda_handler

Runs every dataset in datasets.DATASETS (exchanges, tickers, aggregates,
splits, dividends, stock overview) and writes each raw response to the
S3 Bronze layer.

Event (all fields optional):
    {
        "run_id": "batch_20260930_140000",   # Bronze run_id= partition; generated if absent
        "symbol": "IBM",                     # per-symbol datasets; default AAPL
        "from_date": "2026-09-30",           # aggregates date; default today (UTC)
        "to_date": "2026-09-30"              # passed to ingest_aggregates
    }

Response:
    statusCode 200 when no dataset failed, 500 otherwise (summary.status
    PARTIAL_FAILURE). Per-dataset outcomes are in "results".

Files in this Lambda:
    app.py                 lambda_handler: resolves the event, runs every dataset, builds the response
    datasets/__init__.py   DATASETS registry: which endpoints run, in what order, their Bronze dataset names
    datasets/<name>.py     one endpoint each: exchanges, tickers, aggregates, splits, dividends, stock_overview
    client.py              the Massive SDK REST client
    serialization.py       Massive SDK model → dict
    config.py              environment variables and defaults
    bronze.py              Bronze S3 key layout + JSON writer
"""

import logging
import uuid
from datetime import datetime, timezone

import client
import config
from bronze import write_bronze_json
from datasets import DATASETS, MassiveDataset, MassiveRequest


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
# EVENT → REQUEST
# ============================================================

def parse_request(
    event: dict,
    execution_start_time: datetime,
) -> MassiveRequest:
    """
    Resolve the dataset inputs from the Lambda event.
    """

    return MassiveRequest(
        symbol=event.get(
            "symbol",
            config.DEFAULT_SYMBOL,
        ),
        from_date=event.get(
            "from_date",
            execution_start_time.strftime("%Y-%m-%d"),
        ),
        to_date=event.get(
            "to_date"
        ),
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
    Upload one dataset's raw response to S3 Bronze.

    Per-symbol datasets are written as <symbol>.json, the others as
    data.json.
    """

    return write_bronze_json(
        data,
        bucket_name=config.S3_BUCKET_NAME,
        source=config.DATASOURCE,
        dataset=dataset,
        run_id=run_id,
        execution_time=execution_time,
        file_name=symbol if symbol else "data",
    )


# ============================================================
# INGEST ONE DATASET
# ============================================================

def ingest_dataset(
    spec: MassiveDataset,
    request: MassiveRequest,
    run_id: str,
    execution_start_time: datetime,
) -> dict:
    """
    Fetch one dataset, write it to Bronze and report the outcome.

    Returns:
        dict: {"status": "SUCCESS" | "EMPTY" | "FAILED", ...}
    """

    try:

        logger.info(
            "[MASSIVE] Starting %s ingestion",
            spec.label,
        )

        symbol = request.symbol if spec.per_symbol else None

        data = spec.fetch(
            client.massive_client,
            request,
        )

        if data:

            s3_key = upload_to_s3(
                data=data,
                dataset=spec.dataset,
                symbol=symbol,
                run_id=run_id,
                execution_time=execution_start_time,
            )

            result = {"status": "SUCCESS"}

            if spec.per_symbol:
                result["symbol"] = symbol

            result["record_count"] = 1 if spec.single_record else len(data)
            result["s3_key"] = s3_key

            return result

        result = {"status": "EMPTY"}

        if spec.per_symbol:
            result["symbol"] = symbol

        result["record_count"] = 0

        return result

    except Exception as e:

        logger.exception(
            "[MASSIVE] %s ingestion failed",
            spec.label.capitalize(),
        )

        return {
            "status": "FAILED",
            "error": str(e),
        }


# ============================================================
# LAMBDA HANDLER
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

    request = parse_request(
        event,
        execution_start_time,
    )

    results = {
        spec.result_key: ingest_dataset(
            spec,
            request,
            run_id,
            execution_start_time,
        )
        for spec in DATASETS
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
