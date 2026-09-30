"""
Bronze Layer Writer.

Every raw API payload lands in S3 under one layout, which the transform job
reads back in src/stock_pipeline/extract.py (`_bronze_bucket_key`):

    stock/bronze/source=<source>/dataset=<dataset>/ingestion_date=YYYY-MM-DD/run_id=<run_id>/<file_name>.json

Each ingestion Lambda folder has its own copy of this file;
tests/test_bronze_layout.py checks every copy against that reader.
"""

import json
import logging
from datetime import datetime

import boto3


logger = logging.getLogger(__name__)


# ============================================================
# AWS CLIENT
# ============================================================

s3_client = boto3.client("s3")


# ============================================================
# BRONZE KEY
# ============================================================

def bronze_key(
    source: str,
    dataset: str,
    execution_time: datetime,
    run_id: str,
    file_name: str = "data",
) -> str:
    """
    Build the S3 object key for one raw Bronze payload.

    Args:
        source: Data source partition (alphavantage, massive, finnhub).
        dataset: Dataset partition (e.g. company_overview).
        execution_time: Ingestion time; its date is the ingestion_date partition.
        run_id: Run partition shared with the transform job.
        file_name: Object name without the .json extension.

    Returns:
        str: S3 object key.
    """

    ingestion_date = execution_time.strftime("%Y-%m-%d")

    return (
        "stock/"
        "bronze/"
        f"source={source}/"
        f"dataset={dataset}/"
        f"ingestion_date={ingestion_date}/"
        f"run_id={run_id}/"
        f"{file_name}.json"
    )


# ============================================================
# BRONZE WRITER
# ============================================================

def write_bronze_json(
    data,
    bucket_name: str,
    source: str,
    dataset: str,
    run_id: str,
    execution_time: datetime,
    file_name: str = "data",
) -> str:
    """
    Serialize a raw API payload as UTF-8 JSON and write it to S3 Bronze.

    Returns:
        str: The S3 object key written.
    """

    s3_key = bronze_key(
        source=source,
        dataset=dataset,
        execution_time=execution_time,
        run_id=run_id,
        file_name=file_name,
    )

    logger.info(
        "[INGESTION][BRONZE][UPLOAD_START] "
        "Uploading raw payload to S3 | "
        "source=%s | dataset=%s | bucket=%s | key=%s",
        source,
        dataset,
        bucket_name,
        s3_key,
    )

    payload_bytes = json.dumps(
        data,
        ensure_ascii=False,
    ).encode("utf-8")

    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=payload_bytes,
        ContentType="application/json",
    )

    logger.info(
        "[INGESTION][BRONZE][UPLOAD_SUCCESS] "
        "Raw payload uploaded successfully | "
        "bucket=%s | key=%s | size_bytes=%d",
        bucket_name,
        s3_key,
        len(payload_bytes),
    )

    return s3_key
