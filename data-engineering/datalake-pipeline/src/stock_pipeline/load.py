"""
Load stage — writes raw and transformed stock data to S3.
"""

import json
import logging
from datetime import datetime, timezone


import boto3

logger = logging.getLogger(__name__)


class StockDataLoader:
    """
    Responsible for persisting stock data to external storage (S3).
    """

    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    # ============================================================
    # S3 BRONZE INGESTION
    # ============================================================

    def upload_raw_to_s3(
        self,
        data,
        bucket_name: str,
        stock_symbol: str,
        bucket_key:str
    ):
        """
        Upload raw Alpha Vantage API response to the S3 Bronze layer.

        The object key follows the convention:
            stock/source=alphavantage/symbol=<SYM>/year=.../month=.../day=.../hour=.../<timestamp>.json
        """

        logger.info(
            "[STEP INGEST_1][S3_UPLOAD_PREP] Preparing stock data for S3 upload. Symbol: %s",
            stock_symbol,
        )
       
        logger.info(
            "[STEP INGEST_1][S3_UPLOAD_KEY] Generated S3 object key. "
            "Symbol: %s, Bucket: %s, Key: %s",
            stock_symbol,
            bucket_name,
            bucket_key,
        )

        try:
            logger.info(
                "[STEP INGEST_1.1][S3_CLIENT] Creating S3 client for stock data upload."
            )

            s3 = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
            )

        
            logger.info("[STEP INGEST_1.2][S3_CLIENT_OK] S3 client created successfully.")

            logger.info(
                "[STEP INGEST_1.3][SERIALIZE] Serializing stock data to JSON. Symbol: %s",
                stock_symbol,
            )

            json_data = json.dumps(data)

            logger.info(
                "[STEP INGEST_1.4][SERIALIZE_OK] Stock data serialized successfully. "
                "Symbol: %s, Payload size: %s bytes",
                stock_symbol,
                len(json_data.encode("utf-8")),
            )

            logger.info(
                "[STEP INGEST_1.5][S3_PUT] Uploading stock data to S3. "
                "Symbol: %s, Bucket: %s, Key: %s",
                stock_symbol,
                bucket_name,
                bucket_key,
            )

            response = s3.put_object(
                Bucket=bucket_name,
                Key=bucket_key,
                Body=json_data,
                ContentType="application/json",
            )

            logger.info(
                "[STEP INGEST_1.6][S3_PUT_OK] Stock data uploaded to S3 successfully. "
                "Symbol: %s, Bucket: %s, Key: %s",
                stock_symbol,
                bucket_name,
                bucket_key,
            )

            logger.debug(
                "[STEP INGEST_1.7][S3_RESPONSE] S3 response metadata for symbol %s: %s",
                stock_symbol,
                response.get("ResponseMetadata"),
            )

            return response

        except Exception as e:
            logger.exception(
                "[STEP INGEST_1.E][S3_UPLOAD_FAIL] Failed to upload stock data to S3. "
                "Symbol: %s, Bucket: %s, Key: %s, Error: %s",
                stock_symbol,
                bucket_name,
                bucket_key,
                e,
            )
            raise
