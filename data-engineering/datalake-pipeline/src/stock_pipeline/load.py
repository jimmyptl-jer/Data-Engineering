"""
Load Stage Module — Low-level storage writer for S3 landing operations.

Handles direct SDK interactions with AWS S3 using `boto3`.
Primary responsibility is landing immutable raw JSON API payloads into the Bronze S3 layer.
"""

import json
import logging
from datetime import datetime, timezone

import boto3

logger = logging.getLogger(__name__)


class StockDataLoader:
    """
    Loader class responsible for persisting raw API data directly to AWS S3.
    """

    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str):
        """
        Initialize the S3 Loader with AWS credentials.

        Args:
            aws_access_key_id: AWS Access Key ID.
            aws_secret_access_key: AWS Secret Access Key.
        """
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    # ============================================================
    # S3 BRONZE RAW JSON UPLOADER
    # ============================================================

    def upload_raw_to_s3(
        self,
        data: dict,
        bucket_name: str,
        stock_symbol: str,
        bucket_key: str,
    ) -> dict:
        """
        Serialize raw JSON dictionary payload and upload directly to AWS S3 Bronze layer.

        Args:
            data: Raw dictionary payload returned by the Alpha Vantage REST API.
            bucket_name: Name of the target S3 bucket (e.g. 'graywolf--data--lake').
            stock_symbol: Stock ticker symbol (e.g. 'IBM').
            bucket_key: Complete partition key path within the bucket.

        Returns:
            dict: AWS boto3 S3 response payload containing HTTP status and ETag metadata.

        Raises:
            Exception: If JSON serialization fails or S3 `put_object` API call fails.
        """
        logger.info(
            "[LOAD][PREPARE] Preparing raw JSON payload for S3 upload. Symbol: %s",
            stock_symbol,
        )

        logger.info(
            "[LOAD][S3_KEY] Target S3 partition path — Symbol: %s, Bucket: %s, Key: %s",
            stock_symbol,
            bucket_name,
            bucket_key,
        )

        try:
            # 1. Initialize boto3 S3 Client
            logger.info("[LOAD][S3_CLIENT] Instantiating boto3 S3 client.")
            s3 = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
            )
            logger.info("[LOAD][S3_CLIENT_OK] boto3 S3 client initialized.")

            # 2. Serialize Dictionary to JSON String
            logger.info("[LOAD][SERIALIZE] Serializing dictionary to JSON string. Symbol: %s", stock_symbol)
            json_data = json.dumps(data)

            payload_bytes = len(json_data.encode("utf-8"))
            logger.info(
                "[LOAD][SERIALIZE_OK] JSON serialization complete. Symbol: %s, Size: %d bytes.",
                stock_symbol,
                payload_bytes,
            )

            # 3. Perform S3 put_object API call
            logger.info(
                "[LOAD][S3_PUT] Uploading object to S3 — Bucket: %s, Key: %s",
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
                "[LOAD][S3_PUT_OK] Upload successful. Symbol: %s, S3 Key: %s",
                stock_symbol,
                bucket_key,
            )

            logger.debug(
                "[LOAD][S3_RESPONSE] Response metadata for symbol %s: %s",
                stock_symbol,
                response.get("ResponseMetadata"),
            )

            return response

        except Exception as e:
            logger.exception(
                "[LOAD_FAIL][S3_UPLOAD_ERROR] Failed to upload payload to S3. Symbol: %s, Bucket: %s, Key: %s, Error: %s",
                stock_symbol,
                bucket_name,
                bucket_key,
                e,
            )
            raise
