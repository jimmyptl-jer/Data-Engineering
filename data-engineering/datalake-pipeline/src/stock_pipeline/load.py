"""
Load Stage Module — Low-level storage writer for S3 landing operations.

Handles direct SDK interactions with AWS S3 using `boto3`.
Primary responsibility is landing immutable raw JSON API payloads
into the Bronze S3 layer.
"""

import json
import logging

import boto3


logger = logging.getLogger(__name__)


class StockDataLoader:
    """
    Loader class responsible for persisting raw API data directly
    to AWS S3.
    """

    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
    ):
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
        data,
        bucket_name: str,
        bucket_key: str,
    ) -> dict:
        """
        Serialize a raw JSON payload and upload it directly
        to the AWS S3 Bronze layer.
        """

        try:
            # ----------------------------------------------------
            # Initialize S3 client
            # ----------------------------------------------------

            s3 = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
            )

            # ----------------------------------------------------
            # Serialize payload
            # ----------------------------------------------------

            json_data = json.dumps(
                data,
                default=str,
            )

            payload_bytes = len(
                json_data.encode("utf-8")
            )

            logger.info(
                "[LOAD][S3_UPLOAD] Uploading raw payload | "
                "bucket=%s | key=%s | size_bytes=%d",
                bucket_name,
                bucket_key,
                payload_bytes,
            )

            # ----------------------------------------------------
            # Upload to S3
            # ----------------------------------------------------

            response = s3.put_object(
                Bucket=bucket_name,
                Key=bucket_key,
                Body=json_data,
                ContentType="application/json",
            )

            # ----------------------------------------------------
            # Successful upload
            # ----------------------------------------------------

            logger.info(
                "[LOAD][S3_UPLOAD_SUCCESS] Raw payload uploaded successfully | "
                "bucket=%s | key=%s | size_bytes=%d",
                bucket_name,
                bucket_key,
                payload_bytes,
            )

            return response

        except Exception:

            logger.exception(
                "[LOAD][S3_UPLOAD_FAILED] Failed to upload raw payload | "
                "bucket=%s | key=%s",
                bucket_name,
                bucket_key,
            )

            raise