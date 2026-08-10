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

        Args:
            data:
                Raw API payload.

            bucket_name:
                Name of the target S3 bucket.

            bucket_key:
                Complete S3 object key.

            stock_symbol:
                Optional stock ticker symbol. This is only required
                for symbol-specific datasets.

        Returns:
            dict:
                AWS boto3 S3 response payload containing HTTP status
                and ETag metadata.

        Raises:
            Exception:
                If JSON serialization fails or the S3 put_object
                operation fails.
        """

        logger.info(
            "[LOAD][S3_KEY] Target S3 path — Bucket: %s, Key: %s",
            bucket_name,
            bucket_key,
        )

        try:
            # ----------------------------------------------------
            # 2. Initialize boto3 S3 client
            # ----------------------------------------------------

            logger.info(
                "[LOAD][S3_CLIENT] Instantiating boto3 S3 client."
            )

            s3 = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
            )

            logger.info(
                "[LOAD][S3_CLIENT_OK] boto3 S3 client initialized."
            )

            # ----------------------------------------------------
            # 3. Serialize payload
            # ----------------------------------------------------

            logger.info(
                "[LOAD][SERIALIZE] Serializing raw payload to JSON."
            )

            json_data = json.dumps(
                data,
                default=str,
            )

            payload_bytes = len(
                json_data.encode("utf-8")
            )

            logger.info(
                "[LOAD][SERIALIZE_OK] JSON serialization complete. "
                "Size: %d bytes.",
                payload_bytes,
            )

            # ----------------------------------------------------
            # 4. Upload to S3
            # ----------------------------------------------------

            logger.info(
                "[LOAD][S3_PUT] Uploading object to S3 — "
                "Bucket: %s, Key: %s",
                bucket_name,
                bucket_key,
            )

            response = s3.put_object(
                Bucket=bucket_name,
                Key=bucket_key,
                Body=json_data,
                ContentType="application/json",
            )

            # ----------------------------------------------------
            # 5. Successful upload
            # ----------------------------------------------------

            logger.info(
                "[LOAD][S3_PUT_OK] Upload successful — "
                "Bucket: %s, Key: %s",
                bucket_name,
                bucket_key,
            )

            logger.debug(
                "[LOAD][S3_RESPONSE] Response metadata: %s",
                response.get("ResponseMetadata"),
            )

            return response

        except Exception as e:
            logger.exception(
                "[LOAD_FAIL][S3_UPLOAD_ERROR] "
                "Failed to upload payload to S3 — "
                "Bucket: %s, Key: %s, Error: %s",
                bucket_name,
                bucket_key,
                e,
            )

            raise
