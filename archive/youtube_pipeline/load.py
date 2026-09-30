"""
Load stage — writes raw and transformed YouTube data to S3.
"""

import json
import logging
from datetime import datetime, timezone

import boto3

logger = logging.getLogger(__name__)


class YouTubeDataLoader:
    """
    Responsible for persisting YouTube data to external storage (S3).
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
    ):
        """
        Store raw YouTube API JSON response in the Bronze S3 layer.

        The object key follows the convention:
            youtube/year=.../month=.../day=.../hour=.../<timestamp>.json
        """

        logger.info(
            "Preparing YouTube API data for S3 Bronze upload."
        )

        now = datetime.now(timezone.utc)

        key = (
            f"youtube/"
            f"year={now.year}/"
            f"month={now.month:02d}/"
            f"day={now.day:02d}/"
            f"hour={now.hour:02d}/"
            f"{now.strftime('%Y%m%dT%H%M%S%fZ')}.json"
        )

        logger.info(
            "Uploading YouTube data to S3. Bucket: %s, Key: %s",
            bucket_name,
            key,
        )

        try:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
            )

            response = s3.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=json.dumps(data),
                ContentType="application/json",
            )

            logger.info(
                "YouTube data uploaded to S3 successfully. "
                "Bucket: %s, Key: %s",
                bucket_name,
                key,
            )

            return response

        except Exception as e:
            logger.exception(
                "Failed to upload YouTube data to S3. "
                "Bucket: %s, Key: %s, Error: %s",
                bucket_name,
                key,
                e,
            )
            raise