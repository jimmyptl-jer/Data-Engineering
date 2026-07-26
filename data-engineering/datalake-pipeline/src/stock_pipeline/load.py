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
    ):
        """
        Upload raw Alpha Vantage API response to the S3 Bronze layer.

        The object key follows the convention:
            stock/source=alphavantage/symbol=<SYM>/year=.../month=.../day=.../hour=.../<timestamp>.json
        """

        logger.info(
            "Preparing stock data for S3 upload. Symbol: %s",
            stock_symbol,
        )

        now = datetime.now(timezone.utc)

        key = (
            f"stock/"
            f"source=alphavantage/"
            f"symbol={stock_symbol.upper()}/"
            f"year={now.year}/"
            f"month={now.month:02d}/"
            f"day={now.day:02d}/"
            f"hour={now.hour:02d}/"
            f"{now.strftime('%Y%m%dT%H%M%S%fZ')}.json"
        )

        logger.info(
            "Generated S3 object key. "
            "Symbol: %s, Bucket: %s, Key: %s",
            stock_symbol,
            bucket_name,
            key,
        )

        try:
            logger.info(
                "Creating S3 client for stock data upload."
            )

            s3 = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
            )

            logger.info("S3 client created successfully.")

            logger.info(
                "Serializing stock data to JSON. Symbol: %s",
                stock_symbol,
            )

            json_data = json.dumps(data)

            logger.info(
                "Stock data serialized successfully. "
                "Symbol: %s, Payload size: %s bytes",
                stock_symbol,
                len(json_data.encode("utf-8")),
            )

            logger.info(
                "Uploading stock data to S3. "
                "Symbol: %s, Bucket: %s, Key: %s",
                stock_symbol,
                bucket_name,
                key,
            )

            response = s3.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=json_data,
                ContentType="application/json",
            )

            logger.info(
                "Stock data uploaded to S3 successfully. "
                "Symbol: %s, Bucket: %s, Key: %s",
                stock_symbol,
                bucket_name,
                key,
            )

            logger.debug(
                "S3 response metadata for symbol %s: %s",
                stock_symbol,
                response.get("ResponseMetadata"),
            )

            return response

        except Exception as e:
            logger.exception(
                "Failed to upload stock data to S3. "
                "Symbol: %s, Bucket: %s, Key: %s, Error: %s",
                stock_symbol,
                bucket_name,
                key,
                e,
            )
            raise
