"""
Watermark State Manager Module.

Manages pipeline state persistence for incremental ETL processing across the Medallion architecture.

Watermark Storage Architecture:
  s3a://{S3_BUCKET_NAME}/watermark/{pipeline_name}/{dataset_name}.json

  Sample Watermark JSON Schema:
  {
    "pipeline_name": "bronze_to_silver",
    "dataset_name": "daily_time_series",
    "watermark_column": "day_date",
    "watermark_value": "2026-08-06",
    "last_processed_at": "2026-08-07T03:30:00+00:00",
    "batch_id": "batch_20260807_033000",
    "status": "SUCCESS",
    "updated_at": "2026-08-07T03:31:45+00:00",
    "updated_by": "stock_pipeline",
    "remarks": "Bronze to Silver completed successfully."
  }

Hadoop FileSystem Integration:
  Uses PySpark's JVM gateway (`org.apache.hadoop.fs.FileSystem`) for atomic existence checks
  across S3 (`s3a://`), HDFS (`hdfs://`), and local filesystem (`file://`) paths without requiring
  boto3 list operations or throwing PySpark FileNotFound exceptions.
"""

import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession

from . import config

logger = logging.getLogger(__name__)


class WatermarkManager:
    """
    Manages watermark CRUD operations and state checks for ETL pipelines.
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize WatermarkManager with active SparkSession.

        Args:
            spark: Active SparkSession instance.
        """
        self.spark = spark
        self.watermark_base_path = config.WATERMARK_BASE_PATH

    # ============================================================
    # PRIVATE HELPERS
    # ============================================================

    def _path_exists(self, path_str: str) -> bool:
        """
        Check whether a path exists on the underlying storage filesystem (S3/HDFS/local).

        Uses Hadoop FileSystem API via Spark JVM Gateway to avoid raising PySpark read exceptions.

        Args:
            path_str: Fully qualified path URI to check.

        Returns:
            bool: True if path exists, False otherwise.
        """
        gateway = self.spark.sparkContext._jvm
        hadoop_path = gateway.org.apache.hadoop.fs.Path(path_str)
        conf = self.spark.sparkContext._jsc.hadoopConfiguration()
        file_system = gateway.org.apache.hadoop.fs.FileSystem.get(
            hadoop_path.toUri(), conf
        )
        return file_system.exists(hadoop_path)

    def _build_watermark_path(
        self,
        pipeline_name: str,
        dataset_name: str,
    ) -> str:
        """
        Build fully qualified S3 watermark JSON file URI.

        Args:
            pipeline_name: Pipeline identifier (e.g. 'bronze_to_silver').
            dataset_name: Dataset identifier (e.g. 'daily_time_series').

        Returns:
            str: Full watermark path URI (e.g. 's3a://graywolf--data--lake/watermark/bronze_to_silver/daily_time_series.json').
        """
        watermark_path = (
            f"{self.watermark_base_path}"
            f"{pipeline_name}/"
            f"{dataset_name}.json"
        )

        logger.debug(
            "[WATERMARK][BUILD_PATH] Constructed watermark URI: %s",
            watermark_path,
        )

        return watermark_path

    # ============================================================
    # WATERMARK EXISTENCE CHECK
    # ============================================================

    def watermark_exists(
        self,
        pipeline_name: str,
        dataset_name: str,
    ) -> bool:
        """
        Check if a watermark state JSON file exists for the given pipeline and dataset.

        Args:
            pipeline_name: Pipeline identifier.
            dataset_name: Dataset identifier.

        Returns:
            bool: True if watermark JSON exists, False otherwise.
        """
        try:
            watermark_path = self._build_watermark_path(
                pipeline_name=pipeline_name,
                dataset_name=dataset_name,
            )

            exists = self._path_exists(watermark_path)

            logger.info(
                "[WATERMARK][EXISTS_CHECK] pipeline=%s, dataset=%s, watermark_exists=%s",
                pipeline_name,
                dataset_name,
                exists,
            )

            return exists

        except Exception as e:
            logger.exception(
                "[WATERMARK][EXISTS_FAIL] Error checking watermark existence for dataset=%s: %s",
                dataset_name,
                e,
            )
            raise

    # ============================================================
    # WATERMARK READ
    # ============================================================

    def read_watermark(
        self,
        pipeline_name: str,
        dataset_name: str,
    ) -> dict:
        """
        Read and return the latest watermark state dictionary for a given pipeline and dataset.

        Args:
            pipeline_name: Pipeline identifier.
            dataset_name: Dataset identifier.

        Returns:
            dict: Parsed watermark dictionary payload.

        Raises:
            Exception: If reading or parsing JSON fails.
        """
        logger.info(
            "[WATERMARK][READ] Reading watermark state for pipeline=%s, dataset=%s.",
            pipeline_name,
            dataset_name,
        )

        try:
            watermark_path = self._build_watermark_path(
                pipeline_name=pipeline_name,
                dataset_name=dataset_name,
            )

            watermark_df = (
                self.spark.read
                .option("multiLine", "true")
                .json(watermark_path)
            )

            watermark = watermark_df.first().asDict()

            logger.info(
                "[WATERMARK][READ_OK] Watermark read successfully for dataset=%s.",
                dataset_name,
            )

            logger.debug(
                "[WATERMARK][READ_OK] Watermark payload contents: %s",
                watermark,
            )

            return watermark

        except Exception as e:
            logger.exception(
                "[WATERMARK][READ_FAIL] Failed to read watermark state for "
                "pipeline=%s, dataset=%s: %s",
                pipeline_name,
                dataset_name,
                e,
            )
            raise

    # ============================================================
    # WATERMARK WRITE
    # ============================================================

    def write_watermark(self, watermark: dict) -> None:
        """
        Create or overwrite the watermark state JSON file.

        The `watermark` payload must contain `pipeline_name` and `dataset_name` keys.

        Args:
            watermark: Complete watermark metadata dictionary payload to write.

        Raises:
            Exception: If Spark write operation fails.
        """
        try:
            watermark_path = self._build_watermark_path(
                pipeline_name=watermark["pipeline_name"],
                dataset_name=watermark["dataset_name"],
            )

            logger.info(
                "[WATERMARK][WRITE] Persisting watermark payload to URI: %s",
                watermark_path,
            )

            logger.debug(
                "[WATERMARK][WRITE] Payload: %s",
                watermark,
            )

            watermark_df = self.spark.createDataFrame([watermark])

            (
                watermark_df.write
                .mode("overwrite")
                .json(watermark_path)
            )

            logger.info(
                "[WATERMARK][WRITE_OK] Watermark persisted successfully for dataset=%s.",
                watermark["dataset_name"],
            )

        except Exception as e:
            logger.exception(
                "[WATERMARK][WRITE_FAIL] Failed to persist watermark: %s",
                e,
            )
            raise

    # ============================================================
    # HASH-BASED CHANGE DETECTION
    # ============================================================
    # TODO: Implement compute_content_hash() and hash_changed() methods
    # for hash-based incremental processing of reference/master data
    # (e.g., Company Overview).
    #
    # compute_content_hash(df, exclude_columns) -> str
    #   - Hash only business columns (exclude processed_at, batch_id, etc.)
    #   - Use SHA-256 for deterministic comparison
    #
    # hash_changed(pipeline_name, dataset_name, new_hash) -> bool
    #   - Read existing watermark -> get stored overview_hash
    #   - Compare new hash vs old hash
    #   - Return True if changed (or no prior watermark), False if unchanged