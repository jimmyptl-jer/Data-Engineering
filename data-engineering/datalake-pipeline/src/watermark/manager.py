"""
Watermark Manager — Manages watermark state for incremental ETL pipelines.

Supports two incremental strategies:
  - Date-based watermark   → for transactional / time-series data.
  - Hash-based detection   → for reference / master data.
"""

import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession

from . import config

logger = logging.getLogger(__name__)


class WatermarkManager:
    """
    Manages watermark operations for all ETL pipelines.

    Responsibilities:
        - Check whether a watermark exists.
        - Create an initial watermark.
        - Read the latest watermark.
        - Write/update the watermark after a successful pipeline run.
        - Compute content hashes for hash-based change detection.
        - Compare hashes to decide whether processing is needed.
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize the Watermark Manager.

        Args:
            spark: Active SparkSession.
        """
        self.spark = spark
        self.watermark_base_path = config.WATERMARK_BASE_PATH

    # ============================================================
    # PRIVATE HELPERS
    # ============================================================

    def _path_exists(self, path_str: str) -> bool:
        """
        Check whether a path exists on the configured filesystem (S3/HDFS/local).

        Uses the Hadoop FileSystem API via the Spark JVM gateway, so it works
        transparently with s3a://, hdfs://, and file:// paths.

        Args:
            path_str: Fully qualified path to check.

        Returns:
            True if the path exists, otherwise False.
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
        Build the full path for the watermark file.

        Args:
            pipeline_name: Name of the pipeline (e.g. bronze_to_silver).
            dataset_name: Name of the dataset (e.g. daily_time_series).

        Returns:
            Full watermark path (e.g. s3a://bucket/watermark/bronze_to_silver/daily_time_series.json).
        """
        watermark_path = (
            f"{self.watermark_base_path}"
            f"{pipeline_name}/"
            f"{dataset_name}.json"
        )

        logger.debug(
            "[WATERMARK][BUILD_PATH] Watermark path: %s",
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
        Check whether a watermark file exists.

        Args:
            pipeline_name: Pipeline identifier.
            dataset_name: Dataset identifier.

        Returns:
            True if the watermark exists, otherwise False.
        """
        try:
            watermark_path = self._build_watermark_path(
                pipeline_name=pipeline_name,
                dataset_name=dataset_name,
            )

            exists = self._path_exists(watermark_path)

            logger.info(
                "[WATERMARK][EXISTS] pipeline=%s, dataset=%s, exists=%s",
                pipeline_name,
                dataset_name,
                exists,
            )

            return exists

        except Exception as e:
            logger.exception(
                "[WATERMARK][EXISTS_FAIL] Failed to check watermark existence: %s",
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
        Read the current watermark for a given pipeline and dataset.

        Args:
            pipeline_name: Pipeline identifier.
            dataset_name: Dataset identifier.

        Returns:
            Watermark information as a dictionary.
        """
        logger.info(
            "[WATERMARK][READ] Reading watermark for pipeline=%s, dataset=%s.",
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
                "[WATERMARK][READ_OK] Watermark contents: %s",
                watermark,
            )

            return watermark

        except Exception as e:
            logger.exception(
                "[WATERMARK][READ_FAIL] Failed to read watermark for "
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
        Create or update the watermark.

        The watermark dict must contain ``pipeline_name`` and
        ``dataset_name`` keys so the correct path can be derived.

        Args:
            watermark: Complete watermark payload to persist.
        """
        try:
            watermark_path = self._build_watermark_path(
                pipeline_name=watermark["pipeline_name"],
                dataset_name=watermark["dataset_name"],
            )

            logger.info(
                "[WATERMARK][WRITE] Writing watermark to %s",
                watermark_path,
            )

            logger.debug(
                "[WATERMARK][WRITE] Watermark payload: %s",
                watermark,
            )

            watermark_df = self.spark.createDataFrame([watermark])

            (
                watermark_df.write
                .mode("overwrite")
                .json(watermark_path)
            )

            logger.info(
                "[WATERMARK][WRITE_OK] Watermark written successfully for "
                "dataset=%s.",
                watermark["dataset_name"],
            )

        except Exception as e:
            logger.exception(
                "[WATERMARK][WRITE_FAIL] Failed to write watermark: %s",
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