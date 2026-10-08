# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Watermark Manager — Shared Module
# MAGIC
# MAGIC This notebook is imported via `%run` by every pipeline step notebook. It provides the `WatermarkManager` class for watermark-based incremental ETL state tracking.
# MAGIC
# MAGIC ## Usage in pipeline notebooks
# MAGIC
# MAGIC Add this as the **first code cell** in any pipeline notebook:
# MAGIC
# MAGIC ```python
# MAGIC %run /Workspace/Users/jimmyptl46@gmail.com/delta-lake/stock_market_etl/stock_etl/watermark_manager
# MAGIC ```
# MAGIC
# MAGIC After the `%run` cell, `WatermarkManager` and all config constants are available in the calling notebook's namespace.

# COMMAND ----------

# DBTITLE 1,Config Constants
# ============================================================
# CONFIG — shared constants for all pipeline steps
# ============================================================

S3_BUCKET_NAME = "graywolf--data--lake"
WATERMARK_BASE_PATH = f"s3://{S3_BUCKET_NAME}/watermark/"

# Pipeline identifiers
STOCK_ALPHA_VANTAGE_DAILY_PIPELINE = "alpha_vantage_daily_stock_pipeline"
STOCK_ALPHA_VANTAGE_DAILY_DATASET = "daily_time_series"

STOCK_ALPHA_VANTAGE_COMPANY_OVERVIEW_PIPELINE = "alpha_vantage_company_overview_pipeline"
STOCK_ALPHA_VANTAGE_COMPANY_OVERVIEW_DATASET = "company_overview"

# Twelve Data pipeline identifiers
TWELVE_DATA_1MIN_PIPELINE = "twelvedata_1min_stock_pipeline"
TWELVE_DATA_1MIN_DATASET = "time_series_1min"

# Watermark JSON schema columns (for spark.read.json inference)
WATERMARK_SCHEMA_FIELDS = [
    "pipeline_name", "dataset_name", "watermark_column",
    "watermark_value", "last_processed_at", "batch_id",
    "status", "updated_at", "updated_by", "remarks",
    "overview_hash"  
]

print(f"[CONFIG] WATERMARK_BASE_PATH = {WATERMARK_BASE_PATH}")

# COMMAND ----------

# DBTITLE 1,WatermarkManager Class
# ============================================================
# WatermarkManager — CRUD + state checks for ETL pipelines
# ============================================================
#
# Spark Connect compatible: uses dbutils.fs.ls() instead of
# spark.sparkContext._jvm (which is unavailable on serverless).
#
# Watermark Storage Architecture:
#   s3://{S3_BUCKET_NAME}/watermark/{pipeline_name}/{dataset_name}.json
#
# Sample Watermark JSON:
#   {
#     "pipeline_name": "alpha_vantage_daily",
#     "dataset_name": "daily_time_series",
#     "watermark_column": "day_date",
#     "watermark_value": "2026-08-06",
#     "last_processed_at": "2026-08-07T03:30:00+00:00",
#     "batch_id": "batch_20260807_033000",
#     "status": "SUCCESS",
#     "updated_at": "2026-08-07T03:31:45+00:00",
#     "updated_by": "stock_pipeline",
#     "remarks": "Bronze to Silver completed successfully."
#   }
# ============================================================

import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class WatermarkManager:
    """
    Manages watermark CRUD operations and state checks for ETL pipelines.

    Usage:
        wm = WatermarkManager(spark)

        if wm.watermark_exists("alpha_vantage_daily", "daily_time_series"):
            state = wm.read_watermark("alpha_vantage_daily", "daily_time_series")
            last_date = state["watermark_value"]
        else:
            last_date = "1900-01-01"  # initial load

        # ... process data ...

        wm.write_watermark({
            "pipeline_name": "alpha_vantage_daily",
            "dataset_name": "daily_time_series",
            "watermark_column": "day_date",
            "watermark_value": "2026-09-30",
            "last_processed_at": datetime.now(timezone.utc).isoformat(),
            "batch_id": "batch_20260930_120000",
            "status": "SUCCESS",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "updated_by": "stock_pipeline",
            "remarks": "Incremental load completed."
        })
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.watermark_base_path = WATERMARK_BASE_PATH

    # ========================================================
    # PRIVATE HELPERS
    # ========================================================

    def _path_exists(self, path_str: str) -> bool:
        """
        Check if a path exists on S3/local filesystem.

        Spark Connect compatible — uses dbutils.fs.ls() instead of
        spark.sparkContext._jvm (unavailable on serverless compute).
        """
        try:
            dbutils.fs.ls(path_str)
            return True
        except Exception:
            return False

    def _build_watermark_path(self, pipeline_name: str, dataset_name: str) -> str:
        """Build the full S3 watermark JSON file URI."""
        watermark_path = (
            f"{self.watermark_base_path}"
            f"{pipeline_name}/"
            f"{dataset_name}.json"
        )
        logger.debug("[WATERMARK][BUILD_PATH] %s", watermark_path)
        return watermark_path

    # ========================================================
    # WATERMARK EXISTENCE CHECK
    # ========================================================

    def watermark_exists(self, pipeline_name: str, dataset_name: str) -> bool:
        """Return True if a watermark JSON file exists for this pipeline/dataset."""
        try:
            watermark_path = self._build_watermark_path(pipeline_name, dataset_name)
            exists = self._path_exists(watermark_path)
            logger.info(
                "[WATERMARK][EXISTS] pipeline=%s, dataset=%s, exists=%s",
                pipeline_name, dataset_name, exists,
            )
            return exists
        except Exception as e:
            logger.exception("[WATERMARK][EXISTS_FAIL] %s", e)
            raise

    # ========================================================
    # WATERMARK READ
    # ========================================================

    def read_watermark(self, pipeline_name: str, dataset_name: str) -> dict:
        """
        Read and return the latest watermark state as a dict.

        Returns None if the watermark JSON is empty or not found
        (instead of raising) for graceful first-run handling.
        """
        logger.info(
            "[WATERMARK][READ] pipeline=%s, dataset=%s",
            pipeline_name, dataset_name,
        )
        try:
            watermark_path = self._build_watermark_path(pipeline_name, dataset_name)

            if not self._path_exists(watermark_path):
                logger.info("[WATERMARK][READ] No watermark found — first run.")
                return None

            watermark_df = (
                self.spark.read
                .option("multiLine", "true")
                .json(watermark_path)
            )

            row = watermark_df.first()
            if row is None:
                return None

            watermark = row.asDict()
            logger.info("[WATERMARK][READ_OK] dataset=%s", dataset_name)
            logger.debug("[WATERMARK][READ_OK] payload: %s", watermark)
            return watermark

        except Exception as e:
            logger.exception("[WATERMARK][READ_FAIL] %s", e)
            raise

    # ========================================================
    # WATERMARK WRITE
    # ========================================================

    def write_watermark(self, watermark: dict) -> None:
        """
        Create or overwrite the watermark state JSON file.

        The `watermark` dict must contain 'pipeline_name' and 'dataset_name'.
        """
        try:
            watermark_path = self._build_watermark_path(
                watermark["pipeline_name"],
                watermark["dataset_name"],
            )

            logger.info("[WATERMARK][WRITE] %s", watermark_path)
            logger.debug("[WATERMARK][WRITE] payload: %s", watermark)

            watermark_df = self.spark.createDataFrame([watermark])

            (
                watermark_df.write
                .mode("overwrite")
                .json(watermark_path)
            )

            logger.info(
                "[WATERMARK][WRITE_OK] dataset=%s",
                watermark["dataset_name"],
            )
        except Exception as e:
            logger.exception("[WATERMARK][WRITE_FAIL] %s", e)
            raise

    # ========================================================
    # CONVENIENCE: get watermark value or default
    # ========================================================

    def get_watermark_value(
        self,
        pipeline_name: str,
        dataset_name: str,
        default: str = "1900-01-01",
    ) -> str:
        """
        Return the stored watermark_value, or `default` if no watermark exists.

        This is the most common call in pipeline notebooks:
            last_date = wm.get_watermark_value(PIPELINE_DAILY, "daily_time_series")
            new_df = df.filter(col("day_date") > lit(last_date))
        """
        state = self.read_watermark(pipeline_name, dataset_name)
        
        if state is None:
            logger.info("[WATERMARK] No prior watermark — using default: %s", default)
            return default
        return state.get("watermark_value", default)

    # ========================================================
    # HASH-BASED CHANGE DETECTION (TODO)
    # ========================================================

    def compute_content_hash(self, df, exclude_columns=None):
        """Compute SHA-256 hash of business columns for change detection."""
        # TODO: implement using df.withColumn("hash", ...)
        pass

    def hash_changed(self, pipeline_name: str, dataset_name: str, new_hash: str) -> bool:
        """Compare new hash against stored watermark hash."""
        # TODO: implement using read_watermark + compare
        pass


# ============================================================
# Instantiate a singleton instance available after %run
# ============================================================

wm = WatermarkManager(spark)

print("[WATERMARK] WatermarkManager initialized and available as `wm`")
print(f"[WATERMARK] Base path: {WATERMARK_BASE_PATH}")