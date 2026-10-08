"""
Entry point — ties extract -> transform -> load together, and exposes
lambda_handler for AWS Lambda/SAM.
"""

import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from . import config
from .extract import YouTubeDataExtractor
from .transform import YouTubeDataTransformer
from .load import YouTubeDataLoader


# ============================================================
# LOGGING CONFIGURATION
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# SPARK SESSION FACTORY
# ============================================================

def create_spark_session() -> SparkSession:
    """Create and return a configured SparkSession."""

    logger.info("Initializing Spark session for YouTube Data Pipeline.")

    try:
        spark = (
            SparkSession.builder
            .appName("YouTubeDataPipeline")
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.4.1",
            )
            .getOrCreate()
        )

        logger.info(
            "Spark session initialized successfully. Spark version: %s",
            spark.version,
        )

        return spark

    except Exception as e:
        logger.exception(
            "Failed to initialize Spark session: %s",
            e,
        )
        raise


# ============================================================
# YOUTUBE PIPELINE
# ============================================================

class YouTubePipeline:

    def __init__(self):

        logger.info("Initializing YouTube pipeline configuration.")

        self.youtube_api_key = config.YOUTUBE_API_KEY
        self.aws_access_key_id = config.AWS_ACCESS_KEY_ID
        self.aws_secret_access_key = config.AWS_SECRET_ACCESS_KEY
        self.s3_bucket_name = config.S3_BUCKET_NAME

        self.spark = create_spark_session()

        self.extractor = YouTubeDataExtractor(self.spark)
        self.transformer = YouTubeDataTransformer(self.spark)
        self.loader = YouTubeDataLoader(
            self.aws_access_key_id,
            self.aws_secret_access_key,
        )

        logger.info(
            "YouTube pipeline configuration initialized successfully."
        )

    # ========================================================
    # ETL PIPELINE
    # ========================================================

    def run(self):

        logger.info("Starting YouTube ETL pipeline.")

        try:
            # ====================================================
            # JSON EXTRACTION
            # ====================================================

            logger.info(
                "Starting YouTube JSON extraction from Bronze layer."
            )

            extracted_data = self.extractor.extract_data()

            logger.info(
                "YouTube JSON extraction completed successfully."
            )

            # ====================================================
            # JSON TRANSFORMATION
            # ====================================================

            logger.info(
                "Starting YouTube JSON transformation."
            )

            transformed_json_data = (
                self.transformer.transform_json_data(extracted_data)
            )

            silver_path = (
                f"s3a://graywolf--data--lake/"
                f"silver/youtube/source=youtube_api/"
            )

            transformed_json_data.write.mode("overwrite").option(
                "header", "true"
            ).csv(silver_path)

            logger.info(
                "YouTube JSON transformation completed successfully."
            )

            # ====================================================
            # CSV EXTRACTION
            # ====================================================

            logger.info(
                "Starting YouTube CSV extraction from Bronze layer."
            )

            extracted_data_csv = self.extractor.extract_data_csv()

            logger.info(
                "YouTube CSV extraction completed successfully."
            )

            # ====================================================
            # CSV TRANSFORMATION
            # ====================================================

            logger.info(
                "Starting YouTube CSV transformation."
            )

            transformed_csv_data = (
                self.transformer.transform_csv_data(extracted_data_csv)
            )

            logger.info(
                "YouTube CSV transformation completed successfully."
            )

            # ====================================================
            # DATA UNDERSTANDING / DEBUGGING
            # ====================================================

            logger.info("Printing extracted JSON schema.")

            extracted_data.printSchema()

            logger.info("Printing transformed JSON schema.")

            transformed_json_data.printSchema()

            logger.info("Printing transformed CSV schema.")

            transformed_csv_data.printSchema()

            logger.info(
                "YouTube ETL pipeline completed successfully."
            )

        except Exception as e:
            logger.exception(
                "YouTube ETL pipeline failed: %s",
                e,
            )
            raise


# ============================================================
# AWS LAMBDA HANDLER
# ============================================================

def lambda_handler(event, context):

    start_time = datetime.now(timezone.utc)

    logger.info(
        "YouTube Lambda execution started at %s",
        start_time.isoformat(),
    )

    try:
        youtube_pipeline = YouTubePipeline()

        # ========================================================
        # FETCH NEW DATA FROM YOUTUBE API
        # ========================================================

        logger.info(
            "Starting YouTube API data ingestion."
        )

        params = {
            "part": "snippet",
            "q": "AWS Data Engineering",
            "type": "video",
            "maxResults": 100,
            "order": "date",
            "key": youtube_pipeline.youtube_api_key,
        }

        videos_data = youtube_pipeline.extractor.fetch_from_api(
            "search",
            params=params,
        )

        video_count = len(
            videos_data.get("items", [])
        )

        logger.info(
            "Retrieved %s videos from YouTube API.",
            video_count,
        )

        # ========================================================
        # STORE RAW DATA IN S3 BRONZE
        # ========================================================

        logger.info(
            "Starting raw YouTube data upload to Bronze S3."
        )

        s3_response = youtube_pipeline.loader.upload_raw_to_s3(
            videos_data,
            youtube_pipeline.s3_bucket_name,
        )

        logger.info(
            "Raw YouTube data upload completed successfully."
        )

        # ========================================================
        # RUN EXISTING ETL
        # ========================================================

        logger.info("Starting existing YouTube ETL processing.")

        youtube_pipeline.run()

        logger.info(
            "Existing YouTube ETL processing completed successfully."
        )

        # ========================================================
        # COMPLETION
        # ========================================================

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        logger.info(
            "YouTube Lambda execution completed successfully. "
            "Duration: %.2f seconds",
            duration,
        )

        return {
            "statusCode": 200,
            "body": "YouTube pipeline executed successfully.",
            "s3_response": s3_response,
        }

    except Exception as e:
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        logger.exception(
            "YouTube Lambda execution failed after %.2f seconds: %s",
            duration,
            e,
        )

        raise


# ============================================================
# LOCAL EXECUTION
# ============================================================

if __name__ == "__main__":

    logger.info(
        "Running YouTube pipeline locally."
    )

    lambda_handler({}, {})
