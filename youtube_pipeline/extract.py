import logging

import requests
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    ArrayType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    BooleanType,
    DateType,
    TimestampType,
)

from . import config

logger = logging.getLogger(__name__)

YOUTUBE_API_BASE_URL = "https://www.googleapis.com/youtube/v3"

# ============================================================
# YOUTUBE JSON SCHEMA
# ============================================================

youtube_json_schema = StructType([
    StructField("etag", StringType(), True),
    StructField("kind", StringType(), True),
    StructField("nextPageToken", StringType(), True),
    StructField("regionCode", StringType(), True),

    StructField(
        "items",
        ArrayType(
            StructType([
                StructField("etag", StringType(), True),
                StructField(
                    "id",
                    StructType([
                        StructField("kind", StringType(), True),
                        StructField("videoId", StringType(), True),
                    ]),
                    True,
                ),
                StructField("kind", StringType(), True),
                StructField(
                    "snippet",
                    StructType([
                        StructField("assignable", StringType(), True),
                        StructField("channelId", StringType(), True),
                        StructField("channelTitle", StringType(), True),
                        StructField("description", StringType(), True),
                        StructField("liveBroadcastContent", StringType(), True),
                        StructField("publishTime", StringType(), True),
                        StructField("publishedAt", StringType(), True),
                        StructField("thumbnails", StringType(), True),
                        StructField("title", StringType(), True),
                    ]),
                    True,
                ),
            ])
        ),
        True,
    ),
])

# ============================================================
# YOUTUBE CSV SCHEMA
# ============================================================

youtube_schema = StructType([
    StructField("video_id", StringType(), False),
    StructField("trending_date", DateType(), True),
    StructField("title", StringType(), False),
    StructField("channel_title", StringType(), True),
    StructField("category_id", IntegerType(), True),
    StructField("publish_time", TimestampType(), True),
    StructField("tags", StringType(), True),
    StructField("views", LongType(), True),
    StructField("likes", LongType(), True),
    StructField("dislikes", LongType(), True),
    StructField("comment_count", LongType(), True),
    StructField("thumbnail_link", StringType(), True),
    StructField("comments_disabled", BooleanType(), True),
    StructField("ratings_disabled", BooleanType(), True),
    StructField("video_error_or_removed", BooleanType(), True),
    StructField("description", StringType(), True),
])


class YouTubeDataExtractor:
    """
    Handles extraction of YouTube data from the Bronze S3 layer
    and from the YouTube Data API.

    Supported formats:
    - JSON (raw API response, explicit nested schema)
    - CSV  (trending-videos dataset, explicit flat schema)
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.bucket_name = config.S3_BUCKET_NAME

    # ============================================================
    # YOUTUBE API FETCH
    # ============================================================

    def fetch_from_api(self, endpoint, params=None):
        """
        Make a GET request to the YouTube Data API and return the
        decoded JSON response.
        """

        url = f"{YOUTUBE_API_BASE_URL}/{endpoint}"

        logger.info(
            "Sending GET request to YouTube API endpoint: %s",
            endpoint,
        )

        try:
            response = requests.get(
                url,
                params=params,
                timeout=10,
            )

            logger.info(
                "YouTube API response received. "
                "Endpoint: %s, Status code: %s",
                endpoint,
                response.status_code,
            )

            response.raise_for_status()

            response_data = response.json()

            logger.info(
                "YouTube API request completed successfully. "
                "Items returned: %s",
                len(response_data.get("items", [])),
            )

            return response_data

        except requests.exceptions.HTTPError as e:
            logger.exception(
                "HTTP error while calling YouTube API "
                "endpoint %s: %s",
                endpoint,
                e,
            )
            raise

        except requests.exceptions.RequestException as e:
            logger.exception(
                "Request error while calling YouTube API "
                "endpoint %s: %s",
                endpoint,
                e,
            )
            raise

        except ValueError as e:
            logger.exception(
                "Failed to decode YouTube API JSON response: %s",
                e,
            )
            raise

    # ============================================================
    # JSON EXTRACTION
    # ============================================================

    def extract_data(self) -> DataFrame:
        """
        Extract YouTube JSON data from the Bronze S3 bucket
        and return it as a Spark DataFrame.
        """

        bucket_key = f"s3a://{self.bucket_name}/youtube/"

        logger.info("Extracting YouTube JSON data from: %s", bucket_key)

        try:
            youtube_data_df = (
                self.spark.read
                .format("json")
                .option("multiLine", "true")
                .option("recursiveFileLookup", "true")
                .schema(youtube_json_schema)
                .load(bucket_key)
            )

            logger.info("YouTube JSON extraction completed successfully.")

            # ====================================================
            # DATA UNDERSTANDING
            # ====================================================

            print("\n========== YOUTUBE JSON SCHEMA ==========")
            youtube_data_df.printSchema()

            return youtube_data_df

        except Exception as e:
            logger.exception(
                "Error occurred while extracting YouTube JSON data: %s",
                e,
            )
            raise

    # ============================================================
    # CSV EXTRACTION
    # ============================================================

    def extract_data_csv(self) -> DataFrame:
        """
        Extract YouTube CSV data from the Bronze S3 bucket,
        apply the predefined schema, and return it as a Spark DataFrame.
        """

        bucket_key = f"s3a://{self.bucket_name}/youtube/youtube_dataset"

        logger.info("Extracting YouTube CSV data from: %s", bucket_key)

        try:
            youtube_data_df = (
                self.spark.read
                .format("csv")
                .option("header", "true")
                .option("mode", "PERMISSIVE")
                .option("recursiveFileLookup", "true")
                .schema(youtube_schema)
                .load(bucket_key)
            )

            logger.info("YouTube CSV extraction completed successfully.")

            # ====================================================
            # DATA UNDERSTANDING
            # ====================================================

            print("\n========== YOUTUBE CSV SCHEMA ==========")
            youtube_data_df.printSchema()

            # ====================================================
            # SAVE APPLIED SCHEMA
            # ====================================================

            applied_schema_json = youtube_data_df.schema.json()
            schema_file_path = "youtube_schema.json"

            with open(schema_file_path, "w", encoding="utf-8") as file:
                file.write(applied_schema_json)

            logger.info(
                "Applied YouTube schema saved to: %s",
                schema_file_path,
            )

            return youtube_data_df

        except Exception as e:
            logger.exception(
                "Error occurred while extracting YouTube CSV data: %s",
                e,
            )
            raise