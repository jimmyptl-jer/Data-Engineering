import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, col


logger = logging.getLogger(__name__)


class YouTubeDataTransformer:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def transform_csv_data(
        self,
        data_df: DataFrame,
    ) -> DataFrame:

        logger.info("Starting YouTube data transformation.")

        try:
            logger.info("Selecting required YouTube columns.")

            youtube_df = data_df.select(
                "video_id",
                "trending_date",
                "title",
                "channel_title",
                "category_id",
                "publish_time",
                "tags",
                "views",
                "likes",
                "dislikes",
                "comment_count",
                "thumbnail_link",
                "comments_disabled",
                "ratings_disabled",
                "video_error_or_removed",
                "description",
            )

            logger.info(
                "Required YouTube columns selected successfully."
            )

            print("\n========== TRANSFORMED DATA ==========")
            youtube_df.show(5, truncate=False)

            print("\n========== TRANSFORMED SCHEMA ==========")
            youtube_df.printSchema()

            logger.info(
                "YouTube data transformation completed successfully."
            )

            return youtube_df

        except Exception as e:
            logger.exception(
                "Error occurred while transforming YouTube data: %s",
                e,
            )
            raise
    
    def transform_json_data(
        self,
        data_df: DataFrame,
    ) -> DataFrame:

        logger.info("Starting YouTube JSON data transformation.")

        try:
            logger.info("Selecting required YouTube columns.")

            youtube_df = data_df.withColumn(
                "item",
                explode(col("items"))
            )

            logger.info(
                "Required YouTube columns selected successfully."
            )

            youtube_df = youtube_df.select(
                col("item.id.videoId").alias("video_id"),
                col("item.snippet.channelId").alias("channel_id"),
                col("item.snippet.channelTitle").alias("channel_title"),
                col("item.snippet.description").alias("channel_description"),
                col("item.snippet.publishTime").alias("publish_time"),
                col("item.snippet.publishedAt").alias("publish_at"),
                col("kind")
            )
            print("\n========== TRANSFORMED SCHEMA ==========")
            youtube_df.printSchema()

            logger.info(
                "YouTube data transformation completed successfully."
            )

            return youtube_df

        except Exception as e:
            logger.exception(
                "Error occurred while transforming YouTube data: %s",
                e,
            )
            raise

