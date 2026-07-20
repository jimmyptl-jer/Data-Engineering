"""
Entry point — ties extract -> transform -> load together, and exposes
lambda_handler for AWS Lambda/SAM (topics 54, Lambda gotchas, SAM section).
"""

import logging
import json
from datetime import datetime, timezone
from coinbase.rest import RESTClient

from . import config


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

class BinancePipeline:
    def __init__(self):
        self.aws_access_key_id = config.AWS_ACCESS_KEY_ID
        self.aws_secret_access_key = config.AWS_SECRET_ACCESS_KEY
        self.s3_bucket_name = config.S3_BUCKET_NAME
        

    def run(self):
        logger.info("Starting Binance pipeline...")
        # Here you would implement the extract, transform, and load logic.
        # For example:
        # data = self.extract()
        # transformed_data = self.transform(data)
        # self.load(transformed_data)
        logger.info("Binance pipeline completed.")
        
    def get(self, params=None):
        """
        Helper method to make GET requests to the Binance API.
        """
        import requests
        client = RESTClient()
        
        url = "https://api.coinbase.com/api/v3/brokerage/products?product_type=UNKNOWN_PRODUCT_TYPE&contract_expiry_type=UNKNOWN_CONTRACT_EXPIRY_TYPE&expiring_contract_status=UNKNOWN_EXPIRING_CONTRACT_STATUS&products_sort_order=PRODUCTS_SORT_ORDER_UNDEFINED&futures_underlying_type=UNKNOWN_FUTURES_UNDERLYING_TYPE"
        
        
        headers = {"Authorization": "Bearer ${COINBASE_API_KEY}"}
        
        try:
            response = requests.get(url, params=params, timeout=10, headers=headers)
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"Error occurred while making GET request to {url}: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception occurred while making GET request to {url}: {e}")
            raise

        logger.info(f"GET request to {url} returned status code {response.status_code}")
        logger.info(f"Response content: {response.text}")
        response.raise_for_status()
        return response.json()
    
    def push_to_s3(self, data, bucket_name):
        
        key = f"binance_data_{datetime.now(timezone.utc).isoformat()}.json"
        import boto3
        
        s3 = boto3.client("s3", aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key)
        
        res = s3.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(data))
        
        return res

class YouTubePipeline:
    def __init__(self):
        self.db_host = config.DB_HOST
        self.db_port = config.DB_PORT
        self.db_user = config.DB_USER
        self.db_password = config.DB_PASSWORD
        self.db_name = config.DB_NAME
        self.youtube_api_key = config.YOUTUBE_API_KEY
        self.aws_access_key_id = config.AWS_ACCESS_KEY_ID
        self.aws_secret_access_key = config.AWS_SECRET_ACCESS_KEY
        self.s3_bucket_name = config.S3_BUCKET_NAME
        
        logger.info(f"Youtube api key: {self.youtube_api_key}")

    def run(self):
        logger.info("Starting YouTube pipeline...")
        # Here you would implement the extract, transform, and load logic.
        # For example:
        # data = self.extract()
        # transformed_data = self.transform(data)
        # self.load(transformed_data)
        logger.info("YouTube pipeline completed.")
        
    def get(self, endpoint, params=None):
        """
        Helper method to make GET requests to the YouTube API.
        """
        import requests
        
        url = f"https://www.googleapis.com/youtube/v3/{endpoint}"
        
        
        try:
            response = requests.get(url, params=params, timeout=10)
        except requests.exceptions.HTTPError as e:
            logger.error(f"Error occurred while making GET request to {url}: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception occurred while making GET request to {url}: {e}")
            raise

        logger.info(f"GET request to {url} returned status code {response.status_code}")
        logger.info(f"Response content: {response.text}")
        response.raise_for_status()
        return response.json()
    
    def push_to_s3(self,data,bucket_name):
        
        key = f"youtube_data_{datetime.now(timezone.utc).isoformat()}.json"
        import boto3
        
        s3 = boto3.client("s3", aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key)
        
        res = s3.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(data))
        
        return res

def lambda_handler(event, context):
    logger.info("Lambda function invoked.")
    youtube_pipeline = YouTubePipeline()
    
    logger.info(f"Lambda function run() started at {datetime.now(timezone.utc).isoformat()}")
    youtube_pipeline.run()
    logger.info(f"Lambda function run() completed at {datetime.now(timezone.utc).isoformat()}")
    
    params = {
        "part": "snippet",
        "q": "AWS Data Engineering",
        "type": "video",
        "maxResults": 100,
        "order": "date",
        "key": youtube_pipeline.youtube_api_key,
    }
    
    videos_data = youtube_pipeline.get("search", params=params)
    logger.info(json.dumps(videos_data, indent=4))
    
    s3_response = youtube_pipeline.push_to_s3(videos_data, youtube_pipeline.s3_bucket_name)
    
    logger.info(f"Retrieved {len(videos_data.get('items', []))} videos from YouTube API.")
    logger.info(f"S3 response: {s3_response}")  # Print the retrieved videos data for debugging purposes
    
    
    binance_pipeline = BinancePipeline()
    logger.info(f"Lambda function run() started at {datetime.now(timezone.utc).isoformat()}")
    binance_pipeline.run()
    logger.info(f"Lambda function run() completed at {datetime.now(timezone.utc).isoformat()}")
    
    binance_data = binance_pipeline.get()
    logger.info(json.dumps(binance_data, indent=4))
    
    return {
        'statusCode': 200,
        'body': 'YouTube pipeline executed successfully.',
        's3_response': s3_response
    }


if __name__ == "__main__":
    lambda_handler({}, {})