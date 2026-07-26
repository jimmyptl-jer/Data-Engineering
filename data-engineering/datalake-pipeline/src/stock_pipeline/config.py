"""
Centralized configuration for the stock data pipeline.
"""

import os
from dotenv import load_dotenv

load_dotenv()


# Alpha Vantage
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

# AWS
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# S3
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")