"""
Centralized configuration for the watermark subsystem.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# AWS
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# S3
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

WATERMARK_BASE_PATH = (
    "s3a://graywolf--data--lake/"
    "watermark/"
)