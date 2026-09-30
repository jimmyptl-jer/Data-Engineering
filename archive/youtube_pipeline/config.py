"""
Centralized configuration — every os.getenv() call in the project lives here.
Anyone (including future you) can open this one file and see every
environment variable the app depends on.
"""

from dotenv import load_dotenv
import os

load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")