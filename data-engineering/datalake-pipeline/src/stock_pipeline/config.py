"""
Centralized configuration for the stock data pipeline.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# ALPHA VANTAGE ENDPOINTS
# ============================================================

ALPHA_VANTAGE_ENDPOINTS = [
    {
        "function": "TIME_SERIES_DAILY",
        "dataset": "daily_time_series",
    },
    {
        "function": "OVERVIEW",
        "dataset": "company_overview",
    },
]


def get_dataset_name_by_function(function: str) -> str:
    """
    Get the dataset name corresponding to the given Alpha Vantage function.

    Args:
        function (str): The Alpha Vantage function name.

    Returns:
        str: The corresponding dataset name.

    Raises:
        ValueError: If the function is not found in the configuration.
    """
    for endpoint in ALPHA_VANTAGE_ENDPOINTS:
        if endpoint["function"] == function:
            return endpoint["dataset"]

    raise ValueError(f"Function '{function}' not found in the configuration.")


# ============================================================
# INCREMENTAL STRATEGIES
# ============================================================

WATERMARK_STRATEGIES = {
    "daily_time_series": "date_based",
    "company_overview": "hash_based",
}

# ============================================================
# S3 LAYER PATHS
# ============================================================

SILVER_BASE_PATH = (
    "s3a://graywolf--data--lake/"
    "stock/"
    "silver/"
    "source=alphavantage/"
)

GOLD_BASE_PATH = (
    "s3a://graywolf--data--lake/"
    "stock/"
    "gold/"
    "source=alphavantage/"
)

# Alpha Vantage
ALPHA_VANTAGE_API_KEYS = [
    os.getenv("ALPHA_VANTAGE_API_KEY"),
    os.getenv("ALPHA_VANTAGE_API_KEY_1"),
    os.getenv("ALPHA_VANTAGE_API_KEY_2"),
    os.getenv("ALPHA_VANTAGE_API_KEY_3"),
    os.getenv("ALPHA_VANTAGE_API_KEY_4"),
    os.getenv("ALPHA_VANTAGE_API_KEY_5"),
    os.getenv("ALPHA_VANTAGE_API_KEY_6"),
    os.getenv("ALPHA_VANTAGE_API_KEY_7"),
    os.getenv("ALPHA_VANTAGE_API_KEY_8"),
    os.getenv("ALPHA_VANTAGE_API_KEY_9"),
    os.getenv("ALPHA_VANTAGE_API_KEY_10"),
    os.getenv("ALPHA_VANTAGE_API_KEY_11"),
    os.getenv("ALPHA_VANTAGE_API_KEY_12"),
    os.getenv("ALPHA_VANTAGE_API_KEY_13"),
    os.getenv("ALPHA_VANTAGE_API_KEY_14"),
    os.getenv("ALPHA_VANTAGE_API_KEY_15"),
]


# AWS
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# S3
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")