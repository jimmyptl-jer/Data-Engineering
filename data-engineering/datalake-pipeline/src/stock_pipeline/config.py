"""
Centralized Configuration & Storage Architecture Module.

Defines global pipeline constants, API endpoint mappings, data lake S3 path conventions,
watermark strategy mappings, and environment variables.

Data Lake Path Hierarchy:
  s3a://{S3_BUCKET_NAME}/
    └── stock/
        ├── bronze/source={source}/dataset={dataset}/ingestion_date=YYYY-MM-DD/run_id={run_id}/{file}.json
        ├── silver/datasource={source}/dataset={dataset}/year=YYYY/month=MM/format={csv|parquet}/
        └── gold/datasource=stock/dataset=company_dataset/year=YYYY/month=MM/format=csv/

  Bronze is written by the ingestion Lambdas (ingestion/<source>/bronze.py).
  SILVER_BASE_PATH and GOLD_BASE_PATH below are fixed to graywolf--data--lake.

Incremental Processing Strategies:
  - daily_time_series  : 'date_based'  (Filters rows where day_date > watermark_value)
  - company_overview   : 'hash_based'  (Compares SHA-256 digest of business columns)
"""

import os
from dotenv import load_dotenv

# Load local environment variables from .env file if present
load_dotenv()

# ============================================================
# ALPHA VANTAGE ENDPOINTS & DATASET MAPPINGS
# ============================================================

ALPHA_VANTAGE_ENDPOINTS = [
    {
        "function": "TIME_SERIES_DAILY",
        "dataset": "daily_time_series",
        "description": "Daily OHLCV stock prices, volume, and trading metrics",
    },
    {
        "function": "OVERVIEW",
        "dataset": "company_overview",
        "description": "Company fundamental attributes, financials, ratios, and metadata",
    },
]


def get_dataset_name_by_function(function: str) -> str:
    """
    Lookup and return the dataset directory name for a given Alpha Vantage API function.

    Args:
        function (str): Alpha Vantage function name (e.g. 'TIME_SERIES_DAILY', 'OVERVIEW').

    Returns:
        str: Target dataset folder name (e.g. 'daily_time_series', 'company_overview').

    Raises:
        ValueError: If the function is not registered in `ALPHA_VANTAGE_ENDPOINTS`.
    """
    for endpoint in ALPHA_VANTAGE_ENDPOINTS:
        if endpoint["function"] == function:
            return endpoint["dataset"]

    raise ValueError(f"Function '{function}' is not registered in ALPHA_VANTAGE_ENDPOINTS configuration.")


# ============================================================
# INCREMENTAL WATERMARK STRATEGIES
# ============================================================

WATERMARK_STRATEGIES = {
    "daily_time_series": "date_based",  # Transactional time-series data
    "company_overview": "hash_based",   # Reference / master data snapshot
}

# ============================================================
# S3 MEDALLION DATA LAKE BASE PATHS
# ============================================================

SILVER_BASE_PATH = (
    "s3a://graywolf--data--lake/"
    "stock/"
    "silver/"
)

GOLD_BASE_PATH = (
    "s3a://graywolf--data--lake/"
    "stock/"
    "gold/"
)

# ============================================================
# ENVIRONMENT
# ============================================================

# Target S3 Data Lake Bucket
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "graywolf--data--lake")