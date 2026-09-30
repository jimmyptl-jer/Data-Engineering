"""
Alpha Vantage Ingestion — Configuration.

Environment (set by template.yaml):
    S3_BUCKET_NAME   Bronze bucket (required)
    STOCK_SYMBOLS    Comma-separated fallback symbols when the event has
                     none (read in app.py)
"""

import logging
import os


logger = logging.getLogger(__name__)


# ============================================================
# AWS CONFIGURATION
# ============================================================

S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]

logger.info(
    "[INGESTION][ALPHAVANTAGE][CONFIG] "
    "S3 bucket name loaded from environment | bucket=%s",
    S3_BUCKET_NAME,
)


# ============================================================
# ALPHA VANTAGE CONFIGURATION
# ============================================================

ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"

# Seconds before an Alpha Vantage HTTP request times out
REQUEST_TIMEOUT_SECONDS = 10

# Bronze source= partition when the event has no "datasource"
DEFAULT_DATASOURCE = "alphavantage"

# Symbols ingested when neither the event nor STOCK_SYMBOLS has any
DEFAULT_STOCK_SYMBOLS = ["IBM"]


ALPHA_VANTAGE_ENDPOINTS = [
    {
        "function": "TIME_SERIES_DAILY",
        "dataset": "daily_time_series",
        "description": (
            "Daily OHLCV stock prices, volume, and trading metrics"
        ),
    },
    {
        "function": "OVERVIEW",
        "dataset": "company_overview",
        "description": (
            "Company fundamental attributes, financials, ratios, and metadata"
        ),
    },
]

logger.info(
    "[INGESTION][ALPHAVANTAGE][CONFIG] "
    "Loaded %d Alpha Vantage endpoint definition(s): %s",
    len(ALPHA_VANTAGE_ENDPOINTS),
    [ep["function"] for ep in ALPHA_VANTAGE_ENDPOINTS],
)
