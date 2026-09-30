"""
Finnhub Ingestion — Configuration.

Environment (set by template.yaml):
    FINNHUB_API_KEY   Finnhub API key (required)
    S3_BUCKET_NAME    Bronze bucket (required)
"""

import os


# ============================================================
# ENVIRONMENT VARIABLES
# ============================================================

FINNHUB_API_KEY = os.environ["FINNHUB_API_KEY"]
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]


# ============================================================
# FINNHUB API
# ============================================================

FINNHUB_BASE_URL = "https://finnhub.io/api/v1"

# Seconds before a Finnhub HTTP request times out
REQUEST_TIMEOUT_SECONDS = 60

# Exchange ingested when the event has no "exchange"
DEFAULT_EXCHANGE = "US"


# ============================================================
# BRONZE
# ============================================================

DATASOURCE = "finnhub"
DATASET = "ticker_reference"
