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

S3_BUCKET_NAME = "graywolf--data--lake"

logger.info(
    "[INGESTION][TWELVEDATA][CONFIG] "
    "S3 bucket name loaded from environment | bucket=%s",
    S3_BUCKET_NAME,
)


# ============================================================
# TWELVE DATA CONFIGURATION
# ============================================================

TwelveData_BASE_URL = "https://api.twelvedata.com/time_series"

# Seconds before an Twelve Data HTTP request times out
REQUEST_TIMEOUT_SECONDS = 10

# Bronze source= partition when the event has no "datasource"
DEFAULT_DATASOURCE = "twelvedata"

# Symbols ingested when neither the event nor STOCK_SYMBOLS has any
DEFAULT_STOCK_SYMBOLS = ["IBM"]


TwelveData_Endpoints = [
    {
        "function": "TIME_SERIES",
        "dataset": "time_series",
        "description": (
            "OHLCV stock prices, volume, and trading metrics"
        ),
    },
    # {
    #     "function": "OVERVIEW",
    #     "dataset": "company_overview",
    #     "description": (
    #         "Company fundamental attributes, financials, ratios, and metadata"
    #     ),
    # },
]

logger.info(
    "[INGESTION][TWELVEDATA][CONFIG] "
    "Loaded %d Twelve Data endpoint definition(s): %s",
    len(TwelveData_Endpoints),
    [ep["function"] for ep in TwelveData_Endpoints],
)
