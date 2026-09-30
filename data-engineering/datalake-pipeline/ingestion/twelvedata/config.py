"""
Twelve Data Ingestion — Configuration.

All settings are hardcoded here; the Lambda reads no environment variables
(the API key is in keys.py).
"""

import logging


logger = logging.getLogger(__name__)


# ============================================================
# AWS CONFIGURATION
# ============================================================

S3_BUCKET_NAME = "graywolf--data--lake"

logger.info(
    "[INGESTION][TWELVEDATA][CONFIG] "
    "S3 bucket name loaded | bucket=%s",
    S3_BUCKET_NAME,
)


# ============================================================
# TWELVE DATA CONFIGURATION
# ============================================================

TWELVEDATA_BASE_URL = "https://api.twelvedata.com/time_series"

# Seconds before a Twelve Data HTTP request times out
REQUEST_TIMEOUT_SECONDS = 10

# Bronze source= partition when the event has no "datasource"
DEFAULT_DATASOURCE = "twelvedata"

# 1-minute bars in one regular US trading session (09:30-16:00 ET)
BARS_PER_TRADING_DAY = 390

# Symbols ingested when the event has none
DEFAULT_STOCK_SYMBOLS = ["IBM"]


TWELVEDATA_ENDPOINTS = [
    {
        "function": "TIME_SERIES",
        "dataset": "time_series_1min",
        "interval": "1min",
        "description": (
            "1-minute OHLCV bars for the trading day"
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
    len(TWELVEDATA_ENDPOINTS),
    [ep["function"] for ep in TWELVEDATA_ENDPOINTS],
)
