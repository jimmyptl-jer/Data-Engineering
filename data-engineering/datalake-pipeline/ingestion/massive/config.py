"""
Massive Ingestion — Configuration.

Environment (set by template.yaml):
    MASSIVE_API_KEY   Massive API key (required)
    S3_BUCKET_NAME    Bronze bucket (required)
"""

import os


# ============================================================
# ENVIRONMENT VARIABLES
# ============================================================

MASSIVE_API_KEY = os.environ["MASSIVE_API_KEY"]

S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]


# ============================================================
# BRONZE & DEFAULTS
# ============================================================

# Bronze source= partition
DATASOURCE = "massive"

# Symbol for the per-symbol datasets when the event has no "symbol"
DEFAULT_SYMBOL = "AAPL"
