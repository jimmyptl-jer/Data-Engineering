"""
Massive Ingestion — API Client.

The Massive (Polygon.io) SDK client shared by every dataset module.
"""

from massive import RESTClient

from config import MASSIVE_API_KEY


# ============================================================
# CLIENT
# ============================================================

massive_client = RESTClient(
    MASSIVE_API_KEY
)
