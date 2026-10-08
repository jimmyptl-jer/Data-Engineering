"""
API Key Utility Module.

Provides the `APIKeyManager` class to manage API key rotation for Alpha Vantage.
"""

import logging
import os
import random

logger = logging.getLogger(__name__)

# Raw list of environment variable API key slots (up to 16 keys)
_RAW_API_KEYS = [
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

# Filter out None and empty key slots
VALID_API_KEYS = [k for k in _RAW_API_KEYS if k]


class APIKeyManager:
    """
    Manages API key pool rotation for Alpha Vantage REST API requests.
    """

    def __init__(self):
        """
        Initialize the API Key Manager.

        Raises:
            ValueError: If no valid API keys are configured in environment variables.
        """
        if not VALID_API_KEYS:
            raise ValueError(
                "No valid Alpha Vantage API keys found. Check that at least "
                "one ALPHA_VANTAGE_API_KEY* environment variable is set."
            )

        self.api_keys = VALID_API_KEYS
        logger.info("[UTIL][API_KEY_INIT] APIKeyManager initialized with %d valid key(s).", len(self.api_keys))

    def get_key(self) -> str:
        """
        Randomly select and return an API key from the active key pool.

        Returns:
            str: Alpha Vantage API key string.
        """
        key = random.choice(self.api_keys)
        logger.debug("[UTIL][KEY_ISSUED] Issued key ending in: ...%s", key[-4:] if len(key) > 4 else "****")
        return key