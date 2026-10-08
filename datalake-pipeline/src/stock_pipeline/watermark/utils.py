import logging
import random
import os

logger = logging.getLogger(__name__)

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

api_key = random.choices(ALPHA_VANTAGE_API_KEYS, k=len(ALPHA_VANTAGE_API_KEYS))

class APIKeyManager:
    def __init__(self):
        if not api_key:
            raise ValueError(
                "No valid Alpha Vantage API keys found. Check that at least "
                "one ALPHA_VANTAGE_API_KEY* environment variable is set."
            )

        if len(api_key) < len(ALPHA_VANTAGE_API_KEYS):
            logger.warning(
                "%d of %d configured Alpha Vantage API key slots were empty "
                "and have been excluded from rotation.",
                len(ALPHA_VANTAGE_API_KEYS) - len(api_key),
                len(ALPHA_VANTAGE_API_KEYS),
            )

        self.api_keys = api_key
        logger.info("[STEP UTIL][APIKEY_INIT] APIKeyManager initialized with %d valid key(s).", len(self.api_keys))

    def get_key(self):
        key = random.choice(self.api_keys)
        logger.debug("Issued API key ending in: ...%s", key[-4:] if len(key) > 4 else "****")
        return key