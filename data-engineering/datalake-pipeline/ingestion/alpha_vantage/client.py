"""
Alpha Vantage Ingestion — API Client.

`fetch_endpoint()` makes one request for one symbol and API function and
validates the payload. `fetch_alpha_vantage_api_data()` is the raw HTTP call.
"""

import logging

import requests

from config import ALPHA_VANTAGE_BASE_URL, REQUEST_TIMEOUT_SECONDS
from keys import get_key


logger = logging.getLogger(__name__)


# ============================================================
# FETCH ONE SYMBOL / FUNCTION
# ============================================================

def fetch_endpoint(
    symbol: str,
    function: str,
    additional_params: dict | None = None,
) -> dict:
    """
    Fetch one Alpha Vantage API function for one symbol.

    Args:
        symbol: Stock ticker symbol.
        function: Alpha Vantage function (e.g. TIME_SERIES_DAILY, OVERVIEW).
        additional_params: Extra query parameters (e.g. outputsize).

    Returns:
        dict: Decoded JSON payload.

    Raises:
        ValueError: If Alpha Vantage returns an "Error Message".
        requests.exceptions.RequestException: On HTTP or network errors.
    """

    # ====================================================
    # SELECT API KEY
    # ====================================================

    api_key = get_key()

    logger.info(
        "[INGESTION][ALPHAVANTAGE][API_KEY_READY] "
        "API key selected successfully | symbol=%s",
        symbol,
    )

    # ====================================================
    # BUILD API PARAMETERS
    # ====================================================

    params = {
        "function": function,
        "symbol": symbol,
        "apikey": api_key,
    }

    if additional_params:

        params.update(additional_params)

        logger.info(
            "[INGESTION][ALPHAVANTAGE][ADDITIONAL_PARAMS] "
            "Additional API parameters added | "
            "symbol=%s | params=%s",
            symbol,
            {
                key: value
                for key, value in additional_params.items()
            },
        )

    logger.info(
        "[INGESTION][ALPHAVANTAGE][API_PARAMS] "
        "API parameters prepared | "
        "function=%s | symbol=%s",
        function,
        symbol,
    )

    # ====================================================
    # FETCH DATA
    # ====================================================

    logger.info(
        "[INGESTION][ALPHAVANTAGE][API_REQUEST_START] "
        "Calling Alpha Vantage API | "
        "symbol=%s | function=%s",
        symbol,
        function,
    )

    data = fetch_alpha_vantage_api_data(
        params=params
    )

    logger.info(
        "[INGESTION][ALPHAVANTAGE][API_REQUEST_SUCCESS] "
        "Alpha Vantage API response received | "
        "symbol=%s | data_type=%s",
        symbol,
        type(data).__name__,
    )

    validate_response(
        data,
        symbol=symbol,
        function=function,
    )

    return data


# ============================================================
# RESPONSE VALIDATION
# ============================================================

def validate_response(
    data,
    symbol: str,
    function: str,
) -> None:
    """
    Basic payload checks.

    Alpha Vantage answers errors and rate limits with HTTP 200, so the
    payload itself has to be inspected:
        - "Error Message" → raise ValueError
        - "Note"          → warning (rate-limit / informational message)
        - empty payload   → warning
    """

    logger.debug(
        "[INGESTION][ALPHAVANTAGE][VALIDATION_START] "
        "Starting basic response validation | symbol=%s",
        symbol,
    )

    if not data:

        logger.warning(
            "[INGESTION][ALPHAVANTAGE][EMPTY_RESPONSE] "
            "Alpha Vantage returned an empty response | "
            "symbol=%s | function=%s",
            symbol,
            function,
        )

    if isinstance(data, dict):

        if "Error Message" in data:

            logger.error(
                "[INGESTION][ALPHAVANTAGE][API_ERROR] "
                "Alpha Vantage returned an error message | "
                "symbol=%s | function=%s",
                symbol,
                function,
            )

            raise ValueError(
                data["Error Message"]
            )

        if "Note" in data:

            logger.warning(
                "[INGESTION][ALPHAVANTAGE][API_NOTE] "
                "Alpha Vantage returned a note/rate-limit message | "
                "symbol=%s | function=%s",
                symbol,
                function,
            )

    logger.debug(
        "[INGESTION][ALPHAVANTAGE][VALIDATION_COMPLETE] "
        "Response validation completed | symbol=%s",
        symbol,
    )


# ============================================================
# HTTP REQUEST
# ============================================================

def fetch_alpha_vantage_api_data(
    params: dict | None = None,
) -> dict:
    """
    Execute HTTP GET request against the Alpha Vantage REST API.

    Args:
        params:
            Dictionary containing query parameters:
            function, symbol, apikey, and optional parameters.

    Returns:
        dict:
            Decoded JSON response payload from Alpha Vantage.
    """

    symbol = params.get("symbol") if params else None
    function = params.get("function") if params else None

    logger.info(
        "[EXTRACT][API_CALL] "
        "Executing HTTP GET to Alpha Vantage | "
        "symbol=%s | function=%s",
        symbol,
        function,
    )

    logger.debug(
        "[EXTRACT][API_CALL_PARAMS] "
        "Request query params (excluding apikey) | symbol=%s | params=%s",
        symbol,
        {k: v for k, v in (params or {}).items() if k != "apikey"},
    )

    try:

        response = requests.get(
            ALPHA_VANTAGE_BASE_URL,
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

        logger.info(
            "[EXTRACT][API_RESPONSE] "
            "Received response | "
            "symbol=%s | HTTP status=%s",
            symbol,
            response.status_code,
        )

        logger.debug(
            "[EXTRACT][API_RESPONSE_SIZE] "
            "Raw response payload size | "
            "symbol=%s | size_bytes=%s",
            symbol,
            len(response.content) if response.content else 0,
        )

        response.raise_for_status()

        response_data = response.json()

        logger.info(
            "[EXTRACT][API_RESPONSE_JSON] "
            "Successfully decoded JSON response | "
            "symbol=%s | data_type=%s",
            symbol,
            type(response_data).__name__,
        )

        if isinstance(response_data, dict):

            logger.debug(
                "[EXTRACT][API_KEYS] "
                "Top-level keys returned for symbol=%s: %s",
                symbol,
                list(response_data.keys()),
            )

        return response_data

    except requests.exceptions.HTTPError as e:

        logger.exception(
            "[EXTRACT][API_FAIL] "
            "HTTP Error | symbol=%s | error=%s",
            symbol,
            e,
        )

        raise

    except requests.exceptions.Timeout as e:

        logger.exception(
            "[EXTRACT][API_FAIL] "
            "Timeout Error | symbol=%s | error=%s",
            symbol,
            e,
        )

        raise

    except requests.exceptions.ConnectionError as e:

        logger.exception(
            "[EXTRACT][API_FAIL] "
            "Connection Error | symbol=%s | error=%s",
            symbol,
            e,
        )

        raise

    except requests.exceptions.RequestException as e:

        logger.exception(
            "[EXTRACT][API_FAIL] "
            "Request Error | symbol=%s | error=%s",
            symbol,
            e,
        )

        raise

    except ValueError as e:

        logger.exception(
            "[EXTRACT][API_FAIL] "
            "JSON Decode Error | symbol=%s | error=%s",
            symbol,
            e,
        )

        raise
