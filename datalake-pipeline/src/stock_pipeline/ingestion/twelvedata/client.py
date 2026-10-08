"""
Twelve Data Ingestion — API Client.

`fetch_endpoint()` makes one request for one symbol and API function and
validates the payload. `fetch_twelvedata_api_data()` is the raw HTTP call.
"""

from __future__ import annotations

import logging

import requests

from config import REQUEST_TIMEOUT_SECONDS, TWELVEDATA_BASE_URL
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
    Fetch one Twelve Data API function for one symbol.

    Args:
        symbol: Stock ticker symbol.
        function: Endpoint name from config.TWELVEDATA_ENDPOINTS (e.g. TIME_SERIES).
        additional_params: Extra query parameters (e.g. interval, outputsize).

    Returns:
        dict: Decoded JSON payload.

    Raises:
        ValueError: If Twelve Data returns "status": "error".
        requests.exceptions.RequestException: On HTTP or network errors.
    """

    # ====================================================
    # SELECT API KEY
    # ====================================================

    api_key = get_key()

    params = {
        "symbol": symbol,
        "interval": "1min",
        "apikey": api_key,
    }

    if additional_params:
        params.update(additional_params)

    data = fetch_twelvedata_api_data(
        params=params
    )

    logger.info(
        "[INGESTION][TWELVEDATA][API_REQUEST_SUCCESS] "
        "Twelve Data API response received | "
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

    Twelve Data can answer errors and rate limits with HTTP 200, so the
    payload itself has to be inspected:
        - "status": "error" → raise ValueError (bad symbol/params, 429 rate limit)
        - empty payload     → warning
    """

    logger.debug(
        "[INGESTION][TWELVEDATA][VALIDATION_START] "
        "Starting basic response validation | symbol=%s",
        symbol,
    )

    if not data:

        logger.warning(
            "[INGESTION][TWELVEDATA][EMPTY_RESPONSE] "
            "Twelve Data returned an empty response | "
            "symbol=%s | function=%s",
            symbol,
            function,
        )

    if isinstance(data, dict):

        if data.get("status") == "error":

            logger.error(
                "[INGESTION][TWELVEDATA][API_ERROR] "
                "Twelve Data returned an error | "
                "symbol=%s | function=%s | code=%s",
                symbol,
                function,
                data.get("code"),
            )

            raise ValueError(
                data.get("message", "Twelve Data returned status=error")
            )

    logger.debug(
        "[INGESTION][TWELVEDATA][VALIDATION_COMPLETE] "
        "Response validation completed | symbol=%s",
        symbol,
    )


# ============================================================
# HTTP REQUEST
# ============================================================

def fetch_twelvedata_api_data(
    params: dict | None = None,
) -> dict:
    """
    Execute HTTP GET request against the Twelve Data REST API.

    Args:
        params:
            Dictionary containing query parameters:
            symbol, apikey, and optional parameters.

    Returns:
        dict:
            Decoded JSON response payload from Twelve Data.
    """

    symbol = params.get("symbol") if params else None

    try:

        response = requests.get(
            TWELVEDATA_BASE_URL,
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

        response.raise_for_status()

        response_data = response.json()

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
