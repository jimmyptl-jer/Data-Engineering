"""
Massive Datasets — the registry of everything the Massive Lambda ingests.

Each MassiveDataset names the fetch function to call and where its result
lands in Bronze. app.lambda_handler runs them in this order on every
invocation.

To add a dataset: write datasets/<name>.py with an ingest_<name>() function
and add one entry to DATASETS.
"""

from dataclasses import dataclass
from typing import Any, Callable

from massive import RESTClient

from .aggregates import ingest_aggregates
from .dividends import ingest_dividends
from .exchanges import ingest_exchanges
from .splits import ingest_splits
from .stock_overview import ingest_stock_overview
from .tickers import ingest_tickers


# ============================================================
# REQUEST & DATASET DEFINITIONS
# ============================================================

@dataclass(frozen=True)
class MassiveRequest:
    """
    Inputs resolved from the Lambda event (see app.parse_request).
    """

    symbol: str
    from_date: str
    to_date: str | None


@dataclass(frozen=True)
class MassiveDataset:
    """
    One Massive endpoint ingested on every run.

    Attributes:
        result_key: Key of this dataset in the Lambda response "results".
        dataset: Bronze dataset= partition.
        label: Name used in log messages.
        fetch: Calls the endpoint and returns the serialized response.
        per_symbol: Written as <symbol>.json and reported with its symbol.
        single_record: The response is one record, counted as 1.
    """

    result_key: str
    dataset: str
    label: str
    fetch: Callable[[RESTClient, MassiveRequest], Any]
    per_symbol: bool = False
    single_record: bool = False


# ============================================================
# DATASETS (run order)
# ============================================================

DATASETS = (
    MassiveDataset(
        result_key="exchanges",
        dataset="exchanges",
        label="exchanges",
        fetch=lambda client, request: ingest_exchanges(
            client=client,
        ),
    ),
    MassiveDataset(
        result_key="tickers",
        dataset="ticker_reference",
        label="tickers",
        fetch=lambda client, request: ingest_tickers(
            client=client,
        ),
    ),
    MassiveDataset(
        result_key="aggregates",
        dataset="aggregates",
        label="aggregates",
        fetch=lambda client, request: ingest_aggregates(
            client=client,
            from_date=request.from_date,
            to_date=request.to_date,
        ),
    ),
    MassiveDataset(
        result_key="splits",
        dataset="splits",
        label="splits",
        fetch=lambda client, request: ingest_splits(
            client=client,
            symbol=request.symbol,
        ),
        per_symbol=True,
    ),
    MassiveDataset(
        result_key="dividends",
        dataset="dividends",
        label="dividends",
        fetch=lambda client, request: ingest_dividends(
            client=client,
            symbol=request.symbol,
        ),
        per_symbol=True,
    ),
    MassiveDataset(
        result_key="stock_overview",
        dataset="stock_overview",
        label="stock overview",
        fetch=lambda client, request: ingest_stock_overview(
            client=client,
            symbol=request.symbol,
        ),
        per_symbol=True,
        single_record=True,
    ),
)
