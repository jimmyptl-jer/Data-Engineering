"""
Contract tests between the ingestion Lambdas (writers) and the transform
job (reader).

The transform job reads Bronze data from
`stock/bronze/source=<source>/dataset=<dataset>/ingestion_date=<date>/run_id=<run_id>/`
(src/stock_pipeline/extract.py). Every object an ingestion Lambda writes must
land under that prefix, or the transform job will not find it. These tests
run each Lambda handler end to end against fake APIs and a fake S3 client.
"""

import re
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from conftest import FakeResponse, load_lambda
from src.stock_pipeline.extract import MassiveApiExtractor, StockDataExtractor

RUN_ID = "batch_20260930_140000"

BRONZE_KEY = re.compile(
    r"^stock/bronze/source=(?P<source>[^/]+)/dataset=(?P<dataset>[^/]+)/"
    r"ingestion_date=(?P<date>\d{4}-\d{2}-\d{2})/run_id=(?P<run_id>[^/]+)/(?P<file>[^/]+)\.json$"
)


def reader_prefix(extractor_cls, source: str, dataset: str, day: str, run_id: str) -> str:
    """Bronze prefix the transform job reads, without the s3a://bucket/ part."""
    extractor = SimpleNamespace(bucket_name="test-bucket")
    execution_time = datetime.strptime(day, "%Y-%m-%d")
    uri = extractor_cls._bronze_bucket_key(extractor, source, dataset, execution_time, run_id)
    return uri.removeprefix("s3a://test-bucket/")


def assert_readable(key: str, extractor_cls=MassiveApiExtractor) -> re.Match:
    match = BRONZE_KEY.match(key)
    assert match, f"not a Bronze key: {key}"
    prefix = reader_prefix(extractor_cls, match["source"], match["dataset"], match["date"], match["run_id"])
    assert key.startswith(prefix), f"{key} is outside the reader prefix {prefix}"
    return match


# ============================================================
# KEY BUILDER (each Lambda folder has its own bronze.py)
# ============================================================

@pytest.mark.parametrize("source", ["alpha_vantage", "finnhub", "massive", "twelvedata"])
@pytest.mark.parametrize("extractor_cls", [StockDataExtractor, MassiveApiExtractor])
def test_bronze_key_matches_both_readers(source, extractor_cls):
    execution_time = datetime(2026, 9, 30, 23, 59, tzinfo=timezone.utc)
    bronze_key = load_lambda(source).bronze.bronze_key

    key = bronze_key("massive", "stock_overview", execution_time, RUN_ID, file_name="IBM")

    assert key == (
        "stock/bronze/source=massive/dataset=stock_overview/"
        "ingestion_date=2026-09-30/run_id=batch_20260930_140000/IBM.json"
    )
    assert key.startswith(reader_prefix(extractor_cls, "massive", "stock_overview", "2026-09-30", RUN_ID))


# ============================================================
# ALPHA VANTAGE
# ============================================================

def alpha_vantage_api(url, params):
    if params["function"] == "TIME_SERIES_DAILY":
        return FakeResponse({
            "Meta Data": {"2. Symbol": params["symbol"]},
            "Time Series (Daily)": {"2026-09-30": {"4. close": "250.10"}},
        })
    return FakeResponse({"Symbol": params["symbol"], "Name": "International Business Machines"})


def test_alpha_vantage_writes_where_transform_reads(ingestion_lambda, fake_http):
    lam, bronze_s3 = ingestion_lambda("alpha_vantage")
    fake_http(alpha_vantage_api)

    response = lam.app.lambda_handler({"run_id": RUN_ID, "stock_symbols": ["ibm"]}, None)

    assert response["statusCode"] == 200
    written = {assert_readable(key, StockDataExtractor)["dataset"]: key for key in bronze_s3.keys}
    assert set(written) == {"daily_time_series", "company_overview"}
    for key in written.values():
        match = BRONZE_KEY.match(key)
        assert (match["source"], match["run_id"], match["file"]) == ("alphavantage", RUN_ID, "IBM")


# ============================================================
# MASSIVE
# ============================================================

MASSIVE_FILES = {
    "exchanges": "data",
    "ticker_reference": "data",
    "aggregates": "data",
    "splits": "IBM",
    "dividends": "IBM",
    "stock_overview": "IBM",
}


def test_massive_writes_every_dataset_where_transform_reads(ingestion_lambda, fake_massive):
    lam, bronze_s3 = ingestion_lambda("massive")

    response = lam.app.lambda_handler({"run_id": RUN_ID, "symbol": "IBM"}, None)

    assert response["statusCode"] == 200
    written = {assert_readable(key)["dataset"]: BRONZE_KEY.match(key) for key in bronze_s3.keys}
    assert set(written) == set(MASSIVE_FILES)
    for dataset, match in written.items():
        assert (match["source"], match["run_id"]) == ("massive", RUN_ID)
        assert match["file"] == MASSIVE_FILES[dataset], dataset


# ============================================================
# FINNHUB
# ============================================================

def test_finnhub_writes_where_transform_reads(ingestion_lambda, fake_http):
    lam, bronze_s3 = ingestion_lambda("finnhub")
    fake_http(lambda url, params: FakeResponse([{"symbol": "IBM", "mic": "XNYS"}]))

    response = lam.app.lambda_handler({"run_id": RUN_ID, "exchange": "US"}, None)

    assert response["statusCode"] == 200
    [key] = bronze_s3.keys
    match = assert_readable(key)
    assert (match["source"], match["dataset"], match["run_id"], match["file"]) == (
        "finnhub", "ticker_reference", RUN_ID, "US",
    )


# ============================================================
# TWELVE DATA
# ============================================================

def test_twelvedata_writes_where_transform_reads(ingestion_lambda, fake_http):
    lam, bronze_s3 = ingestion_lambda("twelvedata")
    calls = fake_http(lambda url, params: FakeResponse({
        "meta": {"symbol": params["symbol"], "interval": "1min"},
        "values": [{"datetime": "2026-09-30 15:59:00", "close": "250.10"}],
        "status": "ok",
    }))

    response = lam.app.lambda_handler({"run_id": RUN_ID, "stock_symbols": ["ibm"]}, None)

    assert response["statusCode"] == 200
    [call] = calls
    assert call["params"]["apikey"] == lam.keys.API_KEY
    assert (call["params"]["interval"], call["params"]["outputsize"]) == ("1min", 390)
    [key] = bronze_s3.keys
    match = assert_readable(key, StockDataExtractor)
    assert (match["source"], match["dataset"], match["run_id"], match["file"]) == (
        "twelvedata", "time_series_1min", RUN_ID, "IBM",
    )


def test_twelvedata_status_error_is_reported(ingestion_lambda, fake_http):
    lam, bronze_s3 = ingestion_lambda("twelvedata")
    fake_http(lambda url, params: FakeResponse({"code": 429, "message": "rate limit", "status": "error"}))

    response = lam.app.lambda_handler({"run_id": RUN_ID, "stock_symbols": ["IBM"]}, None)

    assert response["statusCode"] == 207
    assert bronze_s3.keys == []
