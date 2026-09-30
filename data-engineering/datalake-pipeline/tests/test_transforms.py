"""
Bronze → Silver transform tests, run on a local SparkSession.

Each test builds a small Bronze DataFrame shaped like the raw API payload
(using the same explicit schemas the extractor uses, or schema inference
like `MassiveApiExtractor`) and checks the Silver output.

Requires Java 17+ (for Spark). Skipped when no Java runtime is available.
"""

import json
from datetime import date, timedelta

import pytest

pyspark = pytest.importorskip("pyspark")

from src.stock_pipeline.extract import (  # noqa: E402
    stock_overview_schema,
    stock_schema_daily,
    stock_schema_weekly,
)
from src.stock_pipeline.transform import (  # noqa: E402
    silver_transform_daily_timeseries,
    silver_transform_overview,
    transform_finnhub_stock_tickers_dataset,
    transform_massive_exchanges_dataset,
    transform_massive_stock_overview,
    transform_weekly_timeseries,
)

pytestmark = pytest.mark.spark


def json_df(spark, records):
    """Bronze DataFrame with inferred schema, like MassiveApiExtractor."""
    rdd = spark.sparkContext.parallelize([json.dumps(r) for r in records])
    return spark.read.json(rdd)


def ohlcv(open_, high, low, close, volume):
    return {
        "1. open": str(open_),
        "2. high": str(high),
        "3. low": str(low),
        "4. close": str(close),
        "5. volume": str(volume),
    }


# ============================================================
# ALPHA VANTAGE
# ============================================================

def test_daily_time_series_keeps_valid_rows_and_drops_invalid(spark):
    today = date.today()
    days = [(today - timedelta(days=offset)).isoformat() for offset in range(3)]

    bronze = spark.createDataFrame(
        [{
            "Meta Data": {"2. Symbol": "ibm", "3. Last Refreshed": days[0]},
            "Time Series (Daily)": {
                days[0]: ohlcv(100, 110, 95, 105, 1000),
                days[1]: ohlcv(101, 111, 96, 106, 2000),
                # high < low → quarantined
                days[2]: ohlcv(100, 90, 95, 92, 3000),
            },
        }],
        schema=stock_schema_daily,
    )

    silver = silver_transform_daily_timeseries(
        spark=spark,
        daily_dataset="daily_time_series",
        data_df=bronze,
    )
    rows = silver.collect()

    assert len(rows) == 2
    assert {row["symbol"] for row in rows} == {"IBM"}
    assert {row["validation_status"] for row in rows} == {"VALID"}
    assert {str(row["day_date"]) for row in rows} == {days[0], days[1]}


def test_daily_time_series_applies_watermark(spark):
    today = date.today()
    old, new = (today - timedelta(days=5)).isoformat(), today.isoformat()

    bronze = spark.createDataFrame(
        [{
            "Meta Data": {"2. Symbol": "IBM", "3. Last Refreshed": new},
            "Time Series (Daily)": {
                old: ohlcv(100, 110, 95, 105, 1000),
                new: ohlcv(101, 111, 96, 106, 2000),
            },
        }],
        schema=stock_schema_daily,
    )

    silver = silver_transform_daily_timeseries(
        spark=spark,
        daily_dataset="daily_time_series",
        data_df=bronze,
        watermark_value=(today - timedelta(days=1)).isoformat(),
    )

    assert [str(row["day_date"]) for row in silver.collect()] == [new]


def test_weekly_time_series_keeps_valid_rows(spark):
    bronze = spark.createDataFrame(
        [{
            "Meta Data": {"2. Symbol": "ibm", "3. Last Refreshed": "2026-09-25"},
            "Weekly Time Series": {
                "2026-09-25": ohlcv(100, 110, 95, 105, 1000),
                "2026-09-18": ohlcv(0, 110, 95, 105, 1000),  # open <= 0 → invalid
            },
        }],
        schema=stock_schema_weekly,
    )

    rows = transform_weekly_timeseries(spark=spark, data_df=bronze).collect()

    assert len(rows) == 1
    assert rows[0]["symbol"] == "IBM"


def test_company_overview_is_typed_and_standardised(spark):
    record = {field.name: None for field in stock_overview_schema.fields}
    record.update({
        "Symbol": " ibm ",
        "AssetType": "Common Stock",
        "Name": "International Business Machines",
        "Exchange": "nyse",
        "Currency": "usd",
        "Country": "USA",
        "Sector": "TECHNOLOGY",
        "Industry": "Computer & Office Equipment",
        "FiscalYearEnd": "December",
        "LatestQuarter": "2026-06-30",
        "MarketCapitalization": "250000000000",
        "PERatio": "22.5",
        "EPS": "9.1",
        "DividendYield": "None",
    })
    bronze = spark.createDataFrame([record], schema=stock_overview_schema)

    rows = silver_transform_overview(spark=spark, data_df=bronze).collect()

    assert len(rows) == 1
    row = rows[0].asDict()
    assert row["symbol"] == "IBM"
    assert row["exchange"] == "NYSE"
    assert row["currency"] == "USD"
    assert row["sector"] == "technology"


# ============================================================
# MASSIVE
# ============================================================

def test_massive_exchanges_flags_invalid_rows(spark):
    bronze = json_df(spark, [
        {"asset_class": "stocks", "id": 1, "locale": "US", "name": "NYSE",
         "operating_mic": "xnys", "participant_id": "N", "type": "exchange"},
        {"asset_class": "stocks", "id": 2, "locale": "US", "name": None,
         "operating_mic": "xnas", "participant_id": "Q", "type": "exchange"},
    ])

    rows = {
        row["id"]: row
        for row in transform_massive_exchanges_dataset(spark=spark, data_df=bronze).collect()
    }

    assert rows[1]["validation_status"] == "VALID"
    assert rows[1]["mic"] == "XNYS"
    assert rows[1]["type"] == "EXCHANGE"
    assert rows[1]["locale"] == "us"
    assert rows[2]["validation_status"] == "INVALID"
    assert "MISSING NAME" in rows[2]["validation_reason"]


def test_massive_stock_overview_renames_and_standardises(spark):
    bronze = json_df(spark, [{
        "locale": "US",
        "market": "STOCKS",
        "ticker": " ibm ",
        "list_date": "1915-11-11",
        "market_cap": 250000000000,
        "name": "International Business Machines",
        "primary_exchange": "xnys",
        "share_class_figi": "BBG001S5S399",
        "composite_figi": "BBG000BLNNH6",
        "cik": "0000051143",
        "share_class_shares_outstanding": 930000000,
        "extra_field": "dropped",
    }])

    silver = transform_massive_stock_overview(spark=spark, data_df=bronze)
    [row] = silver.collect()

    assert "extra_field" not in silver.columns
    assert row["symbol"] == "IBM"
    assert row["company_name"] == "International Business Machines"
    assert row["primary_exchange"] == "XNYS"
    assert row["locale"] == "us"
    assert row["asset_type"] == "stocks"
    assert row["shares_outstanding"] == 930000000


# ============================================================
# FINNHUB
# ============================================================

def test_finnhub_stock_tickers_flags_invalid_rows(spark):
    def ticker(symbol, currency="USD"):
        return {
            "currency": currency, "description": "INTL BUSINESS MACHINES CORP",
            "displaySymbol": symbol, "figi": "BBG000BLNNH6",
            "figiComposite": "BBG000BLNNH6", "isin": None, "mic": "XNYS",
            "shareClassFIGI": "BBG001S5S399", "symbol": symbol,
            "symbol2": "", "type": "Common Stock",
        }

    bronze = json_df(spark, [ticker("ibm"), ticker("AAPL", currency=None)])

    rows = transform_finnhub_stock_tickers_dataset(spark=spark, data_df=bronze).collect()
    status = {row["symbol"]: row["validation_status"] for row in rows}

    assert status["IBM"] == "VALID"
    assert status["AAPL"] == "INVALID"
