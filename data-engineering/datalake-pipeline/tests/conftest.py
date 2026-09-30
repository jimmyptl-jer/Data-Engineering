"""
Shared test setup.

- Puts the project root on sys.path (for `src.stock_pipeline`).
- `load_lambda()` imports one ingestion Lambda folder the way AWS Lambda does.
- Sets the environment variables the Lambdas read at import time.
- Fakes for S3 and the market-data APIs, a CloudFormation template loader,
  and a local SparkSession for the transform tests.
"""

import importlib
import json
import os
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INGESTION_ROOT = PROJECT_ROOT / "ingestion"

# The Lambdas read these at import time.
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("S3_BUCKET_NAME", "test-bucket")
os.environ.setdefault("MASSIVE_API_KEY", "test-massive-key")
os.environ.setdefault("FINNHUB_API_KEY", "test-finnhub-key")

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ============================================================
# INGESTION LAMBDA LOADER
# ============================================================

_LAMBDAS = {}


def _is_under(module, folder: Path) -> bool:
    file = getattr(module, "__file__", None)
    return bool(file) and Path(file).resolve().is_relative_to(folder)


def load_lambda(source: str) -> SimpleNamespace:
    """
    Import ingestion/<source>/app.py with the folder as the import root, as
    AWS Lambda does, and return the Lambda's modules as attributes
    (app, config, client, bronze, ...; "datasets.tickers" -> datasets_tickers).

    Every Lambda folder uses the same module names, so each one is imported
    in isolation and then removed from sys.modules.
    """
    if source in _LAMBDAS:
        return _LAMBDAS[source]

    folder = (INGESTION_ROOT / source).resolve()
    ingestion_root = INGESTION_ROOT.resolve()

    for name in [n for n, m in sys.modules.items() if _is_under(m, ingestion_root)]:
        del sys.modules[name]

    sys.path.insert(0, str(folder))
    try:
        importlib.import_module("app")
    finally:
        sys.path.remove(str(folder))

    modules = {n: m for n, m in sys.modules.items() if _is_under(m, folder)}
    for name in modules:
        del sys.modules[name]

    lam = SimpleNamespace(**{n.replace(".", "_"): m for n, m in modules.items()})
    _LAMBDAS[source] = lam
    return lam


# ============================================================
# S3 FAKE
# ============================================================

class FakeS3Client:
    """Records put_object calls instead of writing to S3."""

    def __init__(self):
        self.objects = {}

    @property
    def keys(self):
        return list(self.objects)

    def put_object(self, Bucket, Key, Body, ContentType):
        assert ContentType == "application/json"
        self.objects[Key] = json.loads(Body.decode("utf-8"))


@pytest.fixture
def ingestion_lambda(monkeypatch):
    """
    Load an ingestion Lambda with its Bronze S3 client faked:

        lam, s3 = ingestion_lambda("massive")
        lam.app.lambda_handler(event, None)
        s3.keys
    """
    def load(source):
        lam = load_lambda(source)
        fake = FakeS3Client()
        monkeypatch.setattr(lam.bronze, "s3_client", fake)
        return lam, fake

    return load


# ============================================================
# HTTP FAKE (requests.get)
# ============================================================

class FakeResponse:
    """Minimal stand-in for requests.Response."""

    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.content = json.dumps(payload).encode("utf-8")

    def raise_for_status(self):
        import requests

        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


@pytest.fixture
def fake_http(monkeypatch):
    """
    Replace requests.get. Call the fixture with a function
    (url, params) -> FakeResponse | Exception; returns the list of calls.
    """
    import requests

    calls = []

    def install(route):
        def fake_get(url, params=None, timeout=None):
            calls.append({"url": url, "params": dict(params or {}), "timeout": timeout})
            outcome = route(url, params or {})
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        monkeypatch.setattr(requests, "get", fake_get)
        return calls

    return install


# ============================================================
# MASSIVE SDK FAKE
# ============================================================

class FakeMassiveClient:
    """
    Stand-in for massive.RESTClient covering the endpoints the Massive
    Lambda calls. `fail` / `empty` name datasets (by result key) that
    should raise or return nothing.
    """

    def __init__(self):
        self.fail = set()
        self.empty = set()

    def _answer(self, name, records):
        if name in self.fail:
            raise RuntimeError(f"{name} endpoint failed")
        return [] if name in self.empty else records

    def get_exchanges(self, asset_class, locale):
        return self._answer("exchanges", [{"id": 1, "name": "NYSE", "operating_mic": "XNYS"}])

    def list_tickers(self, market, active, order, limit, sort):
        return iter(self._answer("tickers", [{"ticker": "IBM"}, {"ticker": "AAPL"}]))

    def get_grouped_daily_aggs(self, date, adjusted):
        return self._answer("aggregates", [{"T": "IBM", "c": 250.1, "date": date}])

    def list_stocks_splits(self, ticker, limit, sort):
        return iter(self._answer("splits", [{"ticker": ticker, "split_from": 1, "split_to": 2}]))

    def list_stocks_dividends(self, ticker, limit, sort):
        return iter(self._answer("dividends", [{"ticker": ticker, "cash_amount": 1.68}]))

    def get_ticker_details(self, symbol):
        records = self._answer("stock_overview", [{"ticker": symbol, "name": "International Business Machines"}])
        return records[0] if records else None


@pytest.fixture
def fake_massive(monkeypatch):
    """Install a FakeMassiveClient as the Massive Lambda's SDK client."""
    fake = FakeMassiveClient()
    monkeypatch.setattr(load_lambda("massive").client, "massive_client", fake)
    return fake


# ============================================================
# CLOUDFORMATION TEMPLATES
# ============================================================

class CloudFormationLoader(yaml.SafeLoader):
    """SafeLoader that understands CloudFormation short-form tags (!Ref, !Sub, ...)."""


def _construct_tag(loader, tag_suffix, node):
    name = "Ref" if tag_suffix == "Ref" else f"Fn::{tag_suffix}"
    if isinstance(node, yaml.ScalarNode):
        value = loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node, deep=True)
    else:
        value = loader.construct_mapping(node, deep=True)
    return {name: value}


CloudFormationLoader.add_multi_constructor("!", _construct_tag)


def load_template(path):
    with open(path, encoding="utf-8") as handle:
        return yaml.load(handle, Loader=CloudFormationLoader)


# ============================================================
# SPARK
# ============================================================

def _java_available() -> bool:
    java_home = os.environ.get("JAVA_HOME")
    if java_home and (Path(java_home) / "bin").exists():
        return True
    return shutil.which("java") is not None


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession for transform tests (needs Java 17+)."""
    if not _java_available():
        pytest.skip("Java runtime not found; set JAVA_HOME to run Spark tests.")

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("stock-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
