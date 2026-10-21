# Running the Pipeline Locally

How to set up and run the stock data pipeline (and the other pipelines in
this repo) on your own machine. Everything below was verified against the
repo as of 2026-10-22.

## 1. Prerequisites

| Requirement | Notes |
| --- | --- |
| Python 3.12 | matches the pinned `pyspark==4.0.4` in `requirements.txt` |
| Java 11 or 17 | required by local Spark (`java -version` to check) |
| Internet access | first run downloads `org.apache.hadoop:hadoop-aws:3.4.1` via Ivy (cached afterwards in `~/.ivy2`) |
| AWS account | an S3 bucket (default: `graywolf--data--lake`) that your IAM user can read/write |
| API keys | Alpha Vantage (free), Massive/Polygon, Finnhub, Twelve Data, YouTube — only the ones you actually enable are needed |

## 2. Clone and install

```bash
git clone https://github.com/jimmyptl-jer/Data-Engineering.git
cd Data-Engineering/datalake-pipeline

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

`requirements.txt` pins everything, including the SDKs the ingestion modules
import: `massive==2.8.0`, `finnhub-python==2.4.29`, `boto3`, `requests`,
`python-dotenv`.

Sanity check the install (3 tests must pass):

```bash
pytest tests/
```

## 3. Configure credentials — `.env`

`python-dotenv` loads `.env` from your **current working directory**, so put
one `.env` in `datalake-pipeline/` (for the stock pipeline) and, if you also
run the YouTube pipeline, one in the repo root `Data-Engineering/`.

```dotenv
# --- Stock pipeline (datalake-pipeline/.env) ---
ALPHA_VANTAGE_API_KEY=your_key
# Optional key-rotation pool, up to 16 total keys:
# ALPHA_VANTAGE_API_KEY_1=...
# ALPHA_VANTAGE_API_KEY_15=...
MASSIVE_API_KEY=your_key
FINNHUB_API_KEY=your_key
TWELVEDATA_API_KEY=your_key          # only for the Twelve Data Lambda
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
S3_BUCKET_NAME=graywolf--data--lake  # optional for the stock pipeline (this is the default)

# --- YouTube pipeline (Data-Engineering/.env) ---
# YOUTUBE_API_KEY=your_key
# S3_BUCKET_NAME=graywolf--data--lake   # NO default here — must be set
```

Rules:

* **Never commit `.env`** — keep it in `.gitignore`.
* Only set the keys for the sources you run; each ingestion module raises a
  clear `ValueError` (e.g. `MASSIVE_API_KEY environment variable is not set`)
  if you enable its step without a key.
* `ALPHA_VANTAGE_API_KEY` is read **at import time** — if you change it,
  restart the pipeline (don't just rerun a cell in an already-running REPL).

## 4. Run the stock pipeline

From `datalake-pipeline/`:

```bash
python -m src.stock_pipeline.app
```

This simulates a Lambda invocation (`lambda_handler({}, {})`) and processes
the default symbols — currently `["IBM"]` (see `app.py`). To process other
symbols locally, change the default list in `app.py` or call the handler from
a small script with an event, e.g.:

```python
from src.stock_pipeline.app import lambda_handler
lambda_handler({"stock_symbols": ["AAPL", "MSFT"]}, {})
```

### What runs (steps active in `pipeline.run()`)

| Step | Flow | Status |
| --- | --- | --- |
| 1 | Alpha Vantage → Bronze (TIME_SERIES_DAILY + OVERVIEW per symbol) | active |
| 2 | Massive → Bronze (exchanges, aggregates, stock overview, dividends) | active |
| 3 | Finnhub → Bronze | commented out (toggle in `pipeline.py`) |
| 4 | Alpha Vantage overview → Silver | active |
| 5 | Massive stock overview → Silver | active |
| 6 | Finnhub tickers → Silver | commented out (toggle) |
| 7 | Silver → Gold (`company_dataset`) | active |
| 8 | Summary logging | active |

Daily time-series processing and the older `_build_gold_layer()` are also
commented-out toggles in `pipeline.run()` — uncomment them in
`src/stock_pipeline/pipeline.py` to enable.

### Hardcoded values to know about

* Massive aggregates window: `from_date="2026-07-20"`, `to_date="2026-08-10"`
  in `_ingest_from_massive_api()` — adjust per run.
* Default symbols `["IBM"]` in `app.py` and the Massive Lambda config.
* Silver/Gold base paths and the watermark path point at the
  `graywolf--data--lake` bucket (see `config.py` and `watermark/config.py`).

### Where the data lands (S3)

```text
s3://graywolf--data--lake/
├── stock/bronze/source={datasource}/dataset={dataset}/ingestion_date=YYYY-MM-DD/run_id={batch_id}/...   # raw JSON
├── stock/silver/datasource={datasource}/dataset={dataset}/year=YYYY/month=MM/format={csv|parquet}/            # cleaned
├── stock/gold/datasource=stock/dataset=company_dataset/year=YYYY/month=MM/format=csv/                        # joined
└── watermark/                                                                                               # incremental state
```

Verify from the CLI: `aws s3 ls s3://graywolf--data--lake/stock/ --recursive | head`.

## 5. Run the YouTube pipeline

From the **repo root** (`Data-Engineering/`):

```bash
python -m youtube_pipeline.app
```

Requires `YOUTUBE_API_KEY`, `S3_BUCKET_NAME` (no default) and the AWS keys.
Flow: YouTube search ("AWS Data Engineering", 100 newest) → raw JSON to
Bronze → re-read Bronze → transform JSON + CSV → Silver CSV under
`s3a://graywolf--data--lake/silver/youtube/source=youtube_api/`.

## 6. Twelve Data ingestion (SAM Lambda)

`src/stock_pipeline/ingestion/twelvedata/` is a standalone AWS SAM Lambda
(its own `template.yaml` + `requirements.txt`). Local options:

```bash
cd src/stock_pipeline/ingestion/twelvedata
sam build
sam local invoke                     # empty event → defaults (symbols=["IBM"], one day of 1-min bars)
sam local invoke -e event.json       # custom event
```

Event fields (all optional): `stock_symbols`, `full_load` (true → 5000 bars
instead of one trading day), `datasource`, `run_id`. Handler: `app.lambda_handler`.
Needs `TWELVEDATA_API_KEY` in the environment.

## 7. Deploying the Lambdas (instead of local)

The Alpha Vantage and Finnhub ingestion wrappers also ship SAM templates
(`ingestion/alpha_vantage/template.yaml`, `ingestion/finnhub/template.yaml`).
Deploy with:

```bash
sam build && sam deploy --guided
```

The stock pipeline Lambda runs on a cron schedule (weekdays ~14:00 UTC by
default — see the template).

## 8. Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| `ValueError: No valid Alpha Vantage API keys found` | `ALPHA_VANTAGE_API_KEY` missing from `.env`, or `.env` is not in the directory you launched from; restart the process after setting it |
| `ModuleNotFoundError: No module named 'finnhub'` / `'massive'` | run `pip install -r requirements.txt` (pins `finnhub-python==2.4.29`, `massive==2.8.0`) |
| Ivy/Maven download error on startup | first Spark run needs internet to fetch `hadoop-aws`; check proxy, then retry |
| `FileNotFoundError: Path does not exist: s3a://...silver/...` | Silver extractor pointed at a `year=YYYY/month=MM` partition that hasn't been written yet — run the Silver steps (or check the partition year/month matches) |
| S3 403 / AccessDenied | AWS keys wrong, or IAM user lacks read/write on the bucket |
| First run logs `mode=FULL_LOAD` | normal — no watermark exists yet; subsequent runs are incremental |
| `SyntaxError: invalid non-printable character U+FEFF` | a file was saved as "UTF-8 with BOM" — re-save as plain UTF-8 (all repo files were fixed for this on 2026-10-22) |

## 9. Quick smoke test (no AWS needed)

The transformation layer can be exercised without any credentials — it is a
pure Spark function. Useful after refactors:

```python
from src.stock_pipeline.extract import stock_schema_daily
from src.stock_pipeline.transform.daily import silver_transform_daily_timeseries
from pyspark.sql import Row

bronze_df = spark.createDataFrame(
    [{"Meta Data": {"2. Symbol": "IBM", "3. Last Refreshed": "2026-10-21"},
      "Time Series (Daily)": {"2026-10-20": {"1. open": "101.0", "2. high": "103.0",
                                            "3. low": "100.5", "4. close": "102.5", "5. volume": "1200"}}}],
    schema=stock_schema_daily,
)
silver_df = silver_transform_daily_timeseries(spark, "daily_time_series", bronze_df)
silver_df.count()   # → 1
```

*Verified 2026-10-22: all modules import, pytest 3/3, and the daily transform
runs end-to-end on live Spark.*