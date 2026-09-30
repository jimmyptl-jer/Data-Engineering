# Stock Data Lake Pipeline

> A Bronze → Silver → Gold (medallion) data lake for stock market data. Three AWS Lambda functions pull raw JSON from **Alpha Vantage**, **Massive (Polygon.io)** and **Finnhub** into S3. A **PySpark** job builds the Silver and Gold layers, and an **AWS Step Functions** workflow runs it all on a schedule.

Part of the **90-Day Data Engineering Roadmap**.

---

## Table of Contents

- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Technology Stack](#technology-stack)
- [Prerequisites](#prerequisites)
- [Quick Start (local)](#quick-start-local)
- [Deployment](#deployment)
- [Running the Pipeline](#running-the-pipeline)
- [Ingestion Lambdas](#ingestion-lambdas)
- [Transform Job](#transform-job)
- [Data Lake Layout](#data-lake-layout)
- [Incremental Processing & Watermarks](#incremental-processing--watermarks)
- [Monitoring & Alerting](#monitoring--alerting)
- [Security](#security)
- [Storage Lifecycle](#storage-lifecycle)
- [Testing](#testing)
- [Architecture Layers](#architecture-layers)
- [Known Gaps](#known-gaps)
- [Roadmap](#roadmap)

---

## Architecture

```text
                 EventBridge schedule: cron(0 14 ? * MON-FRI *)
                                    │
                                    ▼
 Step Functions workflow: stock-data-pipeline-<EnvironmentName>
 ┌───────────────────────────────────────────────────────────────────────┐
 │ CreateRunId            run_id = batch_<execution name>                │
 │      │                                                                │
 │      ▼                                                                │
 │ IngestToBronze (parallel)                                             │
 │   ├── ingest-alpha-vantage-<env>   Alpha Vantage REST ──┐             │
 │   ├── ingest-massive               Massive SDK        ──┼──▶ BRONZE   │
 │   └── ingest-finnhub               Finnhub REST       ──┘   raw JSON  │
 │      │   (a Lambda that returns statusCode 500 fails the workflow)    │
 │      ▼                                                                │
 │ TransformBronzeToGold                                                 │
 │   stock-data-pipeline-<env> (PySpark)   BRONZE ──▶ SILVER ──▶ GOLD    │
 └───────────────────────────────────────────────────────────────────────┘
```

Every Lambda in one execution receives the same `run_id`. The ingestion Lambdas write to the `run_id=<run_id>/` folder in Bronze, and the transform job reads only that folder, so one workflow execution is one batch.

---

## Project Structure

Ingestion and transformation are deployed separately:

- **`ingestion/<source>/`**: one lightweight Lambda per API. Each folder is a self-contained SAM app with its own `template.yaml`, `samconfig.toml`, `requirements.txt` and code. The folders share no files.
- **`src/stock_pipeline/`**: the PySpark job that reads Bronze and builds Silver and Gold. It is deployed by the root `template.yaml`, together with the bucket, the workflow and the alarms.

```text
datalake-pipeline/
│
├── ingestion/                        # API → S3 Bronze; each folder is a self-contained SAM app
│   ├── alpha_vantage/
│   │   ├── template.yaml  samconfig.toml  requirements.txt
│   │   ├── app.py                    # Lambda entry point (Handler: app.lambda_handler)
│   │   ├── ingest.py                 # Symbol × endpoint loop → Bronze
│   │   ├── client.py                 # HTTP call + response validation
│   │   ├── keys.py                   # API key pool
│   │   ├── config.py                 # Bucket, base URL, endpoint → dataset mapping
│   │   └── bronze.py                 # Bronze S3 key layout + JSON writer
│   ├── finnhub/
│   │   ├── template.yaml  samconfig.toml  requirements.txt
│   │   ├── app.py                    # Lambda entry point
│   │   ├── client.py                 # GET /stock/symbol
│   │   ├── config.py
│   │   └── bronze.py
│   └── massive/
│       ├── template.yaml  samconfig.toml  requirements.txt
│       ├── app.py                    # Lambda entry point: runs every dataset in DATASETS
│       ├── datasets/                 # __init__.py = DATASETS registry; one module per endpoint
│       │   ├── exchanges.py  tickers.py  aggregates.py
│       │   └── splits.py  dividends.py  stock_overview.py
│       ├── client.py                 # Massive SDK client
│       ├── serialization.py          # SDK model → dict
│       ├── config.py
│       └── bronze.py
│
├── src/
│   ├── stock_schema.json  youtube_schema.json
│   └── stock_pipeline/               # Bronze → Silver → Gold (PySpark)
│       ├── app.py                    # Lambda handler + local __main__
│       ├── pipeline.py               # StockPipeline orchestrator, Spark session, Silver/Gold writers
│       ├── config.py                 # Endpoints, Silver/Gold paths, watermark strategies
│       ├── extract.py                # Explicit schemas + Bronze/Silver readers
│       ├── transform/                # One module per dataset (see Transform Job)
│       └── watermark/                # WatermarkManager + watermark paths
│
├── tests/                            # pytest suite (see Testing)
├── notes/                            # Design notes per dataset / layer
├── .env.example                      # Local environment template (transform job)
├── pytest.ini
├── requirements.txt                  # Transform job runtime dependencies
├── requirements-dev.txt              # Transform + ingestion + test/lint dependencies
├── samconfig.toml                    # Root stack deploy settings
└── template.yaml                     # Root stack: bucket, transform Lambda, workflow, SNS, alarms
```

---

## Technology Stack

| Component | Technology |
|-----------|------------|
| Processing engine | PySpark 4.0 |
| Storage | Amazon S3: Bronze as JSON, Silver and Gold as CSV + Parquet |
| Data sources | Alpha Vantage REST, Massive (Polygon.io) SDK, Finnhub REST |
| Compute | AWS Lambda (`python3.14` runtime) |
| Orchestration | AWS Step Functions + Amazon EventBridge schedule |
| Infrastructure as code | AWS SAM: one stack per ingestion folder + one root stack |
| Alerting | CloudWatch alarms → SNS |
| Clients | `requests`, `massive`, `boto3` |
| Testing & linting | pytest, PyYAML, flake8, black |

---

## Prerequisites

- **Python 3.10+** locally (the Lambdas run on `python3.14`)
- **Java 17+** for PySpark (without it, the Spark tests are skipped)
- **AWS SAM CLI** and AWS credentials, to deploy
- **API keys** for Massive and Finnhub, passed at deploy time. The Alpha Vantage keys are already in `ingestion/alpha_vantage/keys.py`.

---

## Quick Start (local)

```bash
cd data-engineering/datalake-pipeline
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt

python -m pytest                     # full suite; Spark tests are skipped without Java
python -m pytest -m "not spark"      # fast tests only
```

`requirements.txt` holds only the transform job's dependencies. Each ingestion folder has its own `requirements.txt`, and `requirements-dev.txt` installs both sets plus the test tools.

You can also run one batch by hand against the real bucket. This needs AWS credentials with access to it.

```bash
# 1. Ingest one source into Bronze (run from the Lambda's folder; it is the import root)
cd ingestion/alpha_vantage
S3_BUCKET_NAME=graywolf--data--lake \
  python -c "import app; print(app.lambda_handler({'run_id': 'batch_local_1', 'stock_symbols': ['IBM']}, None))"

# 2. Transform that run (from datalake-pipeline/; AWS credentials and S3_BUCKET_NAME in .env)
cd ../..
python -c "from src.stock_pipeline.app import lambda_handler; lambda_handler({'run_id': 'batch_local_1'}, None)"
```

The transform loads `.env` with python-dotenv. Copy `.env.example` to `.env` and set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` and `S3_BUCKET_NAME`. The other entries in that file are not used by this pipeline. `python -m src.stock_pipeline.app` also runs the transform, but with no `run_id` it looks for a batch named after the current time and finds nothing.

---

## Deployment

The pipeline is four SAM stacks. Deploy the root stack first, because it creates the S3 bucket that the ingestion Lambdas write to.

| Folder | Stack | Deploys |
|--------|-------|---------|
| `datalake-pipeline/` | `stock-pipeline` | Data lake bucket, transform Lambda `stock-data-pipeline-<env>`, Step Functions workflow, SNS topic, alarms |
| `ingestion/alpha_vantage/` | `ingest-alpha-vantage` | Lambda `ingest-alpha-vantage-<env>` |
| `ingestion/massive/` | `ingest-massive-exchanges` | Lambda `ingest-massive` |
| `ingestion/finnhub/` | `finnhub-ingestion` | Lambda `ingest-finnhub` |

```bash
# 1. Root stack (from datalake-pipeline/)
sam build && sam deploy

# 2. Ingestion stacks
cd ingestion/alpha_vantage && sam build && sam deploy
cd ../massive && sam build && sam deploy --parameter-overrides EnableSchedule=false MassiveApiKey=<key>
cd ../finnhub && sam build && sam deploy --parameter-overrides EnableSchedule=false FinnhubApiKey=<key>
```

- `MassiveApiKey` and `FinnhubApiKey` are `NoEcho` parameters with no default, so you pass them at deploy time and they never go into `samconfig.toml`. Passing `--parameter-overrides` on the command line replaces the list in `samconfig.toml`, so keep `EnableSchedule=false` in it.
- Each ingestion stack has its own EventBridge schedule, but it is switched off (`EnableSchedule="false"`), so the workflow is the only trigger. A Lambda run by its own schedule would write to a `run_id` that the transform never reads. `tests/test_workflow.py` fails if a schedule is switched back on.

Root stack parameters:

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `EnvironmentName` | `Development` | Suffix for resource names |
| `S3BucketName` | `graywolf--data--lake` | Data lake bucket (created by this stack) |
| `ScheduleExpression`, `EnableSchedule` | `cron(0 14 ? * MON-FRI *)`, `true` | When the workflow runs (14:00 UTC, weekdays) |
| `StockSymbol` | `IBM` | Symbol sent to the Alpha Vantage and Massive Lambdas |
| `FinnhubExchange` | `US` | Exchange sent to the Finnhub Lambda |
| `AlphaVantageIngestFunctionName`, `MassiveIngestFunctionName`, `FinnhubIngestFunctionName` | `ingest-alpha-vantage-Development`, `ingest-massive`, `ingest-finnhub` | Lambdas the workflow invokes. If you change `EnvironmentName`, update the Alpha Vantage one. |

---

## Running the Pipeline

The schedule starts the workflow at 14:00 UTC on weekdays. To start a run by hand:

```bash
aws stepfunctions start-execution \
  --state-machine-arn <StateMachineArn output of the stock-pipeline stack> \
  --name manual-20260930-1
# every Lambda in this execution gets run_id = batch_manual-20260930-1
```

1. **CreateRunId** sets `run_id = batch_<execution name>`.
2. **IngestToBronze** runs the three ingestion Lambdas in parallel with that `run_id`.
   - Lambda service errors are retried up to 3 times, 5 s apart with 2× backoff.
   - A Lambda that returns `statusCode` 500 fails its branch, and that fails the workflow.
   - Alpha Vantage's `207` means some calls failed; it does not stop the run.
3. **TransformBronzeToGold** runs the transform Lambda with the same `run_id`.

To run one ingestion Lambda on its own, give it a `run_id` and pass the same `run_id` to the transform later. Without one, each Lambda invents its own, and the invented IDs don't match.

```bash
aws lambda invoke --function-name ingest-finnhub \
  --cli-binary-format raw-in-base64-out \
  --payload '{"run_id": "batch_manual_1", "exchange": "US"}' out.json
```

---

## Ingestion Lambdas

Every folder under `ingestion/` has the same layout:

- `app.py`: the entry point (`Handler: app.lambda_handler`).
- `client.py`: the API calls.
- `config.py`: the settings.
- `bronze.py`: writes to Bronze.

The folder is the Lambda's import root, so imports are flat (`import config`, `from bronze import write_bronze_json`). The docstring at the top of each `app.py` lists every file in that folder.

| Lambda | Event keys | Bronze datasets (file name) | `statusCode` |
|--------|------------|-----------------------------|--------------|
| [`alpha_vantage/`](ingestion/alpha_vantage/app.py) | `run_id`, `stock_symbols` (default `["IBM"]`), `full_load`, `datasource` | `daily_time_series`, `company_overview` (`<SYMBOL>.json`) | 200 all succeeded · 207 some calls failed · 500 invocation failed |
| [`massive/`](ingestion/massive/app.py) | `run_id`, `symbol` (default `AAPL`), `from_date` (default today), `to_date` | `exchanges`, `ticker_reference`, `aggregates` (`data.json`); `splits`, `dividends`, `stock_overview` (`<SYMBOL>.json`) | 200 · 500 with `PARTIAL_FAILURE` if any dataset failed |
| [`finnhub/`](ingestion/finnhub/app.py) | `run_id`, `exchange` (default `US`) | `ticker_reference` (`<EXCHANGE>.json`) | 200 · 500 |

- **Alpha Vantage keys:** `keys.py` holds a pool of keys and picks one at random for each request.
- **Adding a Massive dataset:** add a module under `massive/datasets/` and one entry to `DATASETS` in [`datasets/__init__.py`](ingestion/massive/datasets/__init__.py). The Lambda runs every entry in order and reports each one separately in `results`.
- **The three `bronze.py` files:** each folder has its own copy, because the folders share nothing. If you change the Bronze path format, change all three. `tests/test_bronze_layout.py` checks every copy against the path the transform reads.

---

## Transform Job

The code is in `src/stock_pipeline/`. The root stack deploys it as the Lambda `stock-data-pipeline-<env>` (`Handler: src.stock_pipeline.app.lambda_handler`). It takes the event `{"run_id": "..."}` and reads Bronze `ingestion_date=<today, UTC>/run_id=<run_id>/`.

| Module | Role |
|--------|------|
| [`app.py`](src/stock_pipeline/app.py) | Lambda handler; also runs locally via `__main__` |
| [`pipeline.py`](src/stock_pipeline/pipeline.py) | `StockPipeline`: Spark session, one `_process_*` method per dataset, Silver/Gold writers, `run()` |
| [`extract.py`](src/stock_pipeline/extract.py) | `StockDataExtractor` (Alpha Vantage Bronze + Silver readers) and `MassiveApiExtractor.extract_from_bronze_layer()` (Massive and Finnhub Bronze) |
| [`config.py`](src/stock_pipeline/config.py) | Alpha Vantage endpoint → dataset mapping, watermark strategies, Silver/Gold base paths |
| [`transform/daily.py`](src/stock_pipeline/transform/daily.py) | Daily time series: watermark filter, data quality, 30-day rolling average, 52-week and all-time high/low, lag features |
| [`transform/overview.py`](src/stock_pipeline/transform/overview.py) | Company overview: business-column selection, snake_case names, fake-null normalisation, defaults, type casting |
| [`transform/stock_overview_massive.py`](src/stock_pipeline/transform/stock_overview_massive.py) | Massive stock overview |
| [`transform/exchanges.py`](src/stock_pipeline/transform/exchanges.py) | Massive exchanges: code standardisation, validation tagging |
| [`transform/stock_tickers.py`](src/stock_pipeline/transform/stock_tickers.py) | Finnhub ticker reference |
| [`transform/weekly.py`](src/stock_pipeline/transform/weekly.py) | Weekly time series. Not used yet, because nothing ingests weekly data. |
| [`watermark/manager.py`](src/stock_pipeline/watermark/manager.py) | `WatermarkManager`: `watermark_exists()`, `read_watermark()`, `write_watermark()` |

### What `StockPipeline.run()` does today

| Step | Status |
|------|--------|
| Alpha Vantage `company_overview` → Silver (CSV + Parquet), then write its watermark | Runs |
| Massive `stock_overview` → Silver (CSV + Parquet) | Runs |
| Gold `company_dataset`: Massive stock overview LEFT JOIN Alpha Vantage company overview on `symbol` (CSV) | Runs |
| Alpha Vantage `daily_time_series` → Silver | Commented out |
| Massive `exchanges`, `aggregates`, `dividends` → Silver | Commented out |
| Finnhub `ticker_reference` → Silver | Commented out |
| `_build_gold_layer()` (join of daily and overview) | Commented out |

The code for the commented-out steps is still in `pipeline.py`; uncomment it in `run()` to turn a step back on. The Massive `splits` and `ticker_reference` datasets reach Bronze but have no Silver step yet.

### Data quality

The daily, weekly, exchanges and ticker transforms tag each row with `validation_status` (`VALID` / `INVALID`) and a `validation_reason`. They count and log the invalid rows, then write only the `VALID` rows to Silver. For the daily dataset, a row is invalid when:

- a required field is null (`symbol`, `day_date`, `last_refreshed`, `open`, `high`, `low`, `close`, `volume`);
- a price is ≤ 0, or `volume` is < 0;
- `high` is lower than `low`.

Before those checks, text placeholders (`""`, `"n/a"`, `"na"`, `"null"`, `"none"`, `"-"`) are converted to `NULL`. The Alpha Vantage Bronze readers use explicit `StructType` schemas. The Massive/Finnhub reader lets Spark infer the schema.

---

## Data Lake Layout

```text
s3://graywolf--data--lake/
├── stock/bronze/source=<source>/dataset=<dataset>/ingestion_date=YYYY-MM-DD/run_id=<run_id>/<file>.json
├── stock/silver/datasource=<source>/dataset=<dataset>/year=YYYY/month=MM/format={csv|parquet}/
├── stock/gold/datasource=stock/dataset=company_dataset/year=YYYY/month=MM/format=csv/
└── watermark/bronze_to_silver/<dataset>.json
```

| Layer | Contents |
|-------|----------|
| **Bronze** | Raw API responses exactly as received, one folder per ingestion run. `<source>` is `alphavantage`, `massive` or `finnhub`. |
| **Silver** | Cleaned, typed and validated data, written as both CSV and Parquet. |
| **Gold** | Joined business datasets. There is one today, `company_dataset`. |
| **Watermarks** | One JSON file per dataset (see below). |

---

## Incremental Processing & Watermarks

| Dataset | Strategy | Status |
|---------|----------|--------|
| `daily_time_series` | Date-based: only rows with `day_date > watermark_value` are processed | Built in `transform/daily.py`, but the daily step is commented out in `run()` |
| `company_overview` | Hash-based: compare a SHA-256 of the business columns with the stored hash | Not built yet. `compute_content_hash()` and `hash_changed()` in `watermark/manager.py` are empty stubs, so the overview is reprocessed on every run and its watermark stores an empty hash. |

A watermark file, `watermark/bronze_to_silver/daily_time_series.json`, looks like this:

```json
{
  "pipeline_name": "bronze_to_silver",
  "dataset_name": "daily_time_series",
  "watermark_column": "day_date",
  "watermark_value": "2026-09-29",
  "last_processed_at": "2026-09-30T14:00:00+00:00",
  "batch_id": "batch_manual-20260930-1",
  "status": "SUCCESS",
  "updated_at": "2026-09-30T14:01:45+00:00",
  "updated_by": "stock_pipeline",
  "remarks": "Bronze to Silver completed successfully."
}
```

---

## Monitoring & Alerting

Every alarm notifies the SNS topic `stock-pipeline-alerts-<env>`, which is the `AlertTopicArn` output of the root stack.

| Alarm | Fires when |
|-------|------------|
| `stock-pipeline-workflow-failed-<env>` | A workflow execution fails, from an ingestion 500 or a transform error |
| `stock-pipeline-errors-<env>` | The transform Lambda errors (≥ 1 in 5 minutes) |
| `stock-pipeline-duration-warning-<env>` | A transform run takes ≥ 12 minutes (the timeout is 15) |

To receive the alerts, subscribe to the topic:

```bash
aws sns subscribe --topic-arn <AlertTopicArn> --protocol email --notification-endpoint <your-email>
```

Log lines use a `[LAYER][ACTION]` prefix, for example `[TRANSFORM][DAILY_START] ...` or `[WATERMARK][READ_OK] ...`. The transform's log group keeps logs for 30 days, and each ingestion stack defines its own log group.

---

## Security

- **IAM:** each Lambda can reach only the data lake bucket in S3, and the workflow can invoke only the four pipeline Lambdas.
- **Bucket:** the bucket has SSE-S3 (AES-256) default encryption, and versioning is on.
- **Secrets:** `FinnhubApiKey` and `MassiveApiKey` are `NoEcho` stack parameters that reach the Lambdas as environment variables. The Alpha Vantage keys are hardcoded in `ingestion/alpha_vantage/keys.py`; see [Known Gaps](#known-gaps).

---

## Storage Lifecycle

This applies to Bronze (`stock/bronze`) and Silver (`stock/silver`):

```text
S3 Standard ──30 d──▶ Standard-IA ──180 d──▶ Glacier Instant Retrieval ──365 d──▶ Glacier Flexible Retrieval ──2555 d (7 yr)──▶ Deep Archive
```

Gold and the watermarks have no lifecycle rule, so they stay in S3 Standard.

---

## Testing

```bash
python -m pytest                     # full suite; Spark tests are skipped without Java 17+
python -m pytest -m "not spark"      # skip the Spark tests
```

| File | Covers |
|------|--------|
| [`test_bronze_layout.py`](tests/test_bronze_layout.py) | Checks each Lambda's `bronze.py` against both transform readers. Also runs every ingestion handler end to end with faked APIs and S3, and checks that each file lands where the transform reads. |
| [`test_workflow.py`](tests/test_workflow.py) | The workflow contract: every Lambda gets the same `run_id`, the transform runs after ingestion, a 500 fails the workflow, every payload key is read by its Lambda, and the ingestion schedules are off. |
| [`test_imports.py`](tests/test_imports.py) | The transform modules import. For each ingestion template, `CodeUri`, `Handler` and `requirements.txt` resolve to a callable handler. |
| [`test_transforms.py`](tests/test_transforms.py) | PySpark transforms on sample rows (marked `spark`). |
| [`test_stock_config.py`](tests/test_stock_config.py) | Endpoint → dataset mapping and watermark strategies. |
| [`conftest.py`](tests/conftest.py) | Fakes for S3, HTTP and the Massive SDK, plus `load_lambda()`, which imports each ingestion folder on its own, the way Lambda does. |

The tests never call a real API or AWS.

---

## Architecture Layers

The design covers 16 layers. This table shows where each one stands.

| # | Layer | Status | Where / notes |
|---|-------|--------|---------------|
| 1 | Data quality | ✅ Built | `transform/*.py`: fake-null normalisation, business rules, `validation_status` (invalid rows are logged and dropped, not quarantined to storage) |
| 2 | Metadata & audit | 🟡 Partial | Watermarks and batch ids are stored; there is no run-history table |
| 3 | Monitoring & alerting | ✅ Built | CloudWatch alarms → SNS in the root `template.yaml` |
| 4 | Scheduling & orchestration | ✅ Built | EventBridge → Step Functions → Lambdas |
| 5 | Configuration | ✅ Built | A `config.py` per component, stack parameters, `.env` locally |
| 6 | Security & governance | 🟡 Partial | Scoped IAM, encryption and versioning are done; the Alpha Vantage keys are still in code |
| 7 | Storage formats | ✅ Built | Silver and Gold are written as CSV (for inspection) and Snappy Parquet (for analytics) |
| 8 | Streaming (Kafka / Kinesis) | 🔜 Planned | |
| 9 | Processing engine (PySpark) | 🟡 Partial | Works locally; the Lambda runtime needs changing (see [Known Gaps](#known-gaps)) |
| 10 | Downstream consumers (Athena, Redshift, BI, ML) | 🔜 Planned | |
| 11 | Storage lifecycle | ✅ Built | S3 lifecycle rules for Bronze and Silver |
| 12 | CI/CD (GitHub Actions → SAM) | 🔜 Planned | There is no CI config in the repo yet |
| 13 | Testing | ✅ Built | `tests/` |
| 14 | Structured logging | ✅ Built | `[LAYER][ACTION]` prefixes throughout |
| 15 | Pipeline metrics | 🟡 Partial | Row counts, rejects and durations are logged but not published as CloudWatch metrics |
| 16 | Data catalog (Glue Crawler) | 🔜 Planned | |

---

## Known Gaps

1. **PySpark can't run in the transform Lambda as deployed.** A zip package that includes `pyspark` goes over Lambda's 250 MB unzipped limit, and the Python runtime has no Java. Run the transform as a Lambda container image that includes Java, on AWS Glue, or on EMR Serverless.
2. **Silver and Gold writes overwrite the whole month folder** (`mode("overwrite")` on `year=/month=/`). That is fine for snapshot datasets. The daily dataset is incremental, though, so if you turn it back on as it is, each month folder would keep only the latest run's rows. Before turning it on, switch it to append or partition it by `day_date`.
3. **Hash-based change detection for the company overview is not built** (see [Incremental Processing](#incremental-processing--watermarks)).
4. **Several Silver steps are commented out** in `StockPipeline.run()` (see [What `StockPipeline.run()` does today](#what-stockpipelinerun-does-today)).
5. **The Silver, Gold and watermark paths hardcode `graywolf--data--lake`** in `src/stock_pipeline/config.py` and `watermark/config.py`. Only the Bronze paths follow `S3_BUCKET_NAME`.
6. **The Alpha Vantage API keys are hardcoded** in `ingestion/alpha_vantage/keys.py`. The plan is to move them to AWS Secrets Manager.
7. **There is no CI pipeline yet.**

---

## Roadmap

| Phase | Component |
|-------|-----------|
| 🔜 | Kafka streaming layer |
| 🔜 | AWS Glue jobs |
| 🔜 | AWS Athena |
| 🔜 | Iceberg / Delta tables |
| 🔜 | Amazon EMR |
| 🔜 | Apache Airflow orchestration |
| 🔜 | Great Expectations data quality |
| 🔜 | CloudWatch dashboards |
| 🔜 | Redshift data warehouse |
| 🔜 | BI & dashboard layer |

By day 90, this architecture will have grown into a complete enterprise-grade data platform.

---

## License

Private project, part of the 90-Day Data Engineering Roadmap.
