# Alpha Vantage Data Lake Pipeline

> **Enterprise Medallion Architecture** — A production-grade Bronze → Silver → Gold data lake pipeline built with PySpark, AWS S3, and Alpha Vantage market data APIs.

Part of the **90-Day Data Engineering Roadmap**.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Project Structure](#project-structure)
- [Technology Stack](#technology-stack)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Module Reference](#module-reference)
  - [Ingestion Layer](#ingestion-layer)
  - [Extract Layer](#extract-layer)
  - [Transform Layer](#transform-layer)
  - [Load Layer](#load-layer)
  - [Watermark Framework](#watermark-framework)
  - [Configuration](#configuration)
- [Data Lake Layers](#data-lake-layers)
  - [Bronze Layer](#bronze-layer)
  - [Silver Layer](#silver-layer)
  - [Gold Layer](#gold-layer)
- [Incremental Processing Framework](#incremental-processing-framework)
  - [Date-Based Watermark (Daily Time Series)](#date-based-watermark-daily-time-series)
  - [Hash-Based Change Detection (Company Overview)](#hash-based-change-detection-company-overview)
- [Watermark Storage](#watermark-storage)
- [Pipeline Execution Flow](#pipeline-execution-flow)
- [AWS Lambda Deployment](#aws-lambda-deployment)
- [Enterprise Architecture Diagram](#enterprise-architecture-diagram)
- [Current Status](#current-status)
- [Roadmap](#roadmap)

---

## Architecture Overview

This pipeline implements the **Medallion Architecture** (Bronze → Silver → Gold) pattern used by enterprise data platforms at Netflix, Uber, and Airbnb. It ingests financial market data from the Alpha Vantage API, lands it immutably in the Bronze layer, cleans and enriches it in Silver, and joins it into analytics-ready datasets in Gold.

```text
Alpha Vantage API
       │
       ▼
┌─────────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   INGESTION     │────▶│   BRONZE    │────▶│   SILVER    │────▶│    GOLD     │
│   (API + S3)    │     │  (Raw JSON) │     │  (Clean)    │     │  (Joined)   │
└─────────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                                                   │
                                                   ▼
                                          ┌─────────────────┐
                                          │   WATERMARK     │
                                          │  (State Mgmt)   │
                                          └─────────────────┘
```

**Two incremental strategies** prevent redundant processing:

| Strategy | Dataset | How It Works |
|----------|---------|-------------|
| **Date-based watermark** | Daily Time Series | Only rows with `day_date > last_watermark` are processed |
| **Hash-based detection** | Company Overview | SHA-256 of business columns compared to stored hash |

---

## Project Structure

```text
datalake-pipeline/
│
├── src/
│   ├── __init__.py
│   │
│   ├── stock_pipeline/               # Core ETL pipeline
│   │   ├── __init__.py
│   │   ├── app.py                    # Pipeline orchestrator + Lambda handler
│   │   ├── config.py                 # Centralized configuration
│   │   ├── extract.py                # Bronze/Silver data extractors
│   │   ├── transform.py              # Silver transformations
│   │   ├── load.py                   # S3 Bronze writer (boto3)
│   │   ├── utils.py                  # API key manager
│   │   └── ingestion/
│   │       └── alpha_vantage_ingestion.py  # API ingestion orchestrator
│   │
│   └── watermark/                    # Reusable watermark framework
│       ├── __init__.py
│       ├── config.py                 # Watermark S3 paths
│       └── manager.py                # WatermarkManager class
│
├── tests/
│   └── test_stock_config.py          # Configuration tests
│
├── data/                             # Local data (gitignored)
│   ├── raw/
│   └── processed/
│
├── scripts/                          # Utility scripts
│
├── .env.example                      # Environment variable template
├── .gitignore
├── requirements.txt                  # Production dependencies
└── requirements-dev.txt              # Dev/test dependencies
```

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Processing Engine** | PySpark 4.0 | Distributed data processing |
| **Cloud Storage** | AWS S3 | Data lake storage (Bronze/Silver/Gold) |
| **Data Source** | Alpha Vantage API | Financial market data |
| **Cloud Compute** | AWS Lambda | Serverless pipeline execution |
| **S3 Client** | boto3 | Bronze layer writes |
| **Language** | Python 3.9+ | Core development |
| **Config** | python-dotenv | Environment variable management |
| **Testing** | pytest | Unit & integration testing |
| **Linting** | flake8 + black | Code quality |

---

## Prerequisites

- **Python 3.9+**
- **Java 11+** (required by PySpark)
- **AWS Account** with S3 access
- **Alpha Vantage API key** (free tier: [alphavantage.co](https://www.alphavantage.co/support/#api-key))

---

## Quick Start

### 1. Clone and Set Up

```bash
git clone <repository-url>
cd datalake-pipeline

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
# For development:
pip install -r requirements-dev.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```env
# Alpha Vantage
ALPHA_VANTAGE_API_KEY=your_key_here

# AWS
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
S3_BUCKET_NAME=your-s3-bucket-name
```

The pipeline supports up to 16 API keys for rotation (`ALPHA_VANTAGE_API_KEY` through `ALPHA_VANTAGE_API_KEY_15`).

### 3. Run Locally

```bash
python -m src.stock_pipeline.app
```

### 4. Run as Lambda

The `lambda_handler` function accepts an event with optional `stock_symbols`:

```json
{
  "stock_symbols": ["IBM", "AAPL", "MSFT"]
}
```

Defaults to `["IBM"]` if not provided.

---

## Module Reference

### Ingestion Layer

**File:** [`alpha_vantage_ingestion.py`](src/stock_pipeline/ingestion/alpha_vantage_ingestion.py)

Handles the API-to-Bronze flow:

1. Selects a random API key from the key pool (supports 16 key rotation).
2. Calls the Alpha Vantage REST API.
3. Validates the response (guards against rate-limit/error payloads).
4. Uploads raw JSON to S3 Bronze via boto3.

```python
class AlphaVantageIngestion:
    def ingest(self, symbol, function, dataset, execution_start_time):
        """Fetch from API → validate → upload to S3 Bronze."""
```

**Error detection:** Checks for `Information`, `Note`, and `Error Message` keys in the API response to catch rate-limit errors before they pollute the Bronze layer.

---

### Extract Layer

**File:** [`extract.py`](src/stock_pipeline/extract.py)

Reads data from S3 into Spark DataFrames using explicit schemas:

| Method | Source | Format |
|--------|--------|--------|
| `extract_bronze_daily_data()` | Bronze | JSON |
| `extract_bronze_overview_data()` | Bronze | JSON |
| `extract_bronze_weekly_data()` | Bronze | JSON |
| `extract_silver_daily_data_parquet()` | Silver | Parquet |
| `extract_silver_daily_data_csv()` | Silver | CSV |
| `extract_silver_overview_data_parquet()` | Silver | Parquet |
| `extract_silver_overview_data_csv()` | Silver | CSV |

**Schema enforcement:** Bronze extractors use explicit `StructType` schemas (not `inferSchema`) for type safety and performance.

---

### Transform Layer

**File:** [`transform.py`](src/stock_pipeline/transform.py)

Transforms raw Bronze DataFrames into clean, validated, enriched Silver DataFrames.

#### Daily Time Series Transform

`silver_transform_daily_timeseries(daily_dataset, data_df, watermark_value=None)`

Pipeline steps:

| Step | Operation |
|------|-----------|
| 1 | Flatten nested JSON (explode `Time Series (Daily)`) |
| 2 | Select and rename columns |
| 3 | Watermark filter (if `watermark_value` provided) |
| 4 | Trim whitespace, normalize fake nulls (`"n/a"`, `"none"`, `"-"`, etc.) |
| 5 | Cast types (date, double, long) |
| 6 | Data quality validation (flag `VALID` / `INVALID`) |
| 7 | Add validation reason for invalid records |
| 8 | Filter valid records, count invalids |
| 9 | Deduplicate on `(symbol, day_date)` |
| 10 | Round price columns to 2 decimals |
| 11 | Enrich: `daily_change`, `daily_change_percentage`, `market_movement` |
| 12 | Compute 30-day rolling averages (open/close) |
| 13 | Compute 52-week high/low |
| 14 | Compute all-time high/low |
| 15 | Add partition metadata (`year`, `month`, `day`) |
| 16 | Final column selection and ordering |

#### Company Overview Transform

`silver_transform_overview(data_df)`

| Step | Operation |
|------|-----------|
| 1 | Select 52 business columns |
| 2 | Trim strings, normalize fake nulls |
| 3 | Fill defaults (`Country="Unknown"`, `Sector="Unknown"`, etc.) |
| 4 | Cast to proper types (integer, long, double, date) |
| 5 | Add partition metadata |
| 6 | Rename all columns to snake_case (e.g., `MarketCapitalization` → `market_cap`) |
| 7 | Add `processed_at` timestamp |

---

### Load Layer

**File:** [`load.py`](src/stock_pipeline/load.py)

Handles S3 uploads for the Bronze layer using `boto3.client('s3').put_object()`.

```python
class StockDataLoader:
    def upload_raw_to_s3(self, data, bucket_name, stock_symbol, bucket_key):
        """Serialize JSON and upload to S3 Bronze."""
```

Silver and Gold writes use Spark's native `df.write.parquet()` / `df.write.csv()` directly (see `_write_silver_parquet()` and `_write_silver_csv()` in `app.py`).

---

### Watermark Framework

**File:** [`watermark/manager.py`](src/watermark/manager.py)

A **reusable** watermark manager that tracks pipeline state for incremental processing.

```python
class WatermarkManager:
    def watermark_exists(pipeline_name, dataset_name) -> bool
    def read_watermark(pipeline_name, dataset_name) -> dict
    def write_watermark(watermark: dict) -> None
```

**Key design decisions:**

- Uses the **Hadoop FileSystem API** (via Spark's JVM gateway) for `_path_exists()`, so it works transparently with `s3a://`, `hdfs://`, and `file://` paths.
- Watermarks are stored as **single-row JSON files** on S3, readable by both Spark and standard JSON parsers.
- The `write_watermark()` method derives the path from `pipeline_name` and `dataset_name` keys inside the watermark dict — a single method handles both create and update.

---

### Configuration

**File:** [`config.py`](src/stock_pipeline/config.py)

Centralized pipeline settings:

```python
ALPHA_VANTAGE_ENDPOINTS = [
    {"function": "TIME_SERIES_DAILY", "dataset": "daily_time_series"},
    {"function": "OVERVIEW",          "dataset": "company_overview"},
]

WATERMARK_STRATEGIES = {
    "daily_time_series": "date_based",
    "company_overview":  "hash_based",
}
```

| Setting | Value |
|---------|-------|
| `SILVER_BASE_PATH` | `s3a://graywolf--data--lake/stock/silver/source=alphavantage/` |
| `GOLD_BASE_PATH` | `s3a://graywolf--data--lake/stock/gold/source=alphavantage/` |
| `WATERMARK_BASE_PATH` | `s3a://graywolf--data--lake/watermark/` |

---

## Data Lake Layers

### Bronze Layer

**Path:** `s3://graywolf--data--lake/stock/bronze/source=alphavantage/`

```
dataset=daily_time_series/
  year=2026/month=08/day=07/hour=03/minute=30/
    IBM.json
    AAPL.json

dataset=company_overview/
  year=2026/month=08/day=07/hour=03/minute=30/
    IBM.json
```

**Characteristics:**
- ✅ Raw JSON — exactly as returned by the API
- ✅ Immutable — never modified after landing
- ✅ Source of truth — all downstream layers can be rebuilt from Bronze
- ✅ Time-partitioned — supports replay and point-in-time analysis

---

### Silver Layer

**Path:** `s3://graywolf--data--lake/stock/silver/source=alphavantage/`

```
dataset=daily_time_series/
  year=2026/month=08/day=07/hour=03/minute=30/
    format=csv/
    format=parquet/

dataset=company_overview/
  year=2026/month=08/day=07/hour=03/minute=30/
    format=csv/
    format=parquet/
```

**Characteristics:**
- ✅ Clean, validated data (invalid rows quarantined)
- ✅ Strong types (double, long, date — not strings)
- ✅ Snake_case column names
- ✅ Enriched metrics (daily change %, 30-day averages, 52-week high/low, all-time high/low)
- ✅ Dual format (CSV for ad-hoc queries, Parquet for analytics)
- ✅ Metadata columns (`processed_at`, `validation_status`, `year`, `month`, `day`)

---

### Gold Layer

**Path:** `s3://graywolf--data--lake/stock/gold/source=alphavantage/`

```
dataset=company_dataset/
  year=2026/month=08/day=07/hour=03/minute=30/
    format=csv/
    format=parquet/
```

The Gold layer joins **Daily Time Series** + **Company Overview** on the `symbol` column to produce an analytics-ready dataset combining price action with company fundamentals.

```
Daily (price, volume, change %)  LEFT JOIN  Overview (sector, PE ratio, market cap)
           ON symbol = symbol
```

---

## Incremental Processing Framework

### Date-Based Watermark (Daily Time Series)

For **transactional / time-series data** that grows over time.

```text
Bronze Extract
    ↓
Read Watermark → get last day_date
    ↓
Filter: day_date > watermark_value
    ↓
Transform (only new records)
    ↓
┌─── isEmpty? ──┐
│               │
YES             NO
│               │
Skip            Write Silver (CSV + Parquet)
                    ↓
                Update Watermark (new max day_date)
```

### Hash-Based Change Detection (Company Overview)

For **reference / master data** that is a full snapshot (not append-only).

```text
Bronze Extract
    ↓
Silver Transform
    ↓
Compute SHA-256 of business columns
(exclude: processed_at, batch_id, year, month, day)
    ↓
Read Existing Watermark → get stored overview_hash
    ↓
Compare Hashes
    ↓
┌────────── Same? ──────────┐
│                           │
YES                         NO
│                           │
Skip Silver write           Write Silver (CSV + Parquet)
Skip Watermark update       Update Watermark (new hash)
Log "No changes detected"
```

---

## Watermark Storage

**Path:** `s3://graywolf--data--lake/watermark/bronze_to_silver/`

```
daily_time_series.json
company_overview.json
```

**Watermark Schema:**

```json
{
  "pipeline_name": "bronze_to_silver",
  "dataset_name": "daily_time_series",
  "watermark_column": "day_date",
  "watermark_value": "2026-08-06",
  "last_processed_at": "2026-08-07T03:30:00+00:00",
  "batch_id": "batch_20260807_033000",
  "status": "SUCCESS",
  "updated_at": "2026-08-07T03:31:45+00:00",
  "updated_by": "stock_pipeline",
  "remarks": "Bronze to Silver completed successfully."
}
```

**Four verified scenarios:**

| Scenario | Behavior |
|----------|----------|
| No watermark exists | Full load → Create watermark |
| Watermark exists + new data | Incremental load → Update watermark |
| Watermark exists + no new daily records | Skip Silver write → Keep existing watermark |
| Watermark exists + overview hash unchanged | Skip Silver write → Keep existing watermark |

---

## Pipeline Execution Flow

The `StockPipeline.run()` method orchestrates the full ETL:

```text
┌──────────────────────────────────────────────────────────────────┐
│                        lambda_handler()                          │
│                              │                                   │
│                     StockPipeline.run()                          │
│                              │                                   │
│         ┌────────────────────┼────────────────────┐              │
│         │                    │                    │              │
│         ▼                    ▼                    ▼              │
│  _ingest_from_api()  _process_daily()  _process_overview()       │
│    (API → Bronze)    (Date Watermark)  (Hash Detection)          │
│                              │                    │              │
│                              └────────┬───────────┘              │
│                                       │                          │
│                                       ▼                          │
│                             _build_gold_layer()                  │
│                          (Join Daily + Overview)                 │
│                                       │                          │
│                                       ▼                          │
│                              Return Results                      │
└──────────────────────────────────────────────────────────────────┘
```

---

## Implementation Deep-Dive

### Pipeline Orchestrator — `StockPipeline` Class

The [`app.py`](src/stock_pipeline/app.py) orchestrator is the heart of the pipeline. It breaks the ETL into **four isolated methods**, each responsible for one concern:

```python
class StockPipeline:
    def _ingest_from_api(stock_symbols, execution_start_time) -> list
    def _process_daily_dataset(execution_start_time, batch_id) -> bool
    def _process_overview_dataset(execution_start_time, batch_id) -> bool
    def _build_gold_layer(execution_start_time) -> None
```

| Method | Responsibility | Returns |
|--------|---------------|---------|
| `_ingest_from_api()` | Fetch from Alpha Vantage → upload raw JSON to S3 Bronze | List of result dicts |
| `_process_daily_dataset()` | Bronze → Silver for daily time series (date-based watermark) | `True` if Silver was written |
| `_process_overview_dataset()` | Bronze → Silver for company overview (hash-based detection) | `True` if Silver was written |
| `_build_gold_layer()` | Read Silver → join → write Gold (always runs for consistency) | None |

**Why this design matters:**
- Each method can be tested independently.
- A failure in overview processing doesn't block daily processing.
- Gold layer always rebuilds, ensuring consistency even if only one Silver was updated.
- Adding a new dataset = adding one new `_process_*()` method.

### Daily Transform — 16-Step Pipeline

The daily transform in [`transform.py`](src/stock_pipeline/transform.py) produces this Silver schema:

```
root
 |-- symbol: string              ← Stock ticker (e.g., "IBM")
 |-- day_date: date              ← Trading day
 |-- open: double                ← Opening price (rounded 2 decimals)
 |-- high: double                ← Daily high
 |-- low: double                 ← Daily low
 |-- close: double               ← Closing price
 |-- volume: long                ← Trading volume
 |-- daily_change: double        ← close - open (enriched)
 |-- daily_change_percentage: double ← ((close-open)/open)*100 (enriched)
 |-- market_movement: string     ← "Bull" / "Bear" / "Neutral" (enriched)
 |-- thirty_day_avg_open: double ← 30-day rolling average open (enriched)
 |-- thirty_day_avg_close: double← 30-day rolling average close (enriched)
 |-- fifty_two_week_high: double ← 52-week high per symbol (enriched)
 |-- fifty_two_week_low: double  ← 52-week low per symbol (enriched)
 |-- all_time_high: double       ← All-time high per symbol (enriched)
 |-- all_time_low: double        ← All-time low per symbol (enriched)
 |-- last_refreshed_date: date   ← Last API refresh date
 |-- validation_status: string   ← "VALID" or "INVALID"
 |-- validation_reason: string   ← Why a row was flagged invalid
 |-- year: integer               ← Partition key
 |-- month: integer              ← Partition key
 |-- day: integer                ← Partition key
 |-- processed_at: timestamp     ← Pipeline processing timestamp (IST)
```

**Validation rules applied:**
- All required fields must be non-null
- Prices must be > 0
- Volume must be >= 0
- High price must be >= low price
- Invalid records are flagged (not dropped) for quarantine analysis

### Overview Transform — 52 Business Columns

The overview transform renames all Alpha Vantage PascalCase columns to snake_case:

```
MarketCapitalization → market_cap
PERatio              → pe_ratio
52WeekHigh           → fifty_two_week_high
200DayMovingAverage  → two_hundred_day_moving_average
```

Type casting groups:

| Type | Columns |
|------|---------|
| `IntegerType` | CIK, analyst rating counts (6 columns) |
| `LongType` | Market cap, EBITDA, revenue, shares outstanding (6 columns) |
| `DoubleType` | All financial ratios and metrics (28 columns) |
| `DateType` | Latest quarter, dividend dates (3 columns) |
| `StringType` | Company identifiers and descriptions (10 columns) |

### Data Quality — Fake Null Normalization

Both transforms normalize "fake null" values that APIs often return:

```python
fake_null_values = ["", "n/a", "na", "null", "none", "-"]
```

These are converted to real Spark `null` values so downstream aggregations (averages, counts) are correct.

### S3 Path Convention

All paths follow a consistent hierarchical partition scheme:

```
s3://graywolf--data--lake/
  └── stock/
      ├── bronze/source=alphavantage/dataset={name}/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/{SYMBOL}.json
      ├── silver/source=alphavantage/dataset={name}/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/format={csv|parquet}/
      └── gold/source=alphavantage/dataset={name}/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/format={csv|parquet}/
```

This design supports:
- **Time-travel queries** — Read any historical partition.
- **Cost-effective storage** — Prune partitions for targeted scans.
- **Replay capability** — Re-process from any Bronze snapshot.

### Error Handling Pattern

The pipeline uses a consistent error handling pattern throughout:

```python
try:
    # Business logic
    logger.info("[MODULE][STEP] Description")
    ...
    logger.info("[MODULE][STEP_OK] Success message")

except Exception as e:
    logger.exception("[MODULE][STEP_FAIL] Error: %s", e)
    raise  # Always re-raise — let the caller decide recovery
```

**Key principles:**
- `logger.exception()` (not `logger.error()`) is used to capture full stack traces.
- Exceptions always re-raise — the pipeline never silently swallows errors.
- The `lambda_handler` is the single exception boundary that catches, logs duration, and returns error responses.

### Logging Convention

All log messages follow the `[MODULE][ACTION]` prefix pattern:

```
[EXTRACT][BRONZE_DAILY]    Reading from: s3a://...
[TRANSFORM][DAILY_START]   Starting daily time-series transformation.
[SILVER][WRITE_PARQUET_OK] Parquet write completed for dataset=daily_time_series.
[WATERMARK][READ_OK]       Watermark read successfully for dataset=daily_time_series.
[PIPELINE]                 Completed successfully. Duration: 42.17 seconds.
[LAMBDA]                   Execution started at 2026-08-07T03:30:00+00:00
```

This makes log filtering trivial in CloudWatch:
- Filter by layer: `[EXTRACT]`, `[TRANSFORM]`, `[SILVER]`, `[GOLD]`
- Filter by dataset: `daily_time_series`, `company_overview`
- Filter by outcome: `_OK]`, `_FAIL]`

### API Key Rotation

The pipeline supports **16 concurrent API keys** for Alpha Vantage rate-limit avoidance:

```env
ALPHA_VANTAGE_API_KEY=key0
ALPHA_VANTAGE_API_KEY_1=key1
ALPHA_VANTAGE_API_KEY_2=key2
...
ALPHA_VANTAGE_API_KEY_15=key15
```

Keys are loaded at startup, `None` values (unset env vars) are filtered out, and a random key is selected for each API call. This allows processing multiple symbols without hitting the 5-calls/minute free-tier limit.

### Batch ID Convention

Each pipeline run generates a deterministic batch ID:

```python
batch_id = f"batch_{execution_start_time.strftime('%Y%m%d_%H%M%S')}"
# Example: batch_20260807_033000
```

This is stored in the watermark and can be used to:
- Correlate Silver writes with the exact pipeline run that produced them.
- Debug issues by searching CloudWatch for the batch ID.
- Identify which Bronze partition was the source for a given Silver output.

---

## AWS Lambda Deployment

The pipeline is **AWS Lambda-ready**. The entry point is `lambda_handler(event, context)` in [`app.py`](src/stock_pipeline/app.py).

### Lambda Handler Contract

**Event payload:**

```json
{
  "stock_symbols": ["IBM", "AAPL", "MSFT", "NVDA"]
}
```

Defaults to `["IBM"]` if `stock_symbols` is not provided in the event.

**Success response:**

```json
{
  "statusCode": 200,
  "body": "Stock pipeline executed successfully.",
  "results": [
    {"symbol": "IBM", "function": "TIME_SERIES_DAILY", "response": "..."},
    {"symbol": "IBM", "function": "OVERVIEW", "response": "..."}
  ]
}
```

**Failure behavior:** Exceptions propagate to AWS Lambda runtime, which handles retries and dead-letter queues.

### SAM Template

Create a `template.yaml` at the project root for AWS SAM deployment:

```yaml
AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31
Description: Alpha Vantage Data Lake Pipeline

Globals:
  Function:
    Timeout: 900          # 15 minutes max for Lambda
    MemorySize: 3072      # PySpark needs memory
    Runtime: python3.9

Resources:
  StockPipelineFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: src.stock_pipeline.app.lambda_handler
      CodeUri: .
      Description: Bronze → Silver → Gold ETL pipeline
      Architectures:
        - x86_64
      Environment:
        Variables:
          ALPHA_VANTAGE_API_KEY: !Ref AlphaVantageApiKey
          AWS_S3_BUCKET_NAME: !Ref S3BucketName
      Policies:
        - S3CrudPolicy:
            BucketName: !Ref DataLakeBucket
      Events:
        ScheduledRun:
          Type: Schedule
          Properties:
            Schedule: cron(0 14 ? * MON-FRI *)  # 2 PM UTC, weekdays
            Description: Run after US market close
            Input: '{"stock_symbols": ["IBM", "AAPL", "MSFT"]}'

  DataLakeBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Ref S3BucketName

Parameters:
  AlphaVantageApiKey:
    Type: String
    NoEcho: true
  S3BucketName:
    Type: String
    Default: graywolf--data--lake

Outputs:
  FunctionArn:
    Description: Lambda function ARN
    Value: !GetAtt StockPipelineFunction.Arn
```

### Required IAM Policy

The Lambda execution role needs:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3DataLakeAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::graywolf--data--lake",
        "arn:aws:s3:::graywolf--data--lake/*"
      ]
    }
  ]
}
```

### Environment Variables for Lambda

Set these in the Lambda console or SAM template:

| Variable | Description | Required |
|----------|------------|----------|
| `ALPHA_VANTAGE_API_KEY` | Primary API key | ✅ |
| `ALPHA_VANTAGE_API_KEY_1` through `_15` | Additional rotation keys | Optional |
| `AWS_ACCESS_KEY_ID` | Auto-provided by Lambda execution role | Auto |
| `AWS_SECRET_ACCESS_KEY` | Auto-provided by Lambda execution role | Auto |
| `S3_BUCKET_NAME` | Target S3 bucket name | ✅ |

> **Note:** When running on Lambda, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are automatically injected by the execution role. The pipeline's boto3 client and Spark's S3A connector will use them transparently.

### EventBridge Scheduling

For automated daily runs, configure an EventBridge rule:

```
Schedule: cron(0 14 ? * MON-FRI *)
```

This triggers at **2:00 PM UTC** (after US market close) on weekdays.

**Custom event payload:**

```json
{
  "stock_symbols": ["IBM", "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
}
```

### Deploy with SAM CLI

```bash
# Build
sam build

# Deploy (guided — first time)
sam deploy --guided

# Deploy (subsequent runs)
sam deploy

# Test locally
sam local invoke StockPipelineFunction \
  --event '{"stock_symbols": ["IBM"]}'

# View logs
sam logs -n StockPipelineFunction --tail
```

### Production Considerations

| Concern | Solution |
|---------|----------|
| **Cold start** | PySpark has heavy init (~10-15s). Consider provisioned concurrency for time-sensitive runs. |
| **Memory** | Set to 3072 MB minimum. PySpark + S3 reads need headroom. |
| **Timeout** | Set to 900s (15 min max). Multi-symbol runs with Gold joins can take 5-10 min. |
| **Idempotency** | Silver writes use `mode("overwrite")` — safe to retry without duplicates. |
| **Secrets** | Use AWS Secrets Manager or SSM Parameter Store for API keys (not env vars) in production. |
| **Monitoring** | CloudWatch Logs + CloudWatch Alarms on Lambda errors + duration. |
| **Retries** | Configure Lambda retry policy (max 2 retries) with exponential backoff. |
| **Dead letter queue** | Route failed invocations to SQS DLQ for manual investigation. |

---

## Enterprise Architecture Diagram

```text
══════════════════════════════════════════════════════════════════════════════════════
                         ALPHA VANTAGE DATA LAKE PIPELINE
                        (Enterprise Medallion Architecture)
══════════════════════════════════════════════════════════════════════════════════════


                              ┌──────────────────────────────┐
                              │      Alpha Vantage API       │
                              └──────────────────────────────┘
                                           │
        ┌──────────────────────────────────┼──────────────────────────────────┐
        │                                  │                                  │
        ▼                                  ▼                                  ▼
┌──────────────────┐            ┌──────────────────┐            ┌──────────────────┐
│ TIME_SERIES_DAILY│            │ COMPANY_OVERVIEW │            │TIME_SERIES_WEEKLY│
│       ✅         │            │       🔄         │            │       🚧         │
└──────────────────┘            └──────────────────┘            └──────────────────┘
        │                                  │                                  │
        └──────────────────┬───────────────┴──────────────────────────────────┘
                           │
                           ▼
            ┌──────────────────────────────┐
            │     API INGESTION LAYER      │
            │──────────────────────────────│
            │ • Lambda Entry Point         │
            │ • API Key Rotation (16 keys) │
            │ • Error/Rate-Limit Guard     │
            │ • Request Builder            │
            │ • Structured Logging         │
            └──────────────────────────────┘
                           │
                           ▼
══════════════════════════════════════════════════════════════════════════════════════
                                   BRONZE LAYER
══════════════════════════════════════════════════════════════════════════════════════

                    Raw Landing Zone (Immutable Data)

           s3://graywolf--data--lake/stock/bronze/source=alphavantage/

               dataset=daily_time_series/
               dataset=company_overview/
                  year=YYYY/month=MM/day=DD/hour=HH/minute=MM/
                     IBM.json, AAPL.json, MSFT.json ...

           ✓ Raw JSON only          ✓ Immutable storage
           ✓ No schema changes      ✓ Source of Truth
           ✓ Replayable             ✓ Time-partitioned

                           │
                           ▼
══════════════════════════════════════════════════════════════════════════════════════
                         BRONZE EXTRACTION (Spark Reader)
══════════════════════════════════════════════════════════════════════════════════════

             Read JSON from Bronze S3 → Raw Spark DataFrame
             Explicit schemas (no inferSchema)

                           │
                           ▼
══════════════════════════════════════════════════════════════════════════════════════
                          SILVER TRANSFORMATION
══════════════════════════════════════════════════════════════════════════════════════

           Daily Time Series              Company Overview
           ──────────────────             ──────────────────
           Flatten JSON                   Flatten JSON
           Rename Columns                 Rename Columns
           Snake Case Mapping             Snake Case Mapping
           Data Type Casting              Data Type Casting
           Null Handling                  Null Handling
           Validation (VALID/INVALID)     Fill Defaults
           Enrichment:                    Add Metadata
             • daily_change %
             • 30-day averages
             • 52-week high/low
             • all-time high/low
           Add Metadata

                           │
                           ▼
══════════════════════════════════════════════════════════════════════════════════════
                     INCREMENTAL PROCESSING FRAMEWORK
══════════════════════════════════════════════════════════════════════════════════════

         DAILY DATASET                        OVERVIEW DATASET
              │                                     │
              ▼                                     ▼
        Date Watermark                       Hash Comparison
              │                                     │
     day_date > watermark?            overview_hash == watermark?
              │                                     │
     ┌────────┴────────┐               ┌────────────┴────────────┐
     │                 │               │                         │
    YES               NO             SAME                    DIFFERENT
     │                 │               │                         │
     ▼                 ▼               ▼                         ▼
  Process          Skip             Skip                    Process
  New Records      Processing       Processing              New Snapshot

                           │
                           ▼
══════════════════════════════════════════════════════════════════════════════════════
                             SILVER STORAGE LAYER
══════════════════════════════════════════════════════════════════════════════════════

           s3://graywolf--data--lake/stock/silver/source=alphavantage/

               CSV Output + Parquet Output
               Time-partitioned (year/month/day/hour/minute)

           ✓ Clean Data           ✓ Typed Schema
           ✓ Partitioned          ✓ Analytics Ready
           ✓ Business Columns     ✓ Metadata Columns

                           │
                           ▼
══════════════════════════════════════════════════════════════════════════════════════
                            WATERMARK FRAMEWORK
══════════════════════════════════════════════════════════════════════════════════════

                    WatermarkManager
                    ┌──────────────────────┐
                    │ watermark_exists()   │
                    │ read_watermark()     │
                    │ write_watermark()    │
                    │ _build_watermark_path│
                    │ _path_exists()       │
                    └──────────────────────┘

           s3://graywolf--data--lake/watermark/bronze_to_silver/
               daily_time_series.json
               company_overview.json

                           │
                           ▼
══════════════════════════════════════════════════════════════════════════════════════
                              GOLD LAYER
══════════════════════════════════════════════════════════════════════════════════════

           Read Silver Parquet + CSV
                    │
             Daily Time Series  +  Company Overview
                    │
              LEFT JOIN ON symbol
                    │
              Gold Business Dataset
                    │
           Analytics / Reporting / BI

══════════════════════════════════════════════════════════════════════════════════════
```

---

## Current Status

### ✅ Completed

| Component | Status |
|-----------|--------|
| Python + PySpark foundation | ✅ |
| SparkSession + AWS S3 integration | ✅ |
| Modular project structure | ✅ |
| Structured logging framework | ✅ |
| Configuration management | ✅ |
| Alpha Vantage API ingestion (Daily + Overview) | ✅ |
| Bronze layer (raw JSON, time-partitioned, immutable) | ✅ |
| Silver Daily transformation (15-step pipeline) | ✅ |
| Silver Overview transformation (8-step pipeline) | ✅ |
| CSV + Parquet dual-format Silver writer | ✅ |
| Gold layer (Daily + Overview join) | ✅ |
| WatermarkManager (reusable framework) | ✅ |
| Date-based incremental loading (Daily) | ✅ |
| Pipeline refactoring into reusable methods | ✅ |
| Code cleanup (print → logger, dead code removed) | ✅ |

### 🔄 In Progress

| Component | Status |
|-----------|--------|
| Hash-based incremental loading (Company Overview) | 🔄 |

### Enterprise Concepts Implemented

- ✅ Medallion Architecture (Bronze / Silver / Gold)
- ✅ Immutable Bronze layer
- ✅ Partitioned data lake
- ✅ Incremental ETL with watermarking
- ✅ Hash-based change detection (pattern defined)
- ✅ Metadata management
- ✅ Modular ETL design with reusable pipeline components
- ✅ Production-grade structured logging
- ✅ AWS Lambda-ready handler

---

## Roadmap

Future additions as part of the 90-day data engineering roadmap:

| Phase | Component |
|-------|-----------|
| 🔜 | Kafka Streaming Layer |
| 🔜 | AWS Glue Jobs |
| 🔜 | AWS Athena |
| 🔜 | Iceberg / Delta Tables |
| 🔜 | Amazon EMR |
| 🔜 | Apache Airflow Orchestration |
| 🔜 | Data Quality Layer |
| 🔜 | Monitoring & CloudWatch |
| 🔜 | Redshift Data Warehouse |
| 🔜 | BI & Dashboard Layer |

By Day 90, this architecture will evolve into a **complete enterprise-grade data platform**.

---

## License

Private project — part of the 90-Day Data Engineering Roadmap.
