# Stock Market Data Lake Pipeline

> **Enterprise Medallion Architecture** — A production-grade Bronze → Silver → Gold data lake pipeline built with PySpark, AWS S3, AWS Lambda, and five market-data APIs (Alpha Vantage, Massive/Polygon.io, Finnhub, Twelve Data, YouTube).

> 📘 **How to run it locally** — full step-by-step runbook: [`notes/running_pipeline_locally.md`](notes/running_pipeline_locally.md). Project knowledge and design notes are indexed in [`notes/INDEX.md`](notes/INDEX.md).

Part of the **90-Day Data Engineering Roadmap**.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Data Sources](#data-sources)
- [Project Structure](#project-structure)
- [Technology Stack](#technology-stack)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Enterprise Architecture Layers](#enterprise-architecture-layers)
  - [1. Data Quality Layer](#1-data-quality-layer-)
  - [2. Metadata & Audit Layer](#2-metadata--audit-layer-)
  - [3. Monitoring & Alerting Layer](#3-monitoring--alerting-layer-)
  - [4. Scheduler & Orchestration Layer](#4-scheduler--orchestration-layer-)
  - [5. Configuration Layer](#5-configuration-layer-)
  - [6. Security & Governance Layer](#6-security--governance-layer-)
  - [7. Storage Formats & Rationale](#7-storage-formats--rationale-)
  - [8. Streaming Layer (Placeholder)](#8-streaming-layer-placeholder-)
  - [9. Processing Engine Internal Architecture](#9-processing-engine-internal-architecture-)
  - [10. Data Consumers & Downstream Layer](#10-data-consumers--downstream-layer-)
  - [11. Storage Lifecycle Policies](#11-storage-lifecycle-policies-)
  - [12. CI/CD Pipeline](#12-cicd-pipeline-)
  - [13. Testing Layer](#13-testing-layer-)
  - [14. Structured Logging Framework](#14-structured-logging-framework-)
  - [15. Pipeline Statistics & Metrics Collector](#15-pipeline-statistics--metrics-collector-)
  - [16. Data Catalog Layer](#16-data-catalog-layer-)
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
- [Data Schemas](#data-schemas)
- [Implementation Deep-Dive](#implementation-deep-dive)
- [AWS Lambda Deployment](#aws-lambda-deployment)
- [Twelve Data Ingestion (SAM Lambda)](#twelve-data-ingestion-sam-lambda)
- [YouTube Pipeline](#youtube-pipeline)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Master Enterprise Architecture Diagram](#master-enterprise-architecture-diagram)
- [Current Status](#current-status)
- [Roadmap](#roadmap)
- [License](#license)
- [Author](#author)

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

## Data Sources

Five external APIs feed the lake. Each has a dedicated ingestion module that fetches raw JSON and lands it immutably in Bronze:

| Source | Data ingested | Ingestion module | Env variable(s) |
|---|---|---|---|
| [Alpha Vantage](https://www.alphavantage.co/documentation/) | `TIME_SERIES_DAILY` (daily OHLCV), `OVERVIEW` (company fundamentals) | [`alpha_vantage_ingestion.py`](src/stock_pipeline/ingestion/alpha_vantage_ingestion.py) | `ALPHA_VANTAGE_API_KEY` + `_1`…`_15` pool |
| [Massive (Polygon.io)](https://polygon.io/) | Exchanges, aggregates (1-min bars), stock overview, splits, dividends, tickers | [`massive_ingestion.py`](src/stock_pipeline/ingestion/massive_ingestion.py) | `MASSIVE_API_KEY` |
| [Finnhub](https://finnhub.io/) | Ticker reference (symbol universe) | [`finnhub_ingestion.py`](src/stock_pipeline/ingestion/finnhub_ingestion.py) | `FINNHUB_API_KEY` |
| [Twelve Data](https://twelvedata.com/) | 1-minute bars per symbol | [`ingestion/twelvedata/`](src/stock_pipeline/ingestion/twelvedata/) — standalone SAM Lambda | `TWELVEDATA_API_KEY` |
| [YouTube Data API v3](https://developers.google.com/youtube/v3/) | Video search results + trending videos CSV | [`youtube_pipeline/`](../youtube_pipeline/) (repo root) | `YOUTUBE_API_KEY` |

> All providers offer free tiers. Alpha Vantage's free key allows ~25 requests/day — hence the **16-key rotation pool** managed by `APIKeyManager` in [`utils.py`](src/stock_pipeline/utils.py).

---

## Project Structure

```text
datalake-pipeline/
│
├── src/
│   ├── __init__.py
│   │
│   └── stock_pipeline/               # Core Data Engineering Pipeline
│       ├── __init__.py
│       ├── app.py                    # Entry Point: AWS Lambda handler & local CLI runner
│       ├── pipeline.py               # Core Orchestrator: StockPipeline class & Spark helpers
│       ├── config.py                 # Centralized configuration & environment settings
│       ├── utils.py                  # API Key Manager & 16-key rotation utilities
│       ├── extract.py               # PySpark explicit schemas & Bronze/Silver data extractors
│       ├── load.py                   # Low-level boto3 S3 Bronze layer uploader
│       ├── database.py               # Placeholder for relational DB integration
│       │
│       ├── ingestion/                # Raw API Ingestion Layer (API → S3 Bronze)
│       │   ├── __init__.py
│       │   ├── alpha_vantage_ingestion.py   # Alpha Vantage market data & fundamentals
│       │   ├── massive_ingestion.py         # Massive (Polygon.io) data & reference ingestion
│       │   ├── finnhub_ingestion.py         # Finnhub ticker reference ingestion
│       │   │
│       │   ├── alpha_vantage/      # Per-source SAM Lambda apps (template.yaml + handlers)
│       │   ├── massive/             #   tickers.py, stock_overview.py, splits.py, …
│       │   ├── finnhub/              #   ticker reference Lambda
│       │   └── twelvedata/          # 1-min bars Lambda: app.py, client.py, bronze.py,
│       │                             #   ingest.py, keys.py, config.py, template.yaml
│       │
│       ├── transform/                # Bronze-to-Silver PySpark Transformation Package
│       │   ├── __init__.py
│       │   ├── daily.py               # Daily time series (watermark, DQ assertions, rolling aggs, lag)
│       │   ├── weekly.py              # Weekly time series (DQ rules, metrics)
│       │   ├── overview.py            # Alpha Vantage company overview (hash-based detection)
│       │   ├── stock_overview_massive.py   # Massive stock overview
│       │   ├── exchanges.py           # Massive exchanges reference data
│       │   └── stock_tickers.py       # Finnhub ticker reference
│       │
│       └── watermark/                # Reusable Watermark State Manager
│           ├── __init__.py
│           ├── config.py             # Watermark S3 paths (s3a://graywolf--data--lake/watermark/)
│           ├── manager.py            # WatermarkManager class (Hadoop FileSystem API)
│           └── utils.py               # Watermark helper utilities
│
├── ingestion/                         # Top-level per-source SAM deployment folders
├── tests/
│   └── test_stock_config.py          # Configuration unit tests
├── notes/                             # Project knowledge base (“brain”) — see notes/INDEX.md
│
├── .env.example                      # Environment variable template
├── .gitignore
├── requirements.txt                  # Pinned production dependencies
├── requirements-dev.txt              # Dev/test dependencies (pytest, flake8, black)
├── template.yaml                     # AWS SAM template — pipeline Lambda + EventBridge schedule
├── samconfig.toml                    # SAM deploy defaults
├── deploy.sh                         # One-command build + deploy
└── README.md                         # This file
```

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Processing Engine** | PySpark 4.0 | Distributed data processing & Catalyst optimization |
| **Cloud Storage** | AWS S3 | Data lake storage (Bronze/Silver/Gold) |
| **Data Source** | Alpha Vantage API | Financial market data |
| **Cloud Compute** | AWS Lambda | Serverless pipeline execution |
| **Orchestration** | AWS EventBridge | Time-based cron scheduling |
| **IaC** | AWS SAM | Infrastructure as Code |
| **S3 Client** | boto3 | Bronze layer writes |
| **Language** | Python 3.9+ | Core development |
| **Config** | python-dotenv | Environment variable management |
| **Testing** | pytest | Unit & integration testing |
| **Linting** | flake8 + black | Code quality |

---

## Prerequisites

| Requirement | Notes |
|---|---|
| **Python 3.9+** | 3.12 used in development — matches pinned `pyspark==4.0.4` |
| **Java 11 or 17** | required by local PySpark (`java -version` to check) |
| **AWS account** | S3 bucket (default `graywolf--data--lake`) readable/writable by your IAM user |
| **API keys** | only for the sources you enable — see [Data Sources](#data-sources) |
| **Internet access** | first Spark run fetches `hadoop-aws` via Ivy (cached in `~/.ivy2`) |
| **pytest / flake8 / black** | installed via `requirements-dev.txt` for development |

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

Edit `.env` with your credentials (full reference in the [runbook](notes/running_pipeline_locally.md)):

```env
# ── Data source keys — set only the ones you enable ──────────────
ALPHA_VANTAGE_API_KEY=your_key          # daily OHLCV + company overview
MASSIVE_API_KEY=your_key                # exchanges, aggregates, overview, dividends
FINNHUB_API_KEY=your_key                # ticker reference
TWELVEDATA_API_KEY=your_key             # 1-min bars Lambda only

# ── AWS ────────────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
S3_BUCKET_NAME=graywolf--data--lake     # optional — this is the default
```

Rules:

- Up to **16 Alpha Vantage keys** are supported for rotation (`ALPHA_VANTAGE_API_KEY` through `ALPHA_VANTAGE_API_KEY_15`).
- `python-dotenv` loads `.env` from your **current working directory** — run from `datalake-pipeline/`.
- Keys are read **at import time**; restart the process after changing them.
- **Never commit `.env`** — `.gitignore` already excludes it.
- The YouTube pipeline (repo root) additionally needs `YOUTUBE_API_KEY` and `S3_BUCKET_NAME` (no default there).

### 3. Run Locally

```bash
python -m src.stock_pipeline.app
```

> 📘 The **complete walkthrough** (all `.env` variables, active vs. commented-out pipeline steps, S3 output layout, troubleshooting, and a no-AWS smoke test) lives in [`notes/running_pipeline_locally.md`](notes/running_pipeline_locally.md).

---

## Enterprise Architecture Layers

Production data platforms require robust governance, quality checks, observability, and security. Below are the 16 core architectural layers designed into this pipeline.

---

### 1. Data Quality Layer ⭐⭐⭐⭐⭐

Validation is treated as a dedicated architectural boundary to ensure dirty data is quarantined rather than corrupting Silver/Gold layers.

```text
                SILVER TRANSFORMATION
                        │
                        ▼
══════════════════════════════════════════════════════
                DATA QUALITY LAYER
══════════════════════════════════════════════════════
  • Schema Validation (Explicit StructType)
  • Required Field Check (symbol, day_date, close, etc.)
  • Duplicate Detection (dropDuplicates on business keys)
  • Business Rule Validation (high >= low, open/close > 0)
  • Null / Fake Null Normalization ("", "n/a", "-", "null")
  • Range & Constraint Checks (volume >= 0)
  • Quarantine Invalid Records (validation_status = INVALID)
  • Data Quality Metrics Collection
```

**Quality Checks Breakdown:**
1. **Schema Integrity:** Enforces explicit Spark schemas on raw read to catch structural breaking changes from upstream APIs.
2. **Fake Null Normalization:** Converts string placeholders (`"n/a"`, `"none"`, `"-"`, `""`) to Spark `NULL`.
3. **Business Assertions:**
   - Price check: `open > 0`, `high > 0`, `low > 0`, `close > 0`
   - Bound check: `high >= low`
   - Volume check: `volume >= 0`
4. **Quarantine Pattern:** Rows failing validation are flagged with `validation_status = 'INVALID'` and tagged with `validation_reason` for auditing rather than silently dropped.

*Future Enhancements:* Integration with **Great Expectations**, **AWS Deequ**, and **Soda Core** for automated assertion suites.

---

### 2. Metadata & Audit Layer ⭐⭐⭐⭐⭐

Maintains system auditability, lineage, and incremental state across pipeline runs.

```text
                Metadata Repository
─────────────────────────────────────────────────────
  • Watermarks          (Last processed dates & content hashes)
  • Pipeline Runs       (Execution timestamps, durations, exit status)
  • Batch History       (Unique batch_id per invocation)
  • Schema Versions     (Explicit Spark StructTypes)
  • Audit Logs          (Structured CloudWatch log entries)
```

**Tracked Metadata Attributes:**
- `pipeline_name`: e.g., `bronze_to_silver`
- `dataset_name`: e.g., `daily_time_series`, `company_overview`
- `watermark_column`: e.g., `day_date` or `overview_hash`
- `watermark_value`: Max date string or SHA-256 digest
- `batch_id`: Timestamped execution token (e.g. `batch_20260807_033000`)
- `last_processed_at`: UTC timestamp of execution start
- `updated_by`: Subsystem identifier (`stock_pipeline`)

---

### 3. Monitoring & Alerting Layer ⭐⭐⭐⭐

Ensures real-time observability and instant notification of pipeline anomalies or failures.

```text
Pipeline Run ──▶ CloudWatch Logs ──▶ CloudWatch Metrics ──▶ Alarms ──▶ SNS / Email / Slack
```

**Configured Alarms & Metrics:**
- **Error Alarm:** Triggers when `Errors >= 1` within a 5-minute period.
- **Duration Warning Alarm:** Triggers when execution exceeds 12 minutes (720,000 ms), alerting before Lambda's 15-minute hard limit.
- **API Failure Monitoring:** Logs HTTP status codes, connection errors, and rate-limit notices.
- **Watermark Progress:** Tracks watermark progression per batch.

---

### 4. Scheduler & Orchestration Layer ⭐⭐⭐⭐

Automates pipeline execution on a deterministic schedule.

```text
Amazon EventBridge (Cron) ──▶ AWS Lambda ──▶ StockPipeline.run()
```

- **Current Implementation:** EventBridge rule configured to trigger daily at `cron(0 14 ? * MON-FRI *)` (2:00 PM UTC, post US market close).
- **Roadmap Integration:**
  ```text
  Apache Airflow ──▶ AWS EMR / AWS Glue ──▶ PySpark Pipeline
  ```

---

### 5. Configuration Layer ⭐⭐⭐⭐⭐

Decouples environment-specific parameters from business logic.

```text
Configuration Strategy
┌──────────────┐     ┌──────────────┐     ┌──────────────────────┐     ┌───────────┐
│  config.py   │ ◄───│  .env File   │ ◄───│ AWS Secrets Manager  │ ◄───│ IAM Roles │
└──────────────┘     └──────────────┘     └──────────────────────┘     └───────────┘
```

- `src/stock_pipeline/config.py`: Centralized endpoint mapping, watermark strategies, and S3 URI prefixes.
- `src/watermark/config.py`: Watermark storage paths.
- `.env`: Local development key store.
- AWS Secrets Manager / Parameter Store (Production target).

---

### 6. Security & Governance Layer ⭐⭐⭐⭐

Enforces least-privilege access and data protection principles.

```text
                       SECURITY FRAMEWORK
┌─────────────────────────────────────────────────────────────┐
│  • IAM Roles: Scoped execution policies (S3 CRUD only)      │
│  • AWS Credentials: IAM role assumption (no hardcoded keys) │
│  • S3 Bucket Policies: Block Public Access enabled          │
│  • Encryption: SSE-S3 (AES-256) at-rest encryption          │
│  • Secret Management: API keys passed securely via Lambda   │
└─────────────────────────────────────────────────────────────┘
```

---

### 7. Storage Formats & Rationale ⭐⭐⭐⭐

The Silver and Gold layers store data in dual formats to serve distinct read patterns:

```text
Silver & Gold Storage
        │
        ├── CSV Format
        │     ├── Purpose: Human readable, ad-hoc inspection, legacy exports
        │     └── Options: header=True
        │
        └── Parquet Format
              ├── Purpose: High-performance analytical queries, Athena / Spark
              ├── Columnar Storage: Enables projection pushdown (read subset of columns)
              ├── Compression: Snappy compressed for reduced S3 footprint and I/O
              └── Schema Preservation: Native data types preserved without parsing
```

---

### 8. Streaming Layer (Placeholder) ⭐⭐⭐⭐⭐

Designed to seamlessly accommodate real-time streaming alongside batch processing.

```text
                                STREAMING ARCHITECTURE
┌───────────────────┐        ┌───────────────────┐        ┌───────────────────┐
│  REST API / WS    │ ────►  │ Confluent Kafka   │ ────►  │ Spark Streaming   │ ──► Bronze
│ (Real-time Ticks) │        │ (Topic: market)   │        │ / AWS Kinesis     │
└───────────────────┘        └───────────────────┘        └───────────────────┘
```

---

### 9. Processing Engine Internal Architecture ⭐⭐⭐⭐⭐

Detailed representation of PySpark's execution engine during pipeline runs:

```text
                          PYSPARK ENGINE INTERNALS
┌───────────────────────────────────────────────────────────────────────────┐
│                             Spark Driver                                  │
│   (StockPipeline orchestrator, DataFrame API calls, DAG construction)      │
└─────────────────────────────────────┬─────────────────────────────────────┘
                                      │ Catalyst Optimizer
                                      ▼ (Logical → Physical Plan)
┌───────────────────────────────────────────────────────────────────────────┐
│                             DAG Scheduler                                 │
│                   (Stages: Shuffle Read / Write boundaries)               │
└─────────────────────────────────────┬─────────────────────────────────────┘
                                      │ Task Execution
                                      ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                           Spark Executors                                 │
│        (Distributed transformation tasks, partitions, memory buffers)     │
└───────────────────────────────────────────────────────────────────────────┘
```

---

### 10. Data Consumers & Downstream Layer ⭐⭐⭐⭐⭐

Extends the Gold layer into downstream operational, analytical, and machine learning applications:

```text
                               GOLD CONSUMERS
                                     │
      ┌────────────────┬─────────────┼──────────────┬───────────────┐
      ▼                ▼             ▼              ▼               ▼
┌───────────┐   ┌────────────┐  ┌───────────┐  ┌───────────┐  ┌─────────────┐
│ AWS Athena│   │ Redshift   │  │ Power BI  │  │ ML Models │  │ REST APIs   │
│ (Ad-hoc)  │   │ (Data Whse)│  │ (BI Dash) │  │ (Predict) │  │ (Export)    │
└───────────┘   └────────────┘  └───────────┘  └───────────┘  └─────────────┘
```

---

### 11. Storage Lifecycle Policies ⭐⭐⭐⭐

Automated S3 lifecycle management for cost optimization across data tiers:

```text
                               S3 TIERING
┌──────────────┐     30 Days    ┌──────────────┐    90 Days    ┌──────────────┐
│ S3 Standard  │ ─────────────► │ Standard-IA  │ ────────────► │ S3 Glacier   │
│ (Active Bronze│               │ (Infrequent  │               │ (Archived    │
│  Silver/Gold)│                │  Raw Data)   │               │  Hist Data)  │
└──────────────┘                └──────────────┘               └──────────────┘
```

---

### 12. CI/CD Pipeline ⭐⭐⭐⭐

Automated build, test, and deployment workflow using GitHub Actions and AWS SAM:

```text
GitHub Push ──► GitHub Actions ──► Pytest & Flake8 ──► SAM Build ──► SAM Deploy ──► AWS Lambda
```

---

### 13. Testing Layer ⭐⭐⭐⭐

Ensures code quality and transformation correctness prior to deployment:

```text
                           TESTING SUITE
┌─────────────────────────────────────────────────────────────────────────┐
│ • Unit Tests: Test utility functions, APIKeyManager, path builders      │
│ • Integration Tests: Test Spark transformations with mock DataFrames    │
│ • Pipeline End-to-End Tests: Local execution against sample JSON payloads│
│ • Static Analysis: Flake8 linting & Black formatting checks             │
└─────────────────────────────────────────────────────────────────────────┘
```

---

### 14. Structured Logging Framework ⭐⭐⭐⭐⭐

Centralized, prefixed logging for operational transparency:

```text
ETL Step ──► Python Logging Engine ──► CloudWatch Stream ──► Operational Dashboard
```

Log entry format: `[LAYER][ACTION] Message`
- Example: `[TRANSFORM][DAILY_START] Starting daily time-series transformation.`
- Example: `[WATERMARK][READ_OK] Watermark read successfully for dataset=daily_time_series.`

---

### 15. Pipeline Statistics & Metrics Collector ⭐⭐⭐⭐⭐

Operational metrics collected per pipeline execution:

```text
                      PIPELINE METRICS PAYLOAD
┌─────────────────────────────────────────────────────────────────────────┐
│ • Rows Read from Bronze                                                 │
│ • Rows Written to Silver (Valid)                                        │
│ • Rows Rejected (Quarantined Invalid)                                   │
│ • API Calls Made & Response Status Codes                                │
│ • Processing Duration (Seconds)                                         │
│ • Watermark Old vs New Values                                           │
│ • Output Partition File Counts                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

### 16. Data Catalog Layer ⭐⭐⭐

Automated schema discovery and metastore cataloging:

```text
Silver / Gold S3 ──► AWS Glue Crawler ──► Glue Data Catalog ──► Athena / Redshift Spectrum
```

---

## Module Reference

### Orchestration & Entry Point

- **Entry Point File:** [`app.py`](src/stock_pipeline/app.py)  
  Exposes `lambda_handler(event, context)` for AWS Lambda execution and the `if __name__ == "__main__":` block for local CLI testing.
- **Pipeline Orchestrator:** [`pipeline.py`](src/stock_pipeline/pipeline.py)  
  Contains the `StockPipeline` class which coordinates ingestion, extraction, transformation, watermark state management, and S3 parquet/csv writing.

---

### Ingestion Layer

**Package:** `src/stock_pipeline/ingestion/`

Handles fetching raw data from external market APIs and landing JSON files into S3 Bronze:

| Module | Source API | Datasets Ingested |
|--------|------------|-------------------|
| [`alpha_vantage_ingestion.py`](src/stock_pipeline/ingestion/alpha_vantage_ingestion.py) | Alpha Vantage REST | Daily Time Series, Company Overview |
| [`massive_ingestion.py`](src/stock_pipeline/ingestion/massive_ingestion.py) | Massive (Polygon.io) | Exchanges, Aggregates/OHLCV, Splits, Dividends, Stock Overview |
| [`finnhub_ingestion.py`](src/stock_pipeline/ingestion/finnhub_ingestion.py) | Finnhub REST | Ticker Reference / Stock symbols list |
| [`twelvedata/`](src/stock_pipeline/ingestion/twelvedata/) (SAM Lambda) | Twelve Data REST | 1-minute bars per symbol |

Each source additionally ships its own SAM Lambda app for deployment — see [AWS Lambda Deployment](#aws-lambda-deployment).

---

### Extract Layer

**File:** [`extract.py`](src/stock_pipeline/extract.py)

Reads data from S3 into PySpark DataFrames using explicit `StructType` schemas to avoid schema inference overhead:

| Method | Source Layer | Format |
|--------|--------------|--------|
| `extract_bronze_daily_data()` | Bronze | JSON |
| `extract_bronze_overview_data()` | Bronze | JSON |
| `extract_bronze_weekly_data()` | Bronze | JSON |
| `extract_silver_daily_data_parquet()` | Silver | Parquet |
| `extract_silver_daily_data_csv()` | Silver | CSV |
| `extract_silver_overview_data_parquet()` | Silver | Parquet |
| `extract_silver_overview_data_csv()` | Silver | CSV |

---

### Transform Layer

**Package:** `src/stock_pipeline/transform/`

Transforms raw Bronze DataFrames into cleaned, validated, enriched Silver DataFrames:

| Module | Dataset Processed | Key Transformation Features |
|--------|-------------------|-----------------------------|
| [`daily.py`](src/stock_pipeline/transform/daily.py) | Daily Time Series | Date Watermark filter, DQ Quarantine audit, 30d rolling avg, 52w high/low, all-time high/low, windowed lag features |
| [`overview.py`](src/stock_pipeline/transform/overview.py) | Company Overview | 52 business columns selection, snake_case mapping, fake null normalization, default value imputation, explicit type casting |
| [`weekly.py`](src/stock_pipeline/transform/weekly.py) | Weekly Time Series | Weekly OHLCV flattening, DQ validation assertions, metric enrichment (weekly_change %, Bull/Bear) |
| [`exchanges.py`](src/stock_pipeline/transform/exchanges.py) | Massive Exchanges | Reference data cleaning, code standardization (MIC, type), validation reason tagging |
| [`stock_overview_massive.py`](src/stock_pipeline/transform/stock_overview_massive.py) | Massive Stock Overview | Polygon company reference data → Silver (input to the Gold join) |
| [`stock_tickers.py`](src/stock_pipeline/transform/stock_tickers.py) | Finnhub Ticker Reference | Symbol universe cleaning & standardization |

---

### Load Layer

**File:** [`load.py`](src/stock_pipeline/load.py)

Handles S3 uploads for raw JSON payloads landed into the Bronze layer using `boto3.client('s3').put_object()`.

---

### Watermark Framework

**File:** [`manager.py`](src/stock_pipeline/watermark/manager.py)

A **reusable** state manager that tracks pipeline watermarks for incremental ETL execution across S3:

```python
class WatermarkManager:
    def watermark_exists(pipeline_name, dataset_name) -> bool
    def read_watermark(pipeline_name, dataset_name) -> dict
    def write_watermark(watermark: dict) -> None
```

---

### Configuration

**File:** [`config.py`](src/stock_pipeline/config.py)

Centralized pipeline parameters, Alpha Vantage endpoint definitions, watermark strategies, and S3 URI paths.

---

## Data Lake Layers

### Bronze Layer

**Path:** `s3://graywolf--data--lake/stock/bronze/source={datasource}/dataset={dataset}/ingestion_date=YYYY-MM-DD/run_id={batch}/`

Raw, immutable landing zone — one JSON object per symbol × endpoint × run, partitioned by source, dataset, ingestion date, and batch ID.

---

### Silver Layer

**Path:** `s3://graywolf--data--lake/stock/silver/datasource={datasource}/dataset={dataset}/year=YYYY/month=MM/format={csv|parquet}/`

Cleaned, typed, validated, enriched datasets written in dual format (CSV + Parquet), partitioned by year/month.

---

### Gold Layer

**Path:** `s3://graywolf--data--lake/stock/gold/datasource=stock/dataset=company_dataset/`

Joined business dataset combining the Massive stock overview with Alpha Vantage company fundamentals (join on ticker).

---

## Data Schemas

Full per-dataset specifications live in the notes (`notes/silver_*.md`); the key contracts are summarized here.

### Silver — `daily_time_series` (Alpha Vantage)

**Business key:** `(symbol, day_date)` — one price record per symbol per trading day. **Incremental strategy:** date-based watermark.

| # | Column | Type | Description |
|---|---|---|---|
| 1 | `symbol` | STRING | Stock ticker (uppercase) |
| 2 | `day_date` | DATE | Trading date (ISO 8601) |
| 3 | `open` | DOUBLE | Opening price (> 0) |
| 4 | `high` | DOUBLE | Intraday peak (`high >= low`) |
| 5 | `low` | DOUBLE | Intraday trough |
| 6 | `close` | DOUBLE | Closing price |
| 7 | `volume` | LONG | Shares traded (>= 0) |
| 8 | `daily_change` | DOUBLE | `close − open` |
| 9 | `daily_change_percentage` | DOUBLE | % change open → close |
| 10 | `market_movement` | STRING | `Bull` / `Bear` / `Neutral` |
| 11 | `thirty_day_avg_open` | DOUBLE | Rolling 30-day average open |
| 12 | `thirty_day_avg_close` | DOUBLE | Rolling 30-day average close |
| 13 | `fifty_two_week_high` | DOUBLE | Rolling 365-day max high |
| 14 | `fifty_two_week_low` | DOUBLE | Rolling 365-day min low |
| 15 | `all_time_high` | DOUBLE | Historical max high |
| 16 | `all_time_low` | DOUBLE | Historical min low |
| 17 | `last_refreshed_date` | DATE | API last-refresh date |
| 18 | `validation_status` | STRING | `VALID` / `INVALID` |
| 19 | `validation_reason` | STRING | Quarantine reason (NULL if valid) |
| 20 | `year` / `month` / `day` | INTEGER | Partition columns derived from `day_date` |
| 21 | `processed_at` | TIMESTAMP | Processing timestamp (Asia/Kolkata) |
| 22 | `previous_close` | DOUBLE | Prior session close (`lag` window) |
| 23 | `previous_open` | DOUBLE | Prior session open |
| 24 | `previous_high` | DOUBLE | Prior session high |
| 25 | `previous_low` | DOUBLE | Prior session low |

Full 19-step transform spec: [`notes/silver_daily_time_series.md`](notes/silver_daily_time_series.md).

### Silver — `weekly_time_series` (Alpha Vantage)

Business key `(symbol, week_date)`; weekly OHLCV plus derived `weekly_change`, `weekly_change_percentage`, `market_movement`, ISO `week_number`, `year`, `month`, `processed_at`. Spec: [`notes/silver_weekly_time_series.md`](notes/silver_weekly_time_series.md).

### Silver — `company_overview` (Alpha Vantage)

57 output columns (52 business fields + metadata): company identity (symbol, name, exchange, currency, sector, industry, country), valuation & shares (market cap, PE, EPS, dividend, 52-week range), profitability & growth margins, balance-sheet strength, and analyst targets. **Change detection:** SHA-256 hash of business columns. Spec: [`notes/silver_company_overview.md`](notes/silver_company_overview.md).

### Silver — Massive datasets (`exchanges`, `stock_overview`, `aggregates`, `dividends`)

Reference & market datasets from Polygon.io — the exchange registry (MIC codes, types, locales), the company overview that feeds the Gold join, 1-minute aggregates, and dividend history. Spec: [`notes/silver_exchanges.md`](notes/silver_exchanges.md).

### Silver — `ticker_reference` (Finnhub)

Cleaned symbol universe (symbol, description, type, currency, exchange/MIC) used to validate pipeline symbols. Spec: [`notes/silver_ticker_reference.md`](notes/silver_ticker_reference.md).

### Gold — `company_dataset`

Massive `stock_overview` ⋈ Alpha Vantage `company_overview` (join on ticker) → one enriched row per company, written as CSV under `stock/gold/datasource=stock/dataset=company_dataset/`.

### YouTube (separate pipeline)

* **JSON source (Silver):** `video_id`, `channel_id`, `channel_title`, `channel_description`, `publish_time`, `publish_at`, `kind`
* **CSV source (Silver):** `video_id`, `trending_date`, `title`, `channel_title`, `category_id`, `publish_time`, `tags`, `views`, `likes`, `dislikes`, `comment_count`, `thumbnail_link`, `comments_disabled`, `ratings_disabled`, `video_error_or_removed`, `description`

---

## Incremental Processing Framework

### Date-Based Watermark (Daily Time Series)

Processes only records with `day_date > last_watermark`.

### Hash-Based Change Detection (Company Overview)

Calculates SHA-256 hash of business columns and compares against stored watermark to skip unchanged snapshots.

---

## Watermark Storage

**Path:** `s3://graywolf--data--lake/watermark/bronze_to_silver/`

Schema:
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

---

## Pipeline Execution Flow

```text
┌──────────────────────────────────────────────────────────────────┐
│                        lambda_handler()                          │
│                              │                                   │
│                     StockPipeline.run()                          │
│                              │                                   │
│         ┌────────────────────┼────────────────────┐              │
│         │                    │                    │              │
│         ▼                    ▼                    ▼              │
│  _ingest_from_api()  _process_daily()  _process_overview()      │
│    (API → Bronze)    (Date Watermark)  (Hash Detection)         │
│                              │                    │              │
│                              └────────┬───────────┘              │
│                                       │                          │
│                                       ▼                          │
│                             _build_gold_layer()                  │
│                          (Join Daily + Overview)                 │
│                                       │                          │
│                                       ▼                          │
│                              Return Results                     │
└──────────────────────────────────────────────────────────────────┘
```

---

## Implementation Deep-Dive

### Pipeline Orchestrator — `StockPipeline` Class

Isolated execution methods:
- `_ingest_from_api()`
- `_process_daily_dataset()`
- `_process_overview_dataset()`
- `_build_gold_layer()`

---

## AWS Lambda Deployment

Configured with [`template.yaml`](template.yaml) using AWS SAM.

* **Pipeline Lambda** — packages the `src.stock_pipeline.app:lambda_handler` entry point; scheduled by EventBridge at `cron(0 14 ? * MON-FRI *)` (2:00 PM UTC, after the US market close).
* **One-command deploy** — [`deploy.sh`](deploy.sh) wraps `sam build && sam deploy` with defaults from [`samconfig.toml`](samconfig.toml).
* **Per-source ingestion Lambdas** — each source also ships its own SAM app: [`ingestion/alpha_vantage/`](src/stock_pipeline/ingestion/alpha_vantage/), [`ingestion/massive/`](src/stock_pipeline/ingestion/massive/), [`ingestion/finnhub/`](src/stock_pipeline/ingestion/finnhub/), [`ingestion/twelvedata/`](src/stock_pipeline/ingestion/twelvedata/).

```bash
sam build && sam deploy --guided    # pipeline Lambda
```

> **Note:** PySpark is heavy for Lambda (250 MB limit, slow cold starts). For sustained Spark ETL at scale, the recommended production pattern is **Lambda for ingestion + AWS Glue for Bronze → Silver** — see the [Roadmap](#roadmap).

---

## Twelve Data Ingestion (SAM Lambda)

[`src/stock_pipeline/ingestion/twelvedata/`](src/stock_pipeline/ingestion/twelvedata/) is a standalone SAM Lambda that lands 1-minute bars into Bronze:

| File | Role |
|---|---|
| `app.py` | `lambda_handler` — event contract below |
| `client.py` | Twelve Data REST client |
| `ingest.py` | Per-symbol ingestion loop |
| `bronze.py` | S3 Bronze landing helpers |
| `keys.py` | `TWELVEDATA_API_KEY` loader |
| `config.py`, `template.yaml`, `samconfig.toml`, `requirements.txt` | Config & deployment |

**Event contract** (all fields optional):

| Field | Default | Meaning |
|---|---|---|
| `stock_symbols` | `["IBM"]` | Symbols to ingest |
| `full_load` | `false` | `true` → 5000 bars; `false` → one trading day |
| `datasource` | `twelvedata` | Bronze `source=` partition value |
| `run_id` | auto-generated | Bronze `run_id=` partition value |

```bash
cd src/stock_pipeline/ingestion/twelvedata
sam build
sam local invoke                     # defaults
sam local invoke -e event.json       # custom event
```

---

## YouTube Pipeline

A sibling pipeline lives at the repo root (`youtube_pipeline/`, run from the `Data-Engineering/` directory):

```bash
python -m youtube_pipeline.app
```

Flow: YouTube Data API v3 search (`"AWS Data Engineering"`, latest 100 videos) → raw JSON to Bronze (`youtube/year=…/month=…/day=…/hour=…/`) → transform JSON (explode items) + trending-videos CSV → Silver CSV under `s3a://graywolf--data--lake/silver/youtube/source=youtube_api/`.

Requires `YOUTUBE_API_KEY` and `S3_BUCKET_NAME` (no default) plus the AWS keys in a `.env` at the repo root.

---

## Testing

```bash
pytest tests/            # unit tests
flake8 src/ tests/       # lint
black --check src/       # format check
```

* `tests/test_stock_config.py` — configuration unit tests (3 tests).
* **No-AWS smoke test** — exercise the Spark transform layer with synthetic Bronze data; recipe in [runbook §9](notes/running_pipeline_locally.md).

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `ValueError: No valid Alpha Vantage API keys found` | Key missing from `.env`, or `.env` not in the launch directory; restart after setting |
| `ModuleNotFoundError: finnhub` / `massive` | `pip install -r requirements.txt` (pins `finnhub-python==2.4.29`, `massive==2.8.0`) |
| Ivy/Maven download error on startup | First Spark run needs internet for `hadoop-aws`; check proxy, retry |
| `Path does not exist: s3a://…silver/…` | Silver partition (`year=YYYY/month=MM`) not written yet — run the Silver steps first |
| S3 403 / AccessDenied | Wrong AWS keys, or IAM lacks bucket read/write |
| First run logs `mode=FULL_LOAD` | Normal — no watermark exists yet; later runs are incremental |
| `SyntaxError: invalid non-printable character U+FEFF` | File saved as “UTF-8 with BOM” — re-save as plain UTF-8 |

Full table with more context: [runbook §8](notes/running_pipeline_locally.md).

---

## Master Enterprise Architecture Diagram

```text
══════════════════════════════════════════════════════════════════════════════════════════════════════════════
                                   ALPHA VANTAGE DATA LAKE PIPELINE
                               (Enterprise Medallion Architecture)
══════════════════════════════════════════════════════════════════════════════════════════════════════════════

                                            ┌──────────────────────────────┐
                                            │      Alpha Vantage API       │
                                            └──────────────────────────────┘
                                                         │
          ┌──────────────────────────────────────────────┼──────────────────────────────────────────────┐
          │                                              │                                              │
          ▼                                              ▼                                              ▼
 ┌────────────────────┐                        ┌────────────────────┐                        ┌────────────────────┐
 │ TIME_SERIES_DAILY  │                        │ COMPANY_OVERVIEW   │                        │ TIME_SERIES_WEEKLY │
 │        ✅          │                        │        🔄          │                        │        🚧          │
 └────────────────────┘                        └────────────────────┘                        └────────────────────┘
          │                                              │                                              │
          └───────────────────────────────┬──────────────┴──────────────────────────────────────────────┘
                                          │
                                          ▼
                          ┌─────────────────────────────────────┐
                          │       API INGESTION LAYER           │
                          │─────────────────────────────────────│
                          │ • Lambda Entry Point                │
                          │ • API Authentication & 16-key Pool  │
                          │ • Request Builder & Rate-Limit Guard│
                          │ • Standard API Response Validation  │
                          │ • Error Handling & Retry Logic      │
                          │ • Structured Logging ([INGEST])     │
                          └─────────────────────────────────────┘
                                          │
                                          ▼
══════════════════════════════════════════════════════════════════════════════════════════════════════════════
                                            BRONZE LAYER
══════════════════════════════════════════════════════════════════════════════════════════════════════════════

                              Raw Landing Zone (Immutable Data)

                     s3://graywolf--data--lake/stock/bronze/source=alphavantage/
                                          │
                                          ▼
                     year=YYYY/month=MM/day=DD/hour=HH/minute=MM/
                                          │
                                          ▼
                   dataset=daily_time_series/   dataset=company_overview/
                                          │
                                          ▼
                      IBM.json, AAPL.json, MSFT.json, NVDA.json...

Characteristics:
✓ Raw JSON only        ✓ Immutable storage    ✓ Source of Truth
✓ No schema changes    ✓ No filtering/cleaning ✓ Replayable & Time-partitioned

                                          │
                                          ▼
══════════════════════════════════════════════════════════════════════════════════════════════════════════════
                                  BRONZE EXTRACTION LAYER
══════════════════════════════════════════════════════════════════════════════════════════════════════════════

                     Spark Reader (Explicit StructType Schemas)
                                          │
                                          ▼
                            Raw Spark DataFrame Processing
                                          │
                                          ▼
══════════════════════════════════════════════════════════════════════════════════════════════════════════════
                                   SILVER TRANSFORMATION
══════════════════════════════════════════════════════════════════════════════════════════════════════════════

                         Daily Time Series                    Company Overview
                         ──────────────────                   ──────────────────
                         Flatten JSON                         Flatten JSON
                         Rename Columns                       Rename Columns
                         Snake Case Mapping                   Snake Case Mapping
                         Data Type Casting                    Data Type Casting
                         Null Normalization                   Fill Defaults
                         Enrichment (30d avg, 52w high/low)  Add Metadata
                         Add Partition Metadata

                                          │
                                          ▼
══════════════════════════════════════════════════════════════════════════════════════════════════════════════
                                     DATA QUALITY LAYER
══════════════════════════════════════════════════════════════════════════════════════════════════════════════

              ┌─────────────────────────────────────────────────────────────┐
              │ • Schema Integrity Check                                    │
              │ • Fake Null Normalization ("", "n/a", "none" -> NULL)       │
              │ • Business Assertions (price > 0, high >= low, volume >= 0) │
              │ • Quarantine Invalid Records (validation_status = INVALID)  │
              └─────────────────────────────────────────────────────────────┘

                                          │
                                          ▼
══════════════════════════════════════════════════════════════════════════════════════════════════════════════
                              INCREMENTAL PROCESSING FRAMEWORK
══════════════════════════════════════════════════════════════════════════════════════════════════════════════

                          DAILY DATASET                     OVERVIEW DATASET
                                 │                                │
                                 ▼                                ▼
                        Date Watermark                  Hash Comparison
                                 │                                │
                     day_date > watermark?        overview_hash == watermark?
                                 │                                │
                      ┌──────────┴──────────┐        ┌────────────┴────────────┐
                      │                     │        │                         │
                     YES                   NO       SAME                  DIFFERENT
                      │                     │        │                         │
                      ▼                     ▼        ▼                         ▼
            Process New Records      Skip Processing    Skip Processing   Process New Snapshot

                                          │
                                          ▼
══════════════════════════════════════════════════════════════════════════════════════════════════════════════
                                  SILVER STORAGE LAYER
══════════════════════════════════════════════════════════════════════════════════════════════════════════════

                  s3://graywolf--data--lake/stock/silver/source=alphavantage/
                                           │
                                           ▼
                            CSV Output  +  Parquet Output
                            Time-partitioned (year/month/day/hour/minute)

                                           │
                                           ▼
══════════════════════════════════════════════════════════════════════════════════════════════════════════════
                                   WATERMARK FRAMEWORK
══════════════════════════════════════════════════════════════════════════════════════════════════════════════

                      WatermarkManager (Hadoop FileSystem API)
                      ┌────────────────────────────────────────┐
                      │ watermark_exists()   read_watermark()  │
                      │ write_watermark()    _path_exists()    │
                      └────────────────────────────────────────┘
                                           │
                                           ▼
            s3://graywolf--data--lake/watermark/bronze_to_silver/
                     daily_time_series.json, company_overview.json

                                          │
                                          ▼
══════════════════════════════════════════════════════════════════════════════════════════════════════════════
                                         GOLD LAYER
══════════════════════════════════════════════════════════════════════════════════════════════════════════════

                  Read Silver Parquet + CSV
                          │
                          ▼
              Daily Time Series  +  Company Overview
                          │
                          ▼
                 LEFT JOIN ON symbol
                          │
                          ▼
             Gold Business Dataset (CSV + Parquet)

                                          │
                                          ▼
══════════════════════════════════════════════════════════════════════════════════════════════════════════════
                                  DATA CONSUMERS & ANALYTICS
══════════════════════════════════════════════════════════════════════════════════════════════════════════════

      ┌──────────────┬───────────────┬───────────────┬───────────────┐
      ▼              ▼               ▼               ▼               ▼
   AWS Athena    AWS Redshift     Power BI       ML Models       REST APIs
  (Ad-hoc SQL)   (Data Whse)    (Dashboards)   (Predictive)     (External)

══════════════════════════════════════════════════════════════════════════════════════════════════════════════
```

---

## Current Status

### ✅ Completed

| Component | Status |
|-----------|--------|
| Python + PySpark foundation | ✅ |
| SparkSession + AWS S3 integration | ✅ |
| Modular project structure (`pipeline.py`, `app.py`, `transform/`, `ingestion/`, `watermark/`) | ✅ |
| Structured logging framework | ✅ |
| Configuration management | ✅ |
| Multi-API Ingestion (Alpha Vantage, Massive/Polygon.io, Finnhub) | ✅ |
| Bronze layer (raw JSON, time-partitioned, immutable) | ✅ |
| Silver Daily transformation (17-step pipeline with lag & rolling aggs) | ✅ |
| Silver Overview transformation (8-step fundamental data pipeline) | ✅ |
| Silver Exchanges transformation (Massive reference data) | ✅ |
| Data Quality quarantine pattern (`VALID` / `INVALID` auditing) | ✅ |
| CSV + Parquet dual-format Silver writer | ✅ |
| Gold layer (Daily + Overview join) | ✅ |
| WatermarkManager (consolidated inside `stock_pipeline/watermark/`) | ✅ |
| Date-based incremental loading (Daily) | ✅ |
| Full codebase comment enrichment & docstring standardization | ✅ |
| AWS SAM Infrastructure as Code (`template.yaml`) | ✅ |
| Code cleanup (`print()` → `logger`, dead code `old-app.py` deleted) | ✅ |

### 🔄 In Progress

| Component | Status |
|-----------|--------|
| Hash-based incremental loading (Company Overview) | 🔄 |

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
| 🔜 | Great Expectations Data Quality Integration |
| 🔜 | Monitoring & CloudWatch Dashboards |
| 🔜 | Redshift Data Warehouse |
| 🔜 | BI & Dashboard Layer |

By Day 90, this architecture will evolve into a **complete enterprise-grade data platform**.

---

## License

Private project — part of the 90-Day Data Engineering Roadmap.

---

## Author

**Jimmy Patel** — [GitHub](https://github.com/jimmyptl-jer)
