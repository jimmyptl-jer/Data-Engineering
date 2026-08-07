# Alpha Vantage Data Lake Pipeline

> **Enterprise Medallion Architecture** — A production-grade Bronze → Silver → Gold data lake pipeline built with PySpark, AWS S3, AWS Lambda, and Alpha Vantage market data APIs.

Part of the **90-Day Data Engineering Roadmap**.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
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
- [Implementation Deep-Dive](#implementation-deep-dive)
- [AWS Lambda Deployment](#aws-lambda-deployment)
- [Master Enterprise Architecture Diagram](#master-enterprise-architecture-diagram)
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
├── requirements-dev.txt              # Dev/test dependencies
└── template.yaml                     # AWS SAM Infrastructure as Code template
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

---

### Transform Layer

**File:** [`transform.py`](src/stock_pipeline/transform.py)

Transforms raw Bronze DataFrames into clean, validated, enriched Silver DataFrames.

#### Daily Time Series Transform

`silver_transform_daily_timeseries(daily_dataset, data_df, watermark_value=None)`

#### Company Overview Transform

`silver_transform_overview(data_df)`

---

### Load Layer

**File:** [`load.py`](src/stock_pipeline/load.py)

Handles S3 uploads for the Bronze layer using `boto3.client('s3').put_object()`.

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

---

### Configuration

**File:** [`config.py`](src/stock_pipeline/config.py)

Centralized pipeline settings and endpoint mapping.

---

## Data Lake Layers

### Bronze Layer

**Path:** `s3://graywolf--data--lake/stock/bronze/source=alphavantage/`

Raw, immutable landing zone partition by `year/month/day/hour/minute`.

---

### Silver Layer

**Path:** `s3://graywolf--data--lake/stock/silver/source=alphavantage/`

Cleaned, typed, partitioned, enriched dataset written in CSV and Parquet.

---

### Gold Layer

**Path:** `s3://graywolf--data--lake/stock/gold/source=alphavantage/`

Joined business dataset combining price action with company fundamentals.

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
| Modular project structure | ✅ |
| Structured logging framework | ✅ |
| Configuration management | ✅ |
| Alpha Vantage API ingestion (Daily + Overview) | ✅ |
| Bronze layer (raw JSON, time-partitioned, immutable) | ✅ |
| Silver Daily transformation (15-step pipeline) | ✅ |
| Silver Overview transformation (8-step pipeline) | ✅ |
| Data Quality quarantine pattern | ✅ |
| CSV + Parquet dual-format Silver writer | ✅ |
| Gold layer (Daily + Overview join) | ✅ |
| WatermarkManager (reusable framework) | ✅ |
| Date-based incremental loading (Daily) | ✅ |
| Pipeline refactoring into reusable methods | ✅ |
| AWS SAM Infrastructure as Code (`template.yaml`) | ✅ |
| Code cleanup (print → logger, dead code removed) | ✅ |

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
