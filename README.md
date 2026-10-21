# Data Engineering — Datalake Pipeline

A production-style **serverless data lake** built with **Python**, **PySpark 4.0**, and **AWS** (S3 + Lambda + EventBridge + SAM) that ingests financial market and video data from **five external APIs** — Alpha Vantage, Massive (Polygon.io), Finnhub, Twelve Data, and the YouTube Data API — following the **Bronze → Silver → Gold** medallion architecture, with watermark-based incremental processing, explicit PySpark schemas, data-quality validation and quarantine, and an S3-backed state manager.

> 📘 **How to run it locally** — see the runbook: [`datalake-pipeline/notes/running_pipeline_locally.md`](datalake-pipeline/notes/running_pipeline_locally.md). Project knowledge and design notes are indexed in [`datalake-pipeline/notes/INDEX.md`](datalake-pipeline/notes/INDEX.md).

---

## Table of Contents

> 📖 **The detailed project reference is [`datalake-pipeline/README.md`](datalake-pipeline/README.md)** — data sources, module reference, data schemas, deployment, testing, troubleshooting. This top-level README is the quick overview.

- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Pipelines](#pipelines)
- [ETL Workflow](#etl-workflow)
- [Tech Stack](#tech-stack)
- [Setup & Installation](#setup--installation)
- [Configuration](#configuration)
- [Running Locally](#running-locally)
- [AWS Deployment](#aws-deployment)
- [Data Schema](#data-schema)

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                               DATA SOURCES                               │
│                                                                          │
│Alpha Vantage API                Massive (Polygon.io)                     │
│  • Daily OHLCV time series        • Exchanges, Aggregates                │
│  • Company Overview               • Stock Overview, Dividends            │
│                                                                          │
│Finnhub API                      Twelve Data API                          │
│  • Ticker Reference                • 1-min bars (SAM Lambda)             │
│                                                                          │
│YouTube Data API v3  (separate youtube_pipeline)                          │
└──────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│INGESTION  (ingestion/ — requests + boto3)                                │
│                                                                          │
│ingest() per symbol × endpoint  →  upload_raw_to_s3()                     │
│raw JSON payloads landed immutably in S3                                  │
└──────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│BRONZE LAYER  (raw JSON)                                                  │
│                                                                          │
│stock/bronze/source={ds}/dataset={d}/ingestion_date=…/run_id=…/           │
│youtube/year=YYYY/month=MM/day=DD/hour=HH/                                │
└──────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│TRANSFORM  (transform/ — PySpark, explicit StructType schemas)            │
│                                                                          │
│daily.py  •  weekly.py  •  overview.py                                    │
│exchanges.py  •  stock_tickers.py  •  stock_overview_massive.py           │
│                                                                          │
│WatermarkManager (S3 watermark/) — incremental processing:                │
│  • date-based watermark  (daily time series)                             │
│  • hash-based detection  (company overview)                              │
└──────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│SILVER LAYER  (cleaned, validated, enriched — CSV + Parquet)              │
│                                                                          │
│stock/silver/datasource={ds}/dataset={d}/                                 │
│             year=YYYY/month=MM/format={csv|parquet}/                     │
│silver/youtube/source=youtube_api/                                        │
└──────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│GOLD LAYER  (joined, analytics-ready)                                     │
│                                                                          │
│Massive stock overview  ⋈  Alpha Vantage company overview                 │
│→  company_dataset                                                        │
│stock/gold/datasource=stock/dataset=company_dataset/                      │
└──────────────────────────────────────────────────────────────────────────┘
```

All paths are relative to `s3://graywolf--data--lake/`. See [`datalake-pipeline/README.md`](datalake-pipeline/README.md) for the module-by-module reference and [`datalake-pipeline/notes/running_pipeline_locally.md`](datalake-pipeline/notes/running_pipeline_locally.md) to run it.

---

## Project Structure

```
datalake-pipeline/
├── .env.example              # Environment variable template (copy to .env)
├── .gitignore                # Git ignore rules
├── requirements.txt          # Python dependencies
├── requirements-dev.txt      # Dev dependencies (linting, testing)
│
├── src/
│   ├── __init__.py
│   │
│   ├── stock_pipeline/       # Alpha Vantage stock data pipeline
│   │   ├── __init__.py
│   │   ├── app.py            # Pipeline orchestrator + Lambda handler
│   │   ├── config.py         # Environment variable loader
│   │   ├── extract.py        # API fetching + S3 Bronze reading (Spark)
│   │   ├── transform.py      # Data transformations (PySpark)
│   │   ├── load.py           # S3 Bronze writer (boto3)
│   │   ├── database.py       # Database integration (TODO)
│   │   └── utils.py          # Shared utilities (TODO)
│   │
│   ├── youtube_pipeline/     # YouTube Data API pipeline
│   │   ├── __init__.py
│   │   ├── app.py            # Pipeline orchestrator + Lambda handler
│   │   ├── config.py         # Environment variable loader
│   │   ├── extract.py        # API fetching + S3 Bronze reading (Spark)
│   │   ├── transform.py      # Data transformations (PySpark)
│   │   ├── load.py           # S3 Bronze writer (boto3)
│   │   ├── database.py       # Database integration (TODO)
│   │   └── utils.py          # Shared utilities (TODO)
│   │
│   ├── stock_schema.json     # Inferred stock CSV schema
│   └── youtube_schema.json   # Applied YouTube CSV schema
│
├── scripts/                  # Automation scripts (TODO)
├── tests/                    # Unit & integration tests (TODO)
└── data/
    ├── raw/                  # Local raw data (git-ignored)
    └── processed/            # Local processed data (git-ignored)
```

---

## Pipelines

### Stock Pipeline

Ingests **weekly stock time series** data from the [Alpha Vantage API](https://www.alphavantage.co/documentation/) for multiple symbols.

**Symbols tracked:** `IBM`, `AAPL`, `MSFT`, `GOOGL`, `AMZN`

| Module | Class | Key Methods |
|--------|-------|-------------|
| `extract.py` | `StockDataExtractor` | `fetch_from_api()` — calls Alpha Vantage API |
| | | `extract_data()` — reads Bronze JSON into Spark DataFrame |
| | | `extract_data_csv()` — reads Bronze CSV (PERMISSIVE / DROPMALFORMED / FAILFAST modes) |
| `transform.py` | `StockDataTransformer` | `transform_weekly_timeseries()` — full JSON transformation pipeline |
| `load.py` | `StockDataLoader` | `upload_raw_to_s3()` — writes raw API response to Bronze S3 |
| `app.py` | `StockPipeline` | `run()` — orchestrates extract → transform → load |
| | `lambda_handler()` | AWS Lambda entry point |

**Derived Columns:**
- `weekly_change` — difference between open and close price
- `market_movement` — `Bull`, `Bear`, or `Neutral`
- `weekly_change_percentage` — percentage change from open to close
- `year`, `month`, `week_number` — extracted from `week_date`
- `processed_at` — timestamp when the record was processed

---

### YouTube Pipeline

Ingests **video search results** from the [YouTube Data API v3](https://developers.google.com/youtube/v3/) and processes a **trending videos CSV dataset**.

**Search query:** `"AWS Data Engineering"` (latest 100 videos)

| Module | Class | Key Methods |
|--------|-------|-------------|
| `extract.py` | `YouTubeDataExtractor` | `fetch_from_api()` — calls YouTube Data API |
| | | `extract_data()` — reads Bronze JSON into Spark DataFrame |
| | | `extract_data_csv()` — reads trending CSV dataset |
| `transform.py` | `YouTubeDataTransformer` | `transform_json_data()` — explodes and flattens API response |
| | | `transform_csv_data()` — selects and validates CSV columns |
| `load.py` | `YouTubeDataLoader` | `upload_raw_to_s3()` — writes raw API response to Bronze S3 |
| `app.py` | `YouTubePipeline` | `run()` — orchestrates extract → transform → load |
| | `lambda_handler()` | AWS Lambda entry point |

---

## ETL Workflow

Each pipeline follows the same three-phase pattern:

### Phase 1 — Ingest (API → S3 Bronze)
1. Call external API (`fetch_from_api()`)
2. Upload raw JSON response to S3 Bronze layer (`upload_raw_to_s3()`)
3. S3 key includes time-based partitioning: `year=/month=/day=/hour=/`

### Phase 2 — Extract (S3 Bronze → Spark DataFrame)
1. Read raw data from S3 Bronze using PySpark
2. Apply explicit schema (not `inferSchema`) for type safety
3. Handle malformed data with Spark read modes

### Phase 3 — Transform & Load (Spark → S3 Silver)
1. Explode nested structures, select required columns
2. Cast types, fill nulls, derive analytical columns
3. Write transformed data as CSV to S3 Silver layer

---

## Tech Stack

| Technology | Purpose |
|---|---|
| **Python 3.9+** | Core programming language |
| **PySpark 4.0** | Distributed data processing (extract + transform) |
| **boto3** | AWS SDK for S3 operations (Bronze ingestion) |
| **requests** | HTTP client for external API calls |
| **python-dotenv** | Environment variable management |
| **AWS S3** | Data lake storage (Bronze + Silver layers) |
| **AWS Lambda** | Serverless compute for ingestion |

---

## Setup & Installation

### Prerequisites

- Python 3.9 or higher
- Java 11 or 17 (required for PySpark)
- AWS account with S3 access
- Alpha Vantage API key ([get one free](https://www.alphavantage.co/support/#api-key))
- YouTube Data API key ([Google Cloud Console](https://console.cloud.google.com/))

### Install Dependencies

```bash
# Clone the repository
git clone https://github.com/jimmyptl-jer/Data-Engineering.git
cd Data-Engineering/data-engineering/datalake-pipeline

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

---

## Configuration

Copy the example environment file and fill in your credentials:

```bash
cp .env.example .env
```

### Required Environment Variables

| Variable | Description |
|---|---|
| `ALPHA_VANTAGE_API_KEY` | Alpha Vantage API key |
| `YOUTUBE_API_KEY` | YouTube Data API v3 key |
| `AWS_ACCESS_KEY_ID` | AWS IAM access key |
| `AWS_SECRET_ACCESS_KEY` | AWS IAM secret key |
| `AWS_REGION` | AWS region (default: `us-east-1`) |
| `S3_BUCKET_NAME` | Target S3 bucket for data lake |

> ⚠️ **Never commit `.env` to version control.** The `.gitignore` file already excludes it.

---

## Running Locally

### Stock Pipeline

```bash
cd datalake-pipeline
python3 -m src.stock_pipeline.app
```

### YouTube Pipeline

```bash
cd datalake-pipeline
python3 -m src.youtube_pipeline.app
```

Both pipelines will:
1. Fetch data from their respective APIs
2. Upload raw data to S3 Bronze
3. Run the Spark ETL pipeline (Bronze → Silver)

---

## AWS Deployment

### Recommended Architecture

| Component | AWS Service | Purpose |
|---|---|---|
| Ingestion (API → S3) | **AWS Lambda** | Lightweight API calls + S3 uploads |
| ETL (Bronze → Silver) | **AWS Glue** | Serverless PySpark processing |
| Scheduling | **Amazon EventBridge** | Cron-based pipeline triggers |
| Storage | **Amazon S3** | Bronze and Silver data layers |
| Orchestration | **AWS Step Functions** | Coordinate Lambda → Glue workflow |

> **Note:** PySpark is too heavy for Lambda (250MB limit, slow cold starts). The recommended pattern is to use Lambda for ingestion and AWS Glue for Spark-based ETL.

---

## Data Schema

### Stock Data (Silver Layer)

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `last_refreshed_date` | date | Last data refresh date |
| `week_date` | date | Trading week date |
| `open` | double | Opening price |
| `high` | double | Highest price |
| `low` | double | Lowest price |
| `close` | double | Closing price |
| `volume` | long | Trading volume |
| `weekly_change` | double | Open − Close |
| `market_movement` | string | Bull / Bear / Neutral |
| `weekly_change_percentage` | double | % change from open to close |
| `year` | integer | Year extracted from week_date |
| `month` | integer | Month extracted from week_date |
| `week_number` | integer | ISO week number |
| `processed_at` | timestamp | Processing timestamp |

### YouTube Data — JSON Source (Silver Layer)

| Column | Type | Description |
|---|---|---|
| `video_id` | string | YouTube video ID |
| `channel_id` | string | Channel ID |
| `channel_title` | string | Channel name |
| `channel_description` | string | Video description |
| `publish_time` | string | Video publish time |
| `publish_at` | string | Video published at timestamp |
| `kind` | string | Resource type |

### YouTube Data — CSV Source (Silver Layer)

| Column | Type | Description |
|---|---|---|
| `video_id` | string | YouTube video ID |
| `trending_date` | date | Date the video was trending |
| `title` | string | Video title |
| `channel_title` | string | Channel name |
| `category_id` | integer | YouTube category ID |
| `publish_time` | timestamp | Original publish timestamp |
| `tags` | string | Pipe-separated tags |
| `views` | long | View count |
| `likes` | long | Like count |
| `dislikes` | long | Dislike count |
| `comment_count` | long | Comment count |
| `thumbnail_link` | string | Thumbnail URL |
| `comments_disabled` | boolean | Comments disabled flag |
| `ratings_disabled` | boolean | Ratings disabled flag |
| `video_error_or_removed` | boolean | Video error/removed flag |
| `description` | string | Video description |

---

## License

This project is for educational and portfolio purposes.

---

## Author

**Jimmy Patel** — [GitHub](https://github.com/jimmyptl-jer)
