# Complete Technical Data Flow Architecture & Operations Guide

> **Repository**: `datalake-pipeline`  
> **Architecture Pattern**: Medallion Data Lake Architecture (Bronze → Silver → Gold)  
> **Processing Engine**: Apache Spark (PySpark 3.4.1 + Hadoop AWS S3A Connector `org.apache.hadoop:hadoop-aws:3.4.1`)  
> **Storage Layer**: AWS S3 (`s3a://graywolf--data--lake/`)  
> **Orchestrator**: Custom Python Orchestrator (`src/stock_pipeline/pipeline.py`)  
> **Entry Point**: `src/stock_pipeline/app.py`  
> **Deployment Target**: AWS Lambda (via SAM CLI) / Local Spark CLI

---

## Table of Contents
1. [End-to-End System Architecture Diagram](#1-end-to-end-system-architecture-diagram)
2. [Codebase Structure & Component Responsibilities](#2-codebase-structure--component-responsibilities)
3. [Configuration & Environment Architecture (`config.py`)](#3-configuration--environment-architecture-configpy)
4. [Spark Session & Storage Engine (`create_spark_session`)](#4-spark-session--storage-engine-create_spark_session)
5. [Phase 1: Ingestion & Bronze Layer (Raw Source Truth)](#5-phase-1-ingestion--bronze-layer-raw-source-truth)
6. [Phase 2: Transformation & Silver Layer (Trusted Data)](#6-phase-2-transformation--silver-layer-trusted-data)
7. [Phase 3: Unification & Gold Layer (Business Analytics)](#7-phase-3-unification--gold-layer-business-analytics)
8. [Incremental Processing & Watermark Engine (`WatermarkManager`)](#8-incremental-processing--watermark-engine-watermarkmanager)
9. [Data Quality Audit Engine & Rule Matrix](#9-data-quality-audit-engine--rule-matrix)
10. [Operations & Execution Guide (Step-by-Step Run Book)](#10-operations--execution-guide-step-by-step-run-book)
11. [Error Handling, Logging Tags & Resilience Scenarios](#11-error-handling-logging-tags--resilience-scenarios)
12. [PySpark Execution Mechanics & Optimization Principles](#12-pyspark-execution-mechanics--optimization-principles)

---

## 1. End-to-End System Architecture Diagram

```mermaid
graph TD
    %% EXTERNAL APIS
    subgraph REST APIs [External Data Providers]
        API_AV[Alpha Vantage REST API<br/>https://www.alphavantage.co/query<br/>Functions: TIME_SERIES_DAILY, TIME_SERIES_WEEKLY, OVERVIEW]
        API_FH[Finnhub REST API<br/>https://finnhub.io/api/v1/stock/symbol]
        API_PG[Polygon.io / Massive REST API<br/>https://api.polygon.io/v3/reference/exchanges]
    end

    %% INGESTION CONTROLLERS
    subgraph Ingestion Layer [Python Ingestion Controllers & boto3 Loader]
        ING_AV[AlphaVantageIngestion<br/>Multi-Key Rotation Pool 1-16]
        ING_FH[FinnHubIngestion]
        ING_PG[MassiveIngestion]
        LOADER[StockDataLoader<br/>boto3 S3 Client]
    end

    %% BRONZE S3 STORAGE
    subgraph Bronze Layer [Bronze S3 Bucket: Raw JSON Payloads]
        B_Daily[s3a://graywolf--data--lake/stock/bronze/source=alphavantage/dataset=daily_time_series/.../{SYMBOL}.json]
        B_Weekly[s3a://graywolf--data--lake/stock/bronze/source=alphavantage/dataset=weekly_time_series/.../{SYMBOL}.json]
        B_Overview[s3a://graywolf--data--lake/stock/bronze/source=alphavantage/dataset=company_overview/.../{SYMBOL}.json]
        B_Ticker[s3a://graywolf--data--lake/stock/bronze/source=finnhub/dataset=ticker_reference/.../finnhub_tickers.json]
        B_Exchange[s3a://graywolf--data--lake/stock/bronze/source=massive/dataset=exchanges/.../massive_exchanges.json]
    end

    %% SPARK EXTRACTOR & EXPLICIT SCHEMAS
    subgraph Spark Extractor [StockDataExtractor & Explicit StructTypes]
        EXT_SPARK[StockDataExtractor<br/>Explicit PySpark StructTypes:<br/>stock_schema_daily, stock_schema_weekly, stock_schema_overview]
    end

    %% SILVER PYSPARK TRANSFORMS
    subgraph Silver Transforms [PySpark Silver Transformation Modules]
        T_Daily[silver_transform_daily_timeseries in daily.py<br/>19-Step Transform | 27 Output Columns]
        T_Weekly[transform_weekly_timeseries in weekly.py<br/>15-Step Transform | 17 Output Columns]
        T_Overview[silver_transform_overview in overview.py<br/>10-Step Transform | 57 Output Columns]
        T_Ticker[transform_finnhub_stock_tickers_dataset in stock_tickers.py<br/>12-Step Transform | 13 Output Columns]
        T_Exchange[transform_massive_exchanges_dataset in exchanges.py<br/>9-Step Transform | 10 Output Columns]
    end

    %% SILVER S3 STORAGE
    subgraph Silver Layer [Silver S3 Bucket: Parquet + CSV Format]
        S_Daily[(Silver: daily_time_series<br/>Parquet & CSV)]
        S_Weekly[(Silver: weekly_time_series<br/>Parquet & CSV)]
        S_Overview[(Silver: company_overview<br/>Parquet & CSV)]
        S_Ticker[(Silver: ticker_reference<br/>Parquet & CSV)]
        S_Exchange[(Silver: exchanges<br/>Parquet & CSV)]
        S_Quarantine[(Silver Quarantine Path<br/>INVALID Records)]
    end

    %% WATERMARK ENGINE
    subgraph Watermark Engine [WatermarkManager Catalog Engine]
        WM[WatermarkManager<br/>Hadoop FileSystem JVM API<br/>date_based: day_date > max_watermark<br/>hash_based: SHA-256 Digest Match]
    end

    %% GOLD LAYER UNIFICATION
    subgraph Gold Layer [Gold S3 Bucket: Business Analytics Datasets]
        G_Company[(Gold: company_dataset<br/>daily_time_series LEFT JOIN company_overview ON symbol<br/>Parquet & CSV)]
    end

    %% INGESTION FLOWS
    API_AV --> ING_AV --> LOADER --> B_Daily & B_Weekly & B_Overview
    API_FH --> ING_FH --> LOADER --> B_Ticker
    API_PG --> ING_PG --> LOADER --> B_Exchange

    %% SILVER FLOWS
    B_Daily --> EXT_SPARK --> T_Daily --> S_Daily
    B_Weekly --> EXT_SPARK --> T_Weekly --> S_Weekly
    B_Overview --> EXT_SPARK --> T_Overview --> S_Overview
    B_Ticker --> EXT_SPARK --> T_Ticker --> S_Ticker
    B_Exchange --> EXT_SPARK --> T_Exchange --> S_Exchange

    %% QUARANTINE FLOW
    T_Daily & T_Weekly & T_Ticker & T_Exchange -.->|status == INVALID| S_Quarantine

    %% WATERMARK FLOW
    S_Daily & S_Overview <--> WM

    %% GOLD FLOW
    S_Daily & S_Overview -->|_build_gold_layer LEFT JOIN| G_Company
```

---

## 2. Codebase Structure & Component Responsibilities

```text
datalake-pipeline/
├── src/
│   └── stock_pipeline/               # Core Data Pipeline Package
│       ├── app.py                    # Entry Point: AWS Lambda handler & local CLI runner
│       ├── pipeline.py               # Core Orchestrator: StockPipeline class & Spark helpers
│       ├── config.py                 # Centralized configuration & environment settings
│       ├── utils.py                  # API Key Manager & rotation utilities
│       ├── extract.py                # PySpark explicit schemas & Bronze/Silver data extractors
│       ├── load.py                   # StockDataLoader using boto3 client for Bronze S3 JSON landing
│       ├── ingestion/
│       │   ├── alpha_vantage_ingestion.py  # REST client for Alpha Vantage API endpoints
│       │   ├── finnhub_ingestion.py        # REST client for Finnhub stock ticker reference
│       │   └── massive_ingestion.py        # REST client for Polygon/Massive exchange metadata
│       ├── transform/
│       │   ├── daily.py                    # 19-Step Silver Daily Time Series PySpark transform
│       │   ├── weekly.py                   # 15-Step Silver Weekly Time Series PySpark transform
│       │   ├── overview.py                 # 10-Step Silver Company Overview PySpark transform
│       │   ├── stock_tickers.py            # 12-Step Silver Finnhub Stock Tickers PySpark transform
│       │   └── exchanges.py                # 9-Step Silver Massive Exchanges PySpark transform
│       └── watermark/
│           ├── manager.py                  # WatermarkManager S3 state manager
│           └── utils.py                    # Hash computation helper utilities
└── notes/                             # Architectural & Dataset Specifications
    ├── data_flow_architecture.md
    ├── silver_daily_time_series.md
    ├── silver_weekly_time_series.md
    ├── silver_company_overview.md
    ├── silver_ticker_reference.md
    ├── silver_exchanges.md
    └── gold_layer_master.md
```

---

## 3. Configuration & Environment Architecture (`config.py`)

Centralized constants defined in `src/stock_pipeline/config.py`:

```python
# API Endpoint Mappings
ALPHA_VANTAGE_ENDPOINTS = [
    {"function": "TIME_SERIES_DAILY", "dataset": "daily_time_series"},
    {"function": "OVERVIEW", "dataset": "company_overview"},
]

# Incremental Watermark Processing Mappings
WATERMARK_STRATEGIES = {
    "daily_time_series": "date_based",  # Time-series delta filter (day_date > watermark)
    "company_overview": "hash_based",   # Master snapshot digest comparison (SHA-256)
}

# Data Lake Base Paths
SILVER_BASE_PATH = "s3a://graywolf--data--lake/stock/silver/"
GOLD_BASE_PATH   = "s3a://graywolf--data--lake/stock/gold/"
S3_BUCKET_NAME   = os.getenv("S3_BUCKET_NAME", "graywolf--data--lake")
```

---

## 4. Spark Session & Storage Engine (`create_spark_session`)

Configured in `src/stock_pipeline/pipeline.py`:

```python
spark = (
    SparkSession.builder
    .appName("StockDataPipeline")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
    .getOrCreate()
)
```

- **Hadoop AWS S3A Connector**: Enables Spark DataFrame Readers and Writers to directly access `s3a://` URIs.
- **Dual Format Writers**: Every Silver and Gold dataset is stored in both **Parquet** (columnar format for analytics engines) and **CSV** (with headers for inspection and external tools).

---

## 5. Phase 1: Ingestion & Bronze Layer (Raw Source Truth)

### Architectural Directive
Land raw API responses as **immutable, untransformed JSON files** directly in S3.

### API Ingestion & Rate-Limit Management
1. **APIKeyManager (`utils.py`)**: Pools up to 16 API keys (`ALPHA_VANTAGE_API_KEY_1` ... `15`). `get_key()` randomly selects from the active key pool to balance rate limits across keys.
2. **StockDataLoader (`load.py`)**: Uses `boto3.client("s3")` to upload JSON payloads to S3 keys:

```text
Bronze S3 Partition Format:
s3a://graywolf--data--lake/stock/bronze/
    source={source}/
    dataset={dataset}/
    year=YYYY/
    month=MM/
    day=DD/
    hour=HH/
    minute=MM/
    {SYMBOL}.json
```

---

## 6. Phase 2: Transformation & Silver Layer (Trusted Data)

### Explicit PySpark StructType Schemas (`extract.py`)
To prevent expensive runtime schema inference overhead over thousands of S3 JSON files, `extract.py` enforces explicit `StructType` Spark schemas:

```python
_ohlcv_fields = StructType([
    StructField("1. open", StringType(), True),
    StructField("2. high", StringType(), True),
    StructField("3. low", StringType(), True),
    StructField("4. close", StringType(), True),
    StructField("5. volume", StringType(), True),
])

stock_schema_daily = StructType([
    StructField("Meta Data", StructType([
        StructField("2. Symbol", StringType(), True),
        StructField("3. Last Refreshed", StringType(), True),
    ]), True),
    StructField("Time Series (Daily)", MapType(StringType(), _ohlcv_fields), True),
])
```

### Transformation Sequence Summary Matrix

| Dataset Module | Input Raw Source | Step Count | Primary Business Key | Key Pipeline Operations | Output Columns |
|---|---|---|---|---|---|
| **`daily.py`** | `Time Series (Daily)` JSON map | **19 Steps** | `(symbol, day_date)` | JSON explode → cast types → 14 QA rules → filter valid → dedup → round → 30-day avg → 52-week range → all-time range → window lags | **27 Columns** |
| **`weekly.py`** | `Weekly Time Series` JSON map | **15 Steps** | `(symbol, week_date)` | JSON explode → cast types → 14 QA rules → filter valid → dedup → round → metric enrichment → `weekofyear` partitions | **17 Columns** |
| **`overview.py`** | `OVERVIEW` JSON object | **10 Steps** | `symbol` | Select 52 fields → PascalCase to snake_case map → regex cleaning → fake-null map → default fill → explicit type casting (6 Int, 6 Long, 28 Double, 3 Date) | **57 Columns** |
| **`stock_tickers.py`** | Finnhub Stock Array JSON | **12 Steps** | `(symbol, mic)` | Schema drift guard → string cast → trim → fake-null map → dedup → upper/initcap → 5 QA rules → `concat_ws` validation reason → snake_case rename | **13 Columns** |
| **`exchanges.py`** | Polygon Exchange Array JSON | **9 Steps** | `id` | Select 7 fields → cast `id` to Long → trim → fake-null map → 7 non-null QA rules → `concat_ws` validation reason → casing + `mic` alias | **10 Columns** |

---

## 7. Phase 3: Unification & Gold Layer (Business Analytics)

Implemented in `StockPipeline._build_gold_layer()` inside `src/stock_pipeline/pipeline.py`:

```text
                     Silver daily_time_series
                                │
                                ├── LEFT JOIN ON 'symbol'
                                ▼
                     Silver company_overview
                                │
                                ▼
                    Gold Unified Dataset
                      (company_dataset)
```

1. **Extraction**: `StockDataExtractor` reads `daily_time_series` (Parquet and CSV) and `company_overview` (Parquet and CSV).
2. **Column Projection**:
   - `daily_time_series`: Selects `symbol`, `day_date`, `open`, `high`, `low`, `close`, `volume`, `daily_change`, `daily_change_percentage`, `market_movement`, `thirty_day_avg_open`, `thirty_day_avg_close`, `all_time_high`, `all_time_low`, `processed_at`.
   - `company_overview`: Drops `processed_at` to avoid join collisions.
3. **Left Join**: Executes `daily_parquet.join(overview_parquet, on="symbol", how="left")`.
4. **Dual Output Storage**: Writes joined output to S3:

```text
s3a://graywolf--data--lake/stock/gold/
    source=alphavantage/
    dataset=company_dataset/
    year=YYYY/month=MM/day=DD/hour=HH/minute=MM/
    format={parquet|csv}/
```

---

## 8. Incremental Processing & Watermark Engine (`WatermarkManager`)

Managed by `src/stock_pipeline/watermark/manager.py`:

### Hadoop FileSystem JVM Gateway Integration
To avoid costly boto3 `list_objects` calls or PySpark `FileNotFoundException` crashes during existence checks, `WatermarkManager` executes atomic existence checks using Spark's JVM Gateway:

```python
gateway = self.spark.sparkContext._jvm
hadoop_path = gateway.org.apache.hadoop.fs.Path(path_str)
conf = self.spark.sparkContext._jsc.hadoopConfiguration()
file_system = gateway.org.apache.hadoop.fs.FileSystem.get(hadoop_path.toUri(), conf)
return file_system.exists(hadoop_path)
```

### Watermark JSON Catalog Schema (`s3a://graywolf--data--lake/watermark/{pipeline_name}/{dataset_name}.json`)
```json
{
  "pipeline_name": "bronze_to_silver",
  "dataset_name": "daily_time_series",
  "watermark_column": "day_date",
  "watermark_value": "2026-08-14",
  "last_processed_at": "2026-08-14T14:00:00+00:00",
  "batch_id": "batch_20260814_140000",
  "status": "SUCCESS",
  "updated_at": "2026-08-14T14:01:30+00:00",
  "updated_by": "stock_pipeline",
  "remarks": "Bronze to Silver completed successfully."
}
```

---

## 9. Data Quality Audit Engine & Rule Matrix

| Dataset | Rule # | Target Column | Assertion Condition | Action / Tag |
|---|---|---|---|---|
| **`daily_time_series`** | 1-8 | Essential Fields | Must not be `NULL` | Tag `INVALID` |
| **`daily_time_series`** | 9-12 | Prices (`open`, `high`, `low`, `close`) | Must be strictly positive `> 0` | Tag `INVALID` |
| **`daily_time_series`** | 13 | Volume | Must be non-negative `>= 0` | Tag `INVALID` |
| **`daily_time_series`** | 14 | High vs Low | `high >= low` | Tag `INVALID` |
| **`weekly_time_series`** | 1-14 | Same as Daily | Same as Daily | Tag `INVALID` |
| **`ticker_reference`** | 1-5 | `currency`, `description`, `displaySymbol`, `symbol`, `type` | Must not be `NULL` | Tag `INVALID` |
| **`exchanges`** | 1-7 | `asset_class`, `id`, `locale`, `name`, `operating_mic`, `participant_id`, `type` | Must not be `NULL` | Tag `INVALID` |

---

## 10. Operations & Execution Guide (Step-by-Step Run Book)

### Step 1: Environment Setup & Credentials
Create a `.env` file in the root directory:

```env
# AWS Credentials & S3 Data Lake Target Bucket
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
S3_BUCKET_NAME=graywolf--data--lake

# Alpha Vantage API Key Pool (Up to 16 keys)
ALPHA_VANTAGE_API_KEY=key_1
ALPHA_VANTAGE_API_KEY_1=key_2
ALPHA_VANTAGE_API_KEY_2=key_3
```

### Step 2: Execute End-to-End Pipeline via Local CLI
Run the main module directly using Python:

```bash
# Run local CLI pipeline execution (defaults to ["IBM"] symbol)
python -m src.stock_pipeline.app
```

### Step 3: Run Pipeline with Custom Symbols Payload
Pass custom stock tickers directly into `StockPipeline.run()`:

```python
from datetime import datetime, timezone
from src.stock_pipeline.pipeline import StockPipeline

pipeline = StockPipeline()
results = pipeline.run(
    execution_start_time=datetime.now(timezone.utc),
    stock_symbols=["IBM", "AAPL", "GOOGL", "MSFT"]
)
```

### Step 4: Deploy & Invoke via AWS SAM (Lambda Mode)

```bash
# Build AWS SAM application
sam build

# Deploy to AWS Cloud
sam deploy --guided

# Trigger Lambda function with custom event payload
aws lambda invoke \
  --function-name StockDataPipelineFunction \
  --payload '{"stock_symbols": ["IBM", "AAPL", "NVDA"]}' \
  response.json
```

---

## 11. Error Handling, Logging Tags & Resilience Scenarios

### Standardized Log Tag Convention
Every log statement throughout the pipeline adheres to a structured bracket prefixing convention:

- `[INIT][SPARK]`: Spark session creation & Hadoop AWS initialization
- `[PIPELINE][INIT]`: Orchestrator dependency setup
- `[INGESTION][ALPHAVANTAGE]`: External API HTTP fetching
- `[ALPHAVANTAGE][DAILY]`: Daily time series PySpark transformation
- `[ALPHAVANTAGE][OVERVIEW]`: Company overview PySpark transformation
- `[FINNHUB][TICKERS]`: Finnhub stock reference transformation
- `[MASSIVE][EXCHANGES]`: Polygon exchange metadata transformation
- `[WRITE][PARQUET]` / `[WRITE][CSV]`: Storage load operations
- `[GOLD]`: Gold layer build & join operations

---

## 12. PySpark Execution Mechanics & Optimization Principles

1. **Lazy Evaluation**: Spark transformations (maps, filters, joins) build an execution DAG and only execute when an action (`count()`, `write()`) is triggered.
2. **Explicit StructType Projection**: Reading Bronze JSON files with predefined PySpark schemas avoids dynamic schema inference tasks across distributed S3 objects.
3. **Deterministic Business Key Windowing**: Using `Window.partitionBy(business_key).orderBy(ts.desc())` ensures exact latest-wins deduplication without data loss.
4. **Hadoop S3A Commit Protocol**: `_write_parquet` and `_write_csv` leverage Hadoop S3A file committers for partition writes directly to S3.
