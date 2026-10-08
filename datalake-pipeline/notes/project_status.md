# Stock Data Lakehouse & Warehouse Pipeline — Master Technical Architecture & Specification

> Last updated: 2026-08-18  
> Document Purpose: Comprehensive System Architecture, Detailed Data Flow Diagrams, Inventory of Completed Components, DDL Schemas, and Step-by-Step Technical Roadmap.

---

## 🏗️ 1. Ultra-Detailed End-to-End System Flow Diagram

```text
===================================================================================================================================
                                          END-TO-END DATA LAKEHOUSE & WAREHOUSE PIPELINE FLOW
===================================================================================================================================

  [ EXTERNAL API SOURCES ]
  ┌─────────────────────────────────┐     ┌─────────────────────────────────┐     ┌─────────────────────────────────┐
  │      Alpha Vantage REST API     │     │        Finnhub REST API         │     │     Massive (Polygon.io) API    │
  │ • Daily Time-Series (OHLCV)     │     │ • Stock Ticker Reference        │     │ • Stock Overview Metadata       │
  │ • Weekly Time-Series            │     │ • Exchange Ticker Reference     │     │ • Exchange Reference Data       │
  │ • Company Overview (52 fields)  │     │ • Market Symbols                │     │ • Aggregates & Dividends        │
  └────────────────┬────────────────┘     └────────────────┬────────────────┘     └────────────────┬────────────────┘
                   │                                       │                                       │
                   └───────────────────────────────────────┼───────────────────────────────────────┘
                                                           │
                                                           ▼
  [ INGESTION ENGINE & ORCHESTRATION LAYER ]
  ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │                                           StockPipeline (pipeline.py)                                           │
  │                                                                                                                 │
  │  1. Watermark State Check: Consult WatermarkManager (manager.py) via JVM Hadoop FS                              │
  │     └─► Read S3 JSON: s3a://graywolf--data--lake/watermark/{pipeline}/{dataset}.json                            │
  │     └─► Determine high-watermark key & execution batch_id                                                      │
  │                                                                                                                 │
  │  2. API Rate Limiting & Authentication:                                                                        │
  │     ├─► Alpha Vantage: Multi-key rotation algorithm (1-minute cooldown per key)                                 │
  │     ├─► Finnhub: API Key token header authentication                                                            │
  │     └─► Massive: HTTP 429 Rate-Limit detection + exponential backoff retry                                      │
  │                                                                                                                 │
  │  3. Raw Payload Extraction & Persistence:                                                                      │
  │     └─► StockDataLoader (load.py) serializes raw API responses to bytes via boto3                               │
  └────────────────────────────────────────────────┬────────────────────────────────────────────────────────────────┘
                                                   │
                                                   ▼
  [ BRONZE S3 DATA LAKE LAYER ]
  ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │                                    s3a://graywolf--data--lake/stock/bronze/                                     │
  │                                                                                                                 │
  │ Partitioning Hierarchy:                                                                                         │
  │ ├── source=alphavantage/                                                                                        │
  │ │   ├── dataset=daily_time_series/ingestion_date=YYYY-MM-DD/run_id=batch_XXX/data.json                        │
  │ │   ├── dataset=weekly_time_series/ingestion_date=YYYY-MM-DD/run_id=batch_XXX/data.json                       │
  │ │   └── dataset=company_overview/ingestion_date=YYYY-MM-DD/run_id=batch_XXX/data.json                         │
  │ ├── source=finnhub/                                                                                             │
  │ │   └── dataset=ticker_reference/ingestion_date=YYYY-MM-DD/run_id=batch_XXX/data.json                          │
  │ └── source=massive/                                                                                             │
  │     ├── dataset=exchanges/ingestion_date=YYYY-MM-DD/run_id=batch_XXX/data.json                                  │
  │     └── dataset=stock_overview/ingestion_date=YYYY-MM-DD/run_id=batch_XXX/data.json                            │
  └────────────────────────────────────────────────┬────────────────────────────────────────────────────────────────┘
                                                   │
                                                   ▼
  [ SILVER TRANSFORMATION & DATA QUALITY LAYER ]
  ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │                                      PySpark Medallion Transformations                                          │
  │                                                                                                                 │
  │  Step 1: Extract & Read Bronze JSON via Spark Session (extract.py)                                              │
  │  Step 2: Schema Selection & PascalCase -> snake_case Column Mapping                                             │
  │  Step 3: String Trimming & Casing Standardization (UPPER for symbols/MICs, lower for sectors)                   │
  │  Step 4: Fake-Null Normalization: Convert ["", "n/a", "na", "null", "none", "-"] -> PySpark null                   │
  │  Step 5: Explicit Data Type Casting: String -> Double, Long, Integer, Date (yyyy-MM-dd)                         │
  │  Step 6: Data Quality Validation Assertion Engine:                                                              │
  │          Evaluate rules: symbol NOT NULL, date NOT NULL, open/high/low/close > 0, volume >= 0, high >= low         │
  │                                                                                                                 │
  │                       ┌────────────────────────────────┴────────────────────────────────┐                       │
  │                       ▼                                                                 ▼                       │
  │          [ VALID RECORDS (VALID) ]                                         [ INVALID RECORDS (INVALID) ]        │
  │          ├─► Deduplicate: Window row_number = 1                            ├─► Route to Quarantine S3           │
  │          │   by business_key ORDER BY processed_at DESC                    │   s3a://.../stock/quarantine/      │
  │          ├─► Calculate Lag & Metric Features:                              │   Record JSON + Error Manifest     │
  │          │   daily_change, 30d_avg, 52wk_high/low                           │   Reason: MISSING_SYMBOL, etc.     │
  │          └─► Add Metadata: year, month, processed_at                       └──────────────────────────────────┘
  └────────────────────────────────────────────────┬────────────────────────────────────────────────────────────────┘
                                                   │
                                                   ▼
  [ SILVER S3 DATA LAKE STORAGE ]
  ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │                                    s3a://graywolf--data--lake/stock/silver/                                     │
  │                                                                                                                 │
  │ Storage Formats: Parquet & CSV (Delta Lake MERGE Target)                                                         │
  │ Partitioning Layout:                                                                                            │
  │ └── datasource={source}/dataset={dataset}/year=YYYY/month=MM/format={parquet|csv}/                             │
  └────────────────────────────────────────────────┬────────────────────────────────────────────────────────────────┘
                                                   │
                                                   ▼
  [ GOLD BUSINESS MODELING & ENRICHMENT LAYER ]
  ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │                                         PySpark Gold Layer Aggregations                                         │
  │                                                                                                                 │
  │  1. Merged Company Dataset:                                                                                     │
  │     Alpha Vantage Company Overview  LEFT JOIN  Massive Stock Overview ON symbol                                 │
  │                                                                                                                 │
  │  2. Stock Performance Model:                                                                                    │
  │     Calculate daily return %, 7-day & 30-day moving averages, 52-week price channels, 30-day rolling volatility  │
  │                                                                                                                 │
  │  3. Daily Stock Metrics Model:                                                                                  │
  │     Volume change ratios, cross-market capital ranking via dense_rank() OVER (PARTITION BY day_date)            │
  │                                                                                                                 │
  │  4. Delta Lake MERGE Execution:                                                                                 │
  │     MERGE INTO gold_stock_performance AS target USING incoming_silver AS source ON target.symbol = source.symbol  │
  └────────────────────────────────────────────────┬────────────────────────────────────────────────────────────────┘
                                                   │
                                                   ▼
  [ GOLD S3 DATA LAKE STORAGE ]
  ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │                                     s3a://graywolf--data--lake/stock/gold/                                      │
  │                                                                                                                 │
  │ Business Datasets:                                                                                              │
  │ ├── company_dataset/                                                                                            │
  │ ├── stock_performance/                                                                                          │
  │ ├── daily_stock_metrics/                                                                                        │
  │ └── company_growth/                                                                                             │
  └────────────────────────┬───────────────────────────────────────────────────────────────┬────────────────────────┘
                           │                                                               │
                           ▼                                                               ▼
  [ AWS REDSHIFT DATA WAREHOUSE ]                                                [ SNOWFLAKE DATA WAREHOUSE ]
  ┌──────────────────────────────────────────────┐                               ┌──────────────────────────────────┐
  │  Kimball Star Schema OLAP Warehouse          │                               │  Cloud-Agnostic Star Schema DW   │
  │                                              │                               │                                  │
  │  1. Redshift COPY Command:                   │                               │  1. Create External S3 Stage:    │
  │     Ingest Gold Parquet via IAM Role         │                               │     CREATE STAGE s3_gold_stage...  │
  │     into Staging Tables                      │                               │                                  │
  │                                              │                               │  2. COPY INTO Tasks:             │
  │  2. Dimensional Merge Upsert:                │                               │     COPY INTO fact_stock_daily   │
  │     BEGIN TRANSACTION;                       │                               │     FROM @s3_gold_stage          │
  │     DELETE FROM fact_stock_daily USING stage;│                               │     FILE_FORMAT = (PARQUET);     │
  │     INSERT INTO fact_stock_daily SELECT...;  │                               │                                  │
  │     COMMIT;                                  │                               │  3. Star Schema Models:          │
  │                                              │                               │     • fact_stock_daily           │
  │  3. Star Schema Structure:                   │                               │     • dim_company (SCD Type 2)   │
  │     • fact_stock_daily (DISTKEY: sk_company) │                               │     • dim_exchange               │
  │     • dim_company (SCD Type 2)               │                               │     • dim_date                   │
  │     • dim_exchange (SCD Type 1)              │                               └────────────────┬─────────────────┘
  │     • dim_date                               │                                                │
  └────────────────────────┬─────────────────────┘                                                │
                           │                                                                      │
                           └───────────────────────────────────┬──────────────────────────────────┘
                                                               │
                                                               ▼
                                              [ ANALYTICAL BI & DASHBOARDS ]
                                              ├─► AWS QuickSight / Tableau
                                              ├─► Executive Market Performance Dashboards
                                              └─► Risk & Volatility Analytics
===================================================================================================================================
```

---

## 🔄 2. Detailed Ingestion & Watermarking Sequence Diagram

```text
[ Ingestion Client ]           [ WatermarkManager ]             [ External API ]               [ S3 DataLoader ]
        │                              │                              │                              │
        │  1. Check Watermark State    │                              │                              │
        ├─────────────────────────────►│                              │                              │
        │                              │  2. Read S3 JSON             │                              │
        │                              │     (Hadoop FS Check)        │                              │
        │                              ├─────────────────────────────►│ (S3 Bucket)                  │
        │                              │◄─────────────────────────────┤                              │
        │  3. Return High-Watermark    │                              │                              │
        │◄─────────────────────────────┤                              │                              │
        │                              │                              │                              │
        │  4. Ingest Endpoint Payload (Key Rotation / Retry 429)      │                              │
        ├────────────────────────────────────────────────────────────►│                              │
        │◄────────────────────────────────────────────────────────────┤                              │
        │  5. Raw API Response Payload JSON                           │                              │
        │                                                                                            │
        │  6. Persist Raw JSON to S3 Bronze (boto3 Upload)                                           │
        ├───────────────────────────────────────────────────────────────────────────────────────────►│
        │                                                                                            │
        │                                                                                            │  7. Write S3
        │                                                                                            │     bronze/.../data.json
        │  8. Confirm Upload & Batch ID                                                              │◄───────────────
        │◄───────────────────────────────────────────────────────────────────────────────────────────┤
        │                                                                                            │
        │  9. Write Updated Watermark JSON (SUCCESS Status)                                          │
        ├─────────────────────────────►│                                                             │
        │                              │  10. Persist State                                          │
        │                              ├────────────────────────────────────────────────────────────►│
        │                              │                                                             │  11. Write S3
        │                              │                                                             │      watermark/...
```

---

## 🔀 3. Data Quality Validation & Quarantine Flow Diagram

```text
                                        Incoming Bronze PySpark DataFrame
                                                       │
                                                       ▼
                                       Data Quality Assertion Engine
                                                       │
               ┌───────────────────────────────────────┴───────────────────────────────────────┐
               │                                                                               │
               ▼                                                                               ▼
     [ Rule Check: Valid ]                                                           [ Rule Check: Invalid ]
     • symbol IS NOT NULL                                                            • symbol IS NULL OR
     • day_date IS NOT NULL                                                          • day_date IS NULL OR
     • open > 0 AND high > 0 AND low > 0 AND close > 0                               • open <= 0 OR high <= 0 OR
     • volume >= 0                                                                   • high < low OR volume < 0
     • high >= low                                                                             │
               │                                                                               ▼
               ▼                                                             Generate validation_reason via concat_ws:
     Set validation_status = "VALID"                                         "MISSING_SYMBOL, INVALID_HIGH_PRICE"
     Set validation_reason = NULL                                                              │
               │                                                                               ▼
               ▼                                                             Set validation_status = "INVALID"
    Deterministic Window Deduplication                                                         │
    Window.partitionBy("symbol", "day_date")                                                   ▼
          .orderBy("processed_at" DESC)                                               Filter Quarantine Records
               │                                                             (validation_status == "INVALID")
               ▼                                                                               │
    Keep Row Number == 1                                                                       ▼
               │                                                             Serialize to S3 Quarantine Path:
               ▼                                                             s3a://.../stock/quarantine/
     Enrich & Calculate Metrics                                              dataset=daily_time_series/
   (daily_change, 30d_avg, lag)                                              ├── error_records.json
               │                                                             └── quarantine_manifest.json
               ▼
     Write to Silver Layer S3
     (Parquet / Delta Lake)
```

---

## 📋 4. Overall Completion Status Matrix

| Area / Component | Status | Completion % | What is Completed | What Remains to Implement |
| :--- | :---: | :---: | :--- | :--- |
| **API Ingestion** | 🟢 | **~80%** | Alpha Vantage, Finnhub, Massive clients live | Retry backoff, 429 rate-limit handler, cursor pagination |
| **Bronze Layer** | 🟢 | **~90%** | Raw S3 JSON landing with `run_id` partitioning | Schema registry, payload size monitoring |
| **Watermarking System** | 🟢 | **~80%** | `WatermarkManager` S3 JSON CRUD with JVM Hadoop checks | Overview SHA-256 hash comparison, dataset-wide incremental filters |
| **Silver Layer** | 🟡 | **~70%** | Types cast, fake-nulls normalized, `year/month` partition fix | Explicit `StructType` schemas, multi-rule `concat_ws` validation, windowed dedup |
| **Data Quality Framework**| 🟡 | **~50%** | Status/reasons generated per row | Formal S3 Quarantine layer with error manifests |
| **Gold Layer** | 🟡 | **~40%** | Initial merged overview output live | Analytical fact/dim models (`stock_performance`, `daily_stock_metrics`, `company_growth`) |
| **Delta Lake & MERGE** | 🔴 | **0%** | None | Delta table format, ACID transaction logs, `MERGE` upsert operations |
| **API Hardening** | 🔴 | **~20%** | Basic retry handling | Full rate-limit backoff, generic `HardenedApiClient` abstraction |
| **Dimensional Modelling**| 🔴 | **Conceptual**| Star Schema designed | Fact tables (`fact_stock_daily`), Dimension tables (`dim_company` SCD2, `dim_exchange`, `dim_date`) |
| **Redshift DW Loading** | 🔴 | **0%** | None | `COPY` staging from S3, distribution/sort keys, atomic staging upserts |
| **Snowflake DW Loading** | 🔴 | **0%** | None | External S3 Stages, `COPY INTO` tasks, Star Schema DW loading |
| **SCD Type 2 Tracking** | 🔴 | **0%** | None | Slowly changing dimension logic (`effective_from`, `effective_to`, `is_current`) |
| **Orchestration (Airflow)**| 🔴 | **0%** | AWS Lambda cron / Local CLI entry points live | Airflow DAGs, task dependencies, automated backfills |
| **Monitoring & Alerts** | 🟡 | **Basic** | CloudWatch Lambda metrics & logs | Custom metrics (`RecordCountQuarantined`, `Latency`), SNS failure alerts |
| **Testing Suite** | 🔴 | **0%** | None | PySpark `pytest` + `chispa` unit test fixtures & end-to-end integration tests |
| **CI/CD Pipeline** | 🔴 | **0%** | None | GitHub Actions automated linting, testing, and SAM deployment |

---

# SECTION 1: ✅ COMPLETED STEPS & FEATURES

Below is the complete inventory of all steps, components, and fixes that have been built and verified in the codebase.

---

### Step 1: Infrastructure & Core Engine Setup
- ✅ **AWS SAM Infrastructure (`template.yaml`)**: Deployed Lambda function, S3 bucket resources, and CloudWatch alarms.
- ✅ **S3 Bucket Topology (`graywolf--data--lake`)**: Standardized prefixes:
  - `stock/bronze/` — Raw API payloads
  - `stock/silver/` — Standardized, validated data
  - `stock/gold/` — Joined business datasets
  - `watermark/` — Pipeline execution state JSON
- ✅ **PySpark Engine Configuration**: PySpark session with S3A filesystem connector support.
- ✅ **Environment Centralization (`config.py` & `.env`)**: Centralized loading of API keys (`ALPHA_VANTAGE_API_KEY`, `MASSIVE_API_KEY`, `FINNHUB_API_KEY`) and AWS credentials.
- ✅ **Standardized Logging (`app.py`)**: Root logger configured with format `%(levelname)s %(name)s %(message)s`.

---

### Step 2: Bronze Ingestion Layer (Raw API → S3 JSON)
- ✅ **Alpha Vantage Ingestion (`alpha_vantage_ingestion.py`)**: Daily time-series, weekly time-series, and company overview endpoints with multi-API key rotation and 1-minute cooldown.
- ✅ **Finnhub Ingestion (`finnhub_ingestion.py`)**: Stock ticker reference data ingestion.
- ✅ **Massive (Polygon.io) Ingestion (`massive_ingestion.py`)**: Stock overview & exchange reference data ingestion.
- ✅ **S3 Landing Engine (`load.py`)**: `StockDataLoader` writes raw JSON immutably to Bronze S3 with `run_id` and `ingestion_date` partitioning.

---

### Step 3: Silver Transformation Layer (Bronze JSON → Parquet/CSV)
- ✅ **Daily Time-Series Transform (`daily.py`)**: Exploded JSON maps, OHLCV casting, basic validation rules, rolling 30-day average close, 52-week high/low, and `lag()` features.
- ✅ **Weekly Time-Series Transform (`weekly.py`)**: Exploded JSON maps, OHLCV casting, metric enrichment (`weekly_change`, `market_movement`).
- ✅ **Company Overview Transform (`overview.py`)**: Selected 52 business columns, PascalCase → snake_case column mapping, fake-null normalization (`"None"`, `"n/a"`, `"-"` → Spark `null`), explicit type casting, default value imputation (`country="Unknown"`, `sector="Unknown"`).
- ✅ **Exchanges Transform (`exchanges.py`)**: MIC standardization, string trimming, `validation_reason` aggregation via `concat_ws`.
- ✅ **Stock Tickers Transform (`stock_tickers.py`)**: Deduplicated business key `symbol+mic`, snake_case normalization.
- ✅ **Massive Overview Transform (`stock_overview_massive.py`)**: Standalone module for Polygon reference datasets.

---

### Step 4: Initial Gold Layer Join
- ✅ **Merged Company Overview**: Alpha Vantage overview `LEFT JOIN` Massive stock overview on `symbol` inside `pipeline.py`.

---

### Step 5: Watermark State Management System
- ✅ **State Persistence (`watermark/manager.py`)**: `WatermarkManager` manages CRUD for state JSON files on S3.
- ✅ **Atomic FileSystem Checks**: PySpark JVM Gateway (`org.apache.hadoop.fs.FileSystem`) for atomic existence checks on S3 URIs (`s3a://`).

---

### Step 6: Production Hardening Fixes (Completed Today)
- ✅ **Logging TypeError Fix**: Fixed string placeholder mismatch in `pipeline.py` L1613 (4 format placeholders vs 1 argument).
- ✅ **Silver Partitioning Standardization**: Changed Silver write/read partition paths from `year/month/day/hour/minute` to `year/month` in `pipeline.py` (`_write_parquet`/`_write_csv`) and `extract.py` (`_silver_bucket_key`).
- ✅ **Debug Print Cleanup**: Removed ~80 debug `print()`, `df.printSchema()`, and `df.show()` statements from 8 production python files.
- ✅ **Log Tag Standardization**: Updated legacy `[EXTRACT_MASSIVE_DATASET]` tags to standard `[EXTRACT][MASSIVE_BRONZE]`.
- ✅ **Silver Quality Audit**: Documented current state of validation, deduplication, schema enforcement, null handling, and partitioning in [silver_audit.md](file:///home/ec2-user/workspace/data-engineering/datalake-pipeline/notes/silver_audit.md).

---

# SECTION 2: 🔲 REMAINING IMPLEMENTATION STEPS

Below is the detailed specification for all remaining steps required to bring the pipeline to full production-grade data platform maturity and build the OLAP Data Warehouse layer.

---

## 🔴 Step 1: Fix Existing Pipeline Issues (Completed)

*All items under this step were completed today.*

---

## 🟠 Step 2: Finish Silver Layer (Explicit Schemas & Multi-Rule Validation)

### A. Explicit Schema Enforcement
Replace `inferSchema=True` on Bronze reads with explicit PySpark `StructType` schemas to prevent breaking changes when upstream APIs introduce or alter field types.

#### Massive Exchanges Schema Example:
```python
MASSIVE_EXCHANGES_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("asset_class", StringType(), True),
    StructField("locale", StringType(), True),
    StructField("operating_mic", StringType(), True),
    StructField("participant_id", StringType(), True),
])
```

### B. Multi-Error Data Quality Validation
Instead of short-circuiting on the first invalid field, evaluate all validation assertions independently and combine failure reasons using `concat_ws`:

```python
# Multi-rule validation pattern for Silver Daily
stock_df = stock_df.withColumn(
    "validation_status",
    when(
        col("symbol").isNull() |
        col("day_date").isNull() |
        col("open").isNull() | (col("open") <= 0) |
        col("high").isNull() | (col("high") <= 0) |
        col("low").isNull() | (col("low") <= 0) |
        col("close").isNull() | (col("close") <= 0) |
        col("volume").isNull() | (col("volume") < 0) |
        (col("high") < col("low")),
        "INVALID"
    ).otherwise("VALID")
).withColumn(
    "validation_reason",
    concat_ws(
        ", ",
        when(col("symbol").isNull(), "MISSING_SYMBOL"),
        when(col("day_date").isNull(), "MISSING_DAY_DATE"),
        when(col("open").isNull() | (col("open") <= 0), "INVALID_OPEN_PRICE"),
        when(col("high").isNull() | (col("high") <= 0), "INVALID_HIGH_PRICE"),
        when(col("low").isNull() | (col("low") <= 0), "INVALID_LOW_PRICE"),
        when(col("close").isNull() | (col("close") <= 0), "INVALID_CLOSE_PRICE"),
        when(col("volume").isNull() | (col("volume") < 0), "INVALID_VOLUME"),
        when(col("high") < col("low"), "HIGH_LESS_THAN_LOW"),
    )
)
```

---

## 🟠 Step 3: Deterministic Deduplication

Replace non-deterministic PySpark `dropDuplicates()` calls with explicit Window operations ordered by ingestion timestamp / processing timestamp to ensure deterministic execution across cluster runs:

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

# Deduplicate Daily Time-Series keeping latest processed record
window_spec = Window.partitionBy("symbol", "day_date").orderBy(
    col("processed_at").desc(),
    col("last_refreshed_date").desc()
)

valid_stock_df = (
    valid_stock_df
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)
```

---

## 🟠 Step 4: Incremental Processing Strategy

Complete the full incremental loading lifecycle for each dataset using `WatermarkManager`:

```text
                         ┌─────────────────────────────────┐
                         │   Get High-Watermark Value      │
                         │   from S3 Watermark JSON        │
                         └────────────────┬────────────────┘
                                          │
                                          ▼
                         ┌─────────────────────────────────┐
                         │ Filter Incoming Bronze Batch    │
                         │ WHERE record_date > watermark   │
                         └────────────────┬────────────────┘
                                          │
                        ┌─────────────────┴─────────────────┐
                        │                                   │
                        ▼                                   ▼
               ┌─────────────────┐                 ┌─────────────────┐
               │  No New Records │                 │ New Records Exist│
               └────────┬────────┘                 └────────┬────────┘
                        │                                   │
                        ▼                                   ▼
                  Skip Batch                         Process Silver
                                                            │
                                                            ▼
                                                       Write Silver
                                                            │
                                                            ▼
                                                    Update Watermark
```

### Watermark Mapping by Source:
| Dataset | Business Watermark Column | Watermark Value Format |
|---|---|---|
| `daily_time_series` | `day_date` | `YYYY-MM-DD` |
| `weekly_time_series` | `week_date` | `YYYY-MM-DD` |
| `company_overview` | `overview_hash` | SHA-256 Hex Digest |
| `ticker_reference` | `ingestion_date` | `YYYY-MM-DD` |
| `exchanges` | `ingestion_date` | `YYYY-MM-DD` |

---

## 🟠 Step 5: Overview Content-Hash Comparison

Prevent redundant Silver snapshot overwrites by computing a SHA-256 digest over business fields:

```python
from pyspark.sql.functions import sha2, concat_ws, coalesce, lit

# Calculate SHA-256 content digest across business attributes
hash_columns = [
    "symbol", "asset_type", "company_name", "exchange", "currency",
    "country", "sector", "industry", "market_cap", "shares_outstanding"
]

data_df = data_df.withColumn(
    "overview_hash",
    sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("")) for c in hash_columns]), 256)
)
```

If `current_hash == watermark_hash`, skip the Silver rewrite step and log:  
`"[WATERMARK][OVERVIEW] Content hash unchanged (%s). Skipping Silver write."`

---

## 🟠 Step 6: Delta Lake Storage & MERGE Upserts

Transition Silver and Gold layers from static Parquet writes to **Delta Lake** tables to enable ACID transactions, time travel, and `MERGE` upserts.

### PySpark Delta `MERGE` Implementation:
```python
from delta.tables import DeltaTable

# Path to Silver Delta Table
silver_delta_path = f"s3a://{bucket_name}/stock/silver/delta/daily_time_series/"

if DeltaTable.isDeltaTable(spark, silver_delta_path):
    target_table = DeltaTable.forPath(spark, silver_delta_path)
    
    (
        target_table.alias("target")
        .merge(
            source=incoming_silver_df.alias("source"),
            condition="target.symbol = source.symbol AND target.day_date = source.day_date"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    (
        incoming_silver_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month")
        .save(silver_delta_path)
    )
```

---

## 🟠 Step 7: Expanded Gold Business Models & Metric Analytics

Expand the Gold layer into 6 business-ready analytical datasets:

```text
                                ┌─────────────────────────┐
                                │       Silver Layer      │
                                └────────────┬────────────┘
                                             │
      ┌──────────────────┬───────────────────┼───────────────────┬──────────────────┐
      ▼                  ▼                   ▼                   ▼                  ▼
┌───────────┐  ┌──────────────────┐  ┌───────────────┐  ┌─────────────────┐  ┌──────────────┐
│  company  │  │stock_performance │  │  daily_stock  │  │ company_growth  │  │   market     │
│ _dataset  │  │                  │  │   _metrics    │  │                 │  │   _summary   │
└───────────┘  └──────────────────┘  └───────────────┘  └─────────────────┘  └──────────────┘
```

### Calculated Gold Metrics:
1. `stock_performance`:
   - `daily_return_percentage`: `((close - open) / open) * 100`
   - `volatility_30d`: `stddev(close) OVER (PARTITION BY symbol ORDER BY day_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)`
   - `seven_day_moving_avg`: `avg(close) OVER (PARTITION BY symbol ORDER BY day_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)`
   - `thirty_day_moving_avg`: `avg(close) OVER (PARTITION BY symbol ORDER BY day_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)`
2. `daily_stock_metrics`:
   - `volume_change_ratio`: `volume / lag(volume, 1)`
   - `market_cap_rank`: `dense_rank() OVER (PARTITION BY day_date ORDER BY market_cap DESC)`
3. `company_growth`:
   - `revenue_growth_yoy`: Quarterly revenue YoY growth rate comparison.

---

## 🔴 Step 8: Massive API Hardening (Production Ingestion Engine)

Refactor API ingestion into a robust, object-oriented client featuring retry logic, exponential backoff, rate-limit management, and auto-pagination.

```python
import time
import requests

class HardenedApiClient:
    def __init__(self, base_url: str, max_retries: int = 5, backoff_factor: float = 2.0):
        self.base_url = base_url
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def fetch_with_retry(self, endpoint: str, params: dict) -> dict:
        url = f"{self.base_url}/{endpoint}"
        
        for attempt in range(1, self.max_retries + 1):
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                return response.json()
            
            elif response.status_code == 429: # Rate Limited
                retry_after = int(response.headers.get("Retry-After", self.backoff_factor ** attempt))
                logger.warning("[API][RATE_LIMIT] HTTP 429 received. Waiting %d seconds (attempt %d/%d)", retry_after, attempt, self.max_retries)
                time.sleep(retry_after)
                
            else:
                logger.error("[API][ERROR] HTTP %d: %s (attempt %d/%d)", response.status_code, response.text, attempt, self.max_retries)
                time.sleep(self.backoff_factor ** attempt)
                
        raise RuntimeError(f"API request failed after {self.max_retries} attempts: {endpoint}")
```

---

## 🔴 Step 9: Data Quality & Quarantine Subsystem

Route records that fail Silver validation checks to an isolated S3 Quarantine path (`s3a://{bucket}/stock/quarantine/`) with an error tracking manifest:

```text
                                 Incoming Data Batch
                                          │
                                          ▼
                               Data Quality Evaluator
                                          │
                    ┌─────────────────────┴─────────────────────┐
                    ▼                                           ▼
               VALID ROWS                                 INVALID ROWS
                    │                                           │
                    ▼                                           ▼
             Silver Data Lake                           Quarantine Area
           (Parquet / Delta)                       (S3 JSON Error Records)
```

### Quarantine Table Schema (`quarantine_records`):
- `quarantine_id`: UUID
- `dataset_name`: `daily_time_series` / `exchanges`
- `source_system`: `alphavantage` / `massive` / `finnhub`
- `business_key`: `symbol=IBM, day_date=2026-08-18`
- `raw_record_json`: Full string payload
- `failed_rule`: `INVALID_HIGH_PRICE`
- `validation_reason`: `"High price (150.0) cannot be lower than Low price (155.0)"`
- `quarantined_at`: Timestamp
- `run_id`: Execution batch ID

---

## 🔴 Step 10: Data Warehouse Layer & Dimensional Modelling (Kimball Star Schema)

Design and implement a Star Schema for analytical query performance:

```text
                    ┌─────────────────────────┐
                    │       dim_company       │
                    ├─────────────────────────┤
                    │ sk_company (PK)         │
                    │ symbol (NK)             │
                    │ company_name            │
                    │ sector                  │
                    │ industry                │
                    │ effective_from          │
                    │ effective_to            │
                    │ is_current              │
                    └────────────┬────────────┘
                                 │
                                 │ 1:N
                                 ▼
┌──────────────────────┐   ┌─────────────────────────┐   ┌──────────────────────┐
│       dim_date       │   │    fact_stock_daily     │   │     dim_exchange     │
├──────────────────────┤   ├─────────────────────────┤   ├──────────────────────┤
│ sk_date (PK)         ├──►│ sk_date (FK)            │◄──┤ sk_exchange (PK)     │
│ date_actual          │   │ sk_company (FK)         │   │ mic (NK)             │
│ year / month / day   │   │ sk_exchange (FK)        │   │ exchange_name        │
│ is_trading_day       │   │ sk_asset (FK)           │   │ country              │
└──────────────────────┘   │ open_price              │   └──────────────────────┘
                           │ high_price              │
                           │ low_price               │
                           │ close_price             │
                           │ volume                  │
                           │ daily_return            │
                           └─────────────────────────┘
```

### DDL Definitions for Data Warehouse Tables (Redshift / Snowflake):

```sql
-- 1. Date Dimension Table
CREATE TABLE dim_date (
    sk_date         INT PRIMARY KEY,          -- Format: YYYYMMDD
    date_actual     DATE NOT NULL,
    year            INT NOT NULL,
    quarter         INT NOT NULL,
    month           INT NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    day_of_month    INT NOT NULL,
    day_of_week     INT NOT NULL,
    day_name        VARCHAR(20) NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    is_trading_day  BOOLEAN NOT NULL
);

-- 2. Exchange Dimension Table (SCD Type 1)
CREATE TABLE dim_exchange (
    sk_exchange     INT IDENTITY(1,1) PRIMARY KEY,
    mic             VARCHAR(10) NOT NULL UNIQUE,
    exchange_name   VARCHAR(255) NOT NULL,
    participant_id  VARCHAR(10),
    locale          VARCHAR(10) NOT NULL,
    country         VARCHAR(50) DEFAULT 'Unknown',
    currency        VARCHAR(10) DEFAULT 'USD',
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Company Dimension Table (SCD Type 2)
CREATE TABLE dim_company (
    sk_company      BIGINT IDENTITY(1,1) PRIMARY KEY,
    symbol          VARCHAR(20) NOT NULL,
    company_name    VARCHAR(255) NOT NULL,
    sector          VARCHAR(100) DEFAULT 'Unknown',
    industry        VARCHAR(100) DEFAULT 'Unknown',
    market_cap_tier VARCHAR(50),
    country         VARCHAR(50) DEFAULT 'Unknown',
    cik             INT,
    effective_from  TIMESTAMP NOT NULL,
    effective_to    TIMESTAMP,
    is_current      BOOLEAN NOT NULL DEFAULT TRUE
);

-- 4. Fact Stock Daily Table
CREATE TABLE fact_stock_daily (
    fact_id                 BIGINT IDENTITY(1,1) PRIMARY KEY,
    sk_date                 INT NOT NULL REFERENCES dim_date(sk_date),
    sk_company              BIGINT NOT NULL REFERENCES dim_company(sk_company),
    sk_exchange             INT NOT NULL REFERENCES dim_exchange(sk_exchange),
    symbol                  VARCHAR(20) NOT NULL,
    day_date                DATE NOT NULL,
    open_price              NUMERIC(18,4) NOT NULL,
    high_price              NUMERIC(18,4) NOT NULL,
    low_price               NUMERIC(18,4) NOT NULL,
    close_price             NUMERIC(18,4) NOT NULL,
    volume                  BIGINT NOT NULL,
    daily_change            NUMERIC(18,4),
    daily_return_percentage NUMERIC(10,4),
    thirty_day_avg_close    NUMERIC(18,4),
    fifty_two_week_high     NUMERIC(18,4),
    fifty_two_week_low      NUMERIC(18,4),
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## 🔴 Step 11: AWS Redshift Integration

Automate loading Gold Delta/Parquet tables into AWS Redshift OLAP tables:

### Redshift Loading Flow:
1. **Stage**: Export Gold tables to S3 Parquet.
2. **Copy**: Run Redshift `COPY` command to ingest into staging tables:
   ```sql
   COPY staging_fact_stock_daily
   FROM 's3://graywolf--data--lake/stock/gold/fact_stock_daily/'
   IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3Role'
   FORMAT AS PARQUET;
   ```
3. **Merge**: Atomic upsert from staging table into production fact table:
   ```sql
   BEGIN TRANSACTION;
   
   DELETE FROM fact_stock_daily
   USING staging_fact_stock_daily
   WHERE fact_stock_daily.sk_date = staging_fact_stock_daily.sk_date
     AND fact_stock_daily.sk_company = staging_fact_stock_daily.sk_company;
     
   INSERT INTO fact_stock_daily
   SELECT * FROM staging_fact_stock_daily;
   
   END TRANSACTION;
   ```
4. **Optimization**: Set `DISTKEY(sk_company)` and `COMPOUND SORTKEY(sk_date, sk_company)`.

---

## 🔴 Step 12: Snowflake Integration

Implement a parallel cloud data warehouse deployment in Snowflake:

```sql
-- 1. Create External S3 Stage
CREATE OR REPLACE STAGE snowflake_s3_gold_stage
  URL = 's3://graywolf--data--lake/stock/gold/'
  CREDENTIALS = (AWS_KEY_ID = '...' AWS_SECRET_KEY = '...');

-- 2. COPY INTO Fact Table
COPY INTO fact_stock_daily
FROM @snowflake_s3_gold_stage/fact_stock_daily/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

---

## 🟡 Step 13: Apache Airflow Orchestration & Workflow Management

Migrate orchestration from single Lambda invocations to an enterprise Apache Airflow DAG (`stock_lakehouse_pipeline_dag.py`):

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'stock_lakehouse_pipeline',
    default_args=default_args,
    schedule_interval='0 21 * * 1-5', # Weekdays at 9:00 PM UTC
    catchup=False,
) as dag:

    sensor_bronze = S3KeySensor(
        task_id='sense_bronze_landing',
        bucket_key='stock/bronze/*',
        bucket_name='graywolf--data--lake',
    )
    
    # Task execution steps for Bronze -> Silver -> Gold -> Redshift
```

---

## 🟡 Step 14: Observability, Monitoring & Alerting

- **Custom CloudWatch Metrics**:
  - `RecordCountIngested`: Total raw API records ingested.
  - `RecordCountValid`: Total records passing Silver DQ rules.
  - `RecordCountQuarantined`: Total records routed to quarantine.
  - `PipelineLatencySeconds`: Duration of ETL stage execution.
- **SNS Alarms**: Trigger Slack/Email alerts if `RecordCountQuarantined / RecordCountIngested > 0.05` (5% error threshold) or if any task raises an unhandled exception.

---

## 🟡 Step 15: Testing Suite & CI/CD Automation

### A. PySpark Unit Testing (`pytest` + `chispa`)
```python
import pytest
from chispa.dataframe_comparer import assert_df_equality
from src.stock_pipeline.transform.daily import silver_transform_daily_timeseries

def test_daily_transform_valid_input(spark_session):
    input_data = [...] # Sample raw input
    expected_data = [...] # Expected Silver output
    
    input_df = spark_session.createDataFrame(input_data)
    expected_df = spark_session.createDataFrame(expected_data)
    
    result_df = silver_transform_daily_timeseries(spark_session, "daily_time_series", input_df)
    assert_df_equality(result_df, expected_df, ignore_nullable=True)
```

### B. CI/CD GitHub Actions Pipeline (`.github/workflows/pipeline.yml`)
- Trigger on `git push` to `main`.
- Steps: Code Linting (`black`, `flake8`) ➔ PySpark Unit Tests ➔ SAM Build & Deploy.

---

## 🎯 Implementation Sequence Roadmap

```text
 1. Fix Existing Bugs (Logging, Partitioning, Debug Prints)  [✅ COMPLETED]
        │
        ▼
 2. Finish Silver Layer (Explicit Schemas, Multi-Rule Validation)
        │
        ▼
 3. Deterministic Deduplication & Watermark Hardening
        │
        ▼
 4. Delta Lake & MERGE Integration
        │
        ▼
 5. Massive API Hardening (429 Backoff, Pagination)
        │
        ▼
 6. Expand Gold Layer (Stock Performance & Metrics Models)
        │
        ▼
 7. Data Quality Quarantine Framework
        │
        ▼
 8. Dimensional Modelling (Star Schema Design & Fact/Dim Grain)
        │
        ▼
 9. Implement SCD Type 1 & Type 2 Processing
        │
        ▼
10. AWS Redshift Warehouse Loading
        │
        ▼
11. Snowflake Warehouse Loading
        │
        ▼
12. Comprehensive Testing Suite (PySpark Unit & Data Tests)
        │
        ▼
13. Apache Airflow Workflow Orchestration
        │
        ▼
14. Monitoring, Observability & CloudWatch Alerts
        │
        ▼
15. CI/CD Automated Deployment Pipeline
        │
        ▼
16. Production Operations & Continuous Data Platform
```

---

## 📁 Repository Directory Map

```text
src/stock_pipeline/
├── app.py                          # Single entry point (AWS Lambda & Local CLI)
├── config.py                       # Environment variables & runtime constants
├── database.py                     # Database connection placeholders
├── extract.py                      # S3 readers & Spark schema definitions
├── load.py                         # S3 raw JSON uploader (boto3)
├── pipeline.py                     # Main Medallion ETL orchestrator (StockPipeline)
├── utils.py                        # Pipeline utility helpers
├── ingestion/
│   ├── alpha_vantage_ingestion.py  # Alpha Vantage API client & key rotator
│   ├── finnhub_ingestion.py        # Finnhub stock tickers client
│   └── massive_ingestion.py        # Massive (Polygon) API client
├── transform/
│   ├── daily.py                    # Alpha Vantage daily stock transform
│   ├── weekly.py                   # Alpha Vantage weekly stock transform
│   ├── overview.py                 # Alpha Vantage company overview transform
│   ├── exchanges.py                # Massive exchange reference transform
│   ├── stock_tickers.py            # Finnhub ticker reference transform
│   └── stock_overview_massive.py   # Massive stock overview transform
└── watermark/
    ├── config.py                   # Watermark base paths
    ├── manager.py                  # WatermarkManager CRUD & JVM Hadoop path checks
    └── utils.py                    # State management helpers
notes/
├── project_status.md               # Master Project Status & Roadmap (this document)
├── silver_audit.md                 # Silver Layer Quality Audit
├── data_flow_architecture.md       # Data flow technical specifications
├── gold_layer_master.md            # Gold layer architecture
└── schema_handling.md              # Schema management rules
```
