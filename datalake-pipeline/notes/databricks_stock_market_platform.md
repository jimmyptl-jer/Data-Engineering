# Databricks Stock Market Data Platform — Master Project Notes

> **Purpose**: The Databricks-native evolution of this repo's `datalake-pipeline` (S3/Lambda version). All project knowledge, decisions, conventions, and open issues from our working sessions are recorded here. This repo and the `stock-market-data-platform` project together form the project **"brain"** — every future change gets documented in these notes.
>
> **Last updated**: 2026-10-22
> **Workspace user**: jimmyptl46@gmail.com
> **Cloud**: AWS (Databricks, serverless compute / Spark Connect)

---

## 1. Repository Roles (the "Brain")

| Repo / project | Role | Change policy |
| --- | --- | --- |
| `Data-Engineering` (this repo, cloned at `/Users/jimmyptl46@gmail.com/stock-pipeline/Data-Engineering`) | Notes & specifications, AWS S3 lakehouse pipeline (`datalake-pipeline/`), YouTube ETL (`youtube_pipeline/`) | All notes and spec changes land here |
| `stock-market-data-platform` (Databricks project at `/Users/jimmyptl46@gmail.com/stock-pipeline/stock-market-data-platform`) | Production Databricks pipeline (Unity Catalog + Delta) | **Never edited unless the user explicitly asks** |

---

## 2. Project Location & Layout

Root: `/Users/jimmyptl46@gmail.com/stock-pipeline/stock-market-data-platform/etl/`

```text
etl/
├── 00_setup/
│   └── create_tables            # catalog / schema / table creation
├── 01_bronze/
│   ├── ingest_daily_market_data           # S3 JSON → bronze tables (incremental, watermark-aware)
│   └── ingest_company_overview_data       # Alpha Vantage OVERVIEW → bronze
├── 02_silver/
│   ├── build_silver_daily_stock           # clean/validate/enrich → silver + quarantine
│   └── build_silver_company_overview      # company fundamentals → silver
├── 03_gold/
│   ├── build_gold_stock_enriched          # daily LEFT JOIN overview → gold
│   ├── load_dim_company
│   ├── load_dim_date
│   ├── load_fact_daily_stock
│   ├── load_warehouse            # PRODUCTION orchestrator — loads all 4 warehouse tables
│   ├── redshift_warehouse_pipeline        # REFERENCE code version only — not production
│   └── warehouse_pipeline_documentation.md
├── 04_ops/
│   ├── watermark_manager         # %run-imported utility module
│   └── quality_checks
└── utils/
    ├── dedupe
    └── run_context
```

---

## 3. Unity Catalog Layout

* Catalog: `stock_market`
* Schemas: `bronze`, `silver`, `gold`, `warehouse`, `etl`, `quarantine`
* Key tables: `bronze.daily_stock_extracted`, `bronze.company_overview_data`, `silver.daily_stock_data`, `silver.company_overview_data`, `gold.stock_enriched`, `warehouse.dim_company` / `dim_date` / `fact_daily_stock` / `fact_company_fundamentals`, `etl.pipeline_watermarks`, `quarantine.daily_stock_invalid`

---

## 4. Naming Conventions

Aligned 2026-09-30, refined 2026-10-16:

* **Notebooks**: `{verb}_{subject}` — verb matches layer role: `ingest_*` (bronze), `build_silver_*` (silver), `build_gold_*` / `load_*` (gold)
* **Pipelines**: `_pipeline` suffix (e.g. `alpha_vantage_daily_stock_pipeline`)
* **Datasets**: short descriptors — `daily_time_series`, `company_overview` (never repeat the pipeline name)
* Applied renames: `ingest_daily_market_data_bronze` → `ingest_daily_market_data`; `load_company_overview_data` → `ingest_company_overview_data`; `build_silver` → `build_silver_daily_stock`; `build_gold` → `build_gold_stock_enriched`; `load_fact` → `load_fact_daily_stock`; `stock_data_warehouse_pipeline` → `load_warehouse_pipeline` → (later) `redshift_warehouse_pipeline`

---

## 5. Watermark & Incremental Processing

* **Watermark table** `etl.pipeline_watermarks` (T_WATERMARK): `pipeline_name, source_name, run_id, watermark_value (STRING), status, records_read, records_written, started_at, completed_at, error_message` — one appended row per run (audit log)
* **`04_ops/watermark_manager`** (imported via `%run ../04_ops/watermark_manager`):
  * `get_checkpoint_path()`, `read_watermark()`, `write_watermark()`
  * Checkpoint JSON stored under `/Volumes/{catalog}/etl/checkpoints/{pipeline_name}_{source_name}_watermark.json`
  * `write_watermark` uses atomic temp-file + `os.replace` for crash safety
* **Incremental filter** (01_bronze): `trade_date > date_sub(last_checkpoint, 5)` — 5-day overlap so window `lag()` can compute `previous_day_close` within a batch
* `PIPELINE_NAME` in build_gold = `DailySilverToGoldStockDataPipeline` (fixed; was wrongly `DailySilverToBronze...`)
* **Known limitation**: the 5-day overlap does not account for weekends — the first Monday in a batch gets `previous_day_close = null`. A Silver-seeding approach would fix it (not yet implemented)

---

## 6. Write Mode Rules (implemented 2026-10-16)

Layer-based policy:

* **Overwrite** — bronze raw tables (full re-ingest from S3 each run) and quarantine snapshots
* **Append** — watermark / audit log (`etl.pipeline_watermarks`): each run is a new event
* **MERGE (upsert)** — silver tables, gold enriched, warehouse dims/facts (conditional initial load + upsert)

| Notebook | Table | Mode |
| --- | --- | --- |
| ingest_daily_market_data | bronze.daily_stock_extracted | overwrite |
| ingest_company_overview_data | bronze.company_overview_data | overwrite (fixed from append — was duplicating rows) |
| build_silver_daily_stock | silver.daily_stock_data | MERGE on (symbol, trade_date) |
| build_silver_daily_stock | quarantine.daily_stock_invalid | overwrite |
| build_silver_daily_stock | etl.pipeline_watermarks | append |
| build_silver_company_overview | silver.company_overview_data | MERGE on symbol (fixed from overwrite) |
| build_gold_stock_enriched | gold.stock_enriched | MERGE on (symbol, trade_date) (fixed from overwrite) |
| load_warehouse | warehouse.dim_company | MERGE on symbol |
| load_warehouse | warehouse.dim_date | MERGE on date_key |
| load_warehouse | warehouse.fact_daily_stock | MERGE on (company_key, date_key) |
| load_warehouse | warehouse.fact_company_fundamentals | MERGE on (symbol, snapshot_date) |

See `databricks_delta_write_modes.md` for the reasoning and Delta schema-control details.

---

## 7. Warehouse Layer (Star Schema in Delta)

* **`load_warehouse`** is the single production orchestrator (replaces the individual dim/fact notebook tasks):
  * Reads `stock_market.gold.stock_enriched` → writes schema `stock_market.warehouse`
  * `dim_company` (MERGE on symbol), `dim_date` (MERGE on date_key, 21-column schema), `fact_daily_stock`, `fact_company_fundamentals`
  * Built-in validation: required column checks, duplicate key detection, dimension key resolution checks, final row counts
* **Known issue**: `dim_company` uses `GENERATED ALWAYS AS IDENTITY`. Rebuilding `dim_company` assigns new `company_key` values, so historical `fact_company_fundamentals` rows may reference stale keys. Not yet fixed.
* `redshift_warehouse_pipeline` notebook is the reference code version — **not** used in production

---

## 8. Security Notes

* `redshift_warehouse_pipeline` contains **hardcoded AWS access keys** in plain text (cell with boto3 Redshift client). Action needed: rotate the key and move it to a Databricks secrets scope / `dbutils.secrets`
* Older October AWS key (`AKIA…` — redacted from this note) and Confluent Kafka credentials were placeholder-ized before an earlier push but should still be rotated as a precaution

---

## 9. Transformation Reference

* `datalake-pipeline/src/stock_pipeline/transform/daily.py` (this repo) documents the 19-step Bronze→Silver daily time-series transform: flatten nested Alpha Vantage JSON → trim → fake-null normalization (`["", "n/a", "na", "null", "none", "-"]`) → type casting → validation status/reason → filter valid → dedupe on (symbol, day_date) → round prices → metric enrichment (daily_change, market_movement Bull/Bear/Neutral) → rolling 30-day avg / 52-week high-low / all-time high-low → partition keys + lag features → final schema. The Databricks `build_silver_daily_stock` notebook is the ported implementation.
* Full step-by-step detail: see `silver_daily_time_series.md`

---

## 10. Environment

* Databricks on **AWS**, **serverless compute** (Spark Connect) — no attached cluster
* Spark SQL warehouse available for SQL editor work

---

## 11. Current Status & Open Items (2026-10-22)

**Working**: bronze ingestion (daily + overview, watermark-aware), silver transforms, gold enrichment, warehouse star schema load, quality checks, watermark/audit logging.

**Open items**:

1. Weekend gap → `previous_day_close = null` on first Monday of batch
2. `dim_company` IDENTITY key staleness vs `fact_company_fundamentals`
3. Rotate hardcoded AWS credentials → secrets scope
4. (Optional) Redshift export path — currently only the reference notebook exists

---

*(Source: compiled from assistant workspace memory of user-directed work, 2026-10-16, plus session on 2026-10-22.)