# Data Engineering Journey Tracker & Roadmap

> **Purpose**: Master tracker of the whole data engineering journey — what was learned, what was built, what is next. Career goal: **Data Engineer, job-readiness focused**. Stack: **AWS + Databricks**. Commitment: **2 hours/day**.
> **Companion**: `databricks_stock_market_platform.md` (technical state of the pipeline), `INDEX.md` (all notes).
> **Last updated**: 2026-10-22

---

## 1. The Journey So Far (timeline)

| Date | Milestone |
| --- | --- |
| 2026 (early) | Python, SQL, PySpark foundations — Spark bootcamp, Delta Lake, read modes, data fetching notebooks |
| 2026-10-08 | Workspace reorganized; all learning + projects pushed to GitHub (`Data-Engineering` repo) |
| 2026-09-30 | Naming conventions aligned (pipelines `_pipeline`, datasets short descriptors) |
| 2026-10-10 | ETL restructured into step notebooks under `stock-pipeline/etl/` (00_setup → 04_ops) |
| 2026-10-12 | Incremental bronze ingestion: `etl.pipeline_watermarks` table, 5-day overlap for `lag()`, conditional initial load + Delta MERGE into Silver |
| 2026-10-13 | `watermark_manager` utility (atomic Volume checkpoint JSON); build_gold wired to `read_watermark()` |
| 2026-10-16 | Write-mode fixes (overwrite→MERGE on silver/gold), notebook naming convention finalized, **`load_warehouse` orchestrator** loads full star schema (dim_company, dim_date, fact_daily_stock, fact_company_fundamentals) with validation |
| 2026-10-22 | `Data-Engineering` repo cloned as the project "brain"; master notes written (platform, Delta write modes, this tracker) |

**Journey arc**: foundations → AWS infra (S3, Lambda, SAM, S3 replication) → ingestion (Alpha Vantage) → PySpark transforms → Medallion lakehouse (Bronze/Silver/Gold) → incremental reliability (watermarks, MERGE) → Databricks + Unity Catalog → **star schema warehouse built** → next: productionize + analytics.

---

## 2. ACTUAL Status vs the Old Roadmap

The Oct 12–13-era roadmap listed "finish dim_date, dim_stock, fact table" as the next step. **That work is already done** (as of 2026-10-16):

* ✅ `dim_company` (surrogate `company_key`, MERGE on symbol) — the roadmap's `dim_stock`
* ✅ `dim_date` (21-column calendar schema, MERGE on date_key) — the roadmap's `Date_SK` design
* ✅ `fact_daily_stock` (MERGE on company_key+date_key; verified columns: symbol, trade_date, company_key, date_key, open, high, low, close, previous_day_close, previous_day_open, volume, daily_change, market_movement)
* ✅ `fact_company_fundamentals` (MERGE on symbol+snapshot_date)
* ✅ Watermarks + audit logging (`etl.pipeline_watermarks`, append per run)
* ✅ `load_warehouse` production orchestrator with built-in validation (required columns, duplicate keys, dimension key resolution, row counts)

**Still open** (carried from the master note):

1. Weekend gap → `previous_day_close = null` on first Monday of a batch (silver-seeding fix not implemented)
2. `dim_company` `GENERATED ALWAYS AS IDENTITY` → stale `company_key` in historical `fact_company_fundamentals` rows
3. Hardcoded AWS credentials in `redshift_warehouse_pipeline` — rotate + move to secrets scope
4. Rerun/failure-recovery tests not yet formally executed (MERGE makes loads idempotent in principle — prove it)
5. No scheduling/monitoring yet — everything runs manually
6. No analytical SQL series or dashboard yet

---

## 3. Topic-Wise Progress (condensed — 107 topics across 11 phases)

| Phase | Topics | Status |
| --- | --- | --- |
| 1. Python & DE foundations | Python, OOP, exceptions, SQL, Git, Linux, JSON/CSV/Parquet | Covered / review |
| 2. AWS & cloud | IAM, S3 (+ replication configured), Lambda, SAM, EC2/SSM, CloudWatch | Mostly implemented; IAM pipeline permissions to validate |
| 3. Ingestion | REST APIs, Alpha Vantage (daily + overview) done; multi-source (Finnhub, Massive, Twelve Data) upcoming; rate-limit handling to validate | Core done |
| 4. Spark & PySpark | DataFrames, schemas (StructType), transformations, nested JSON — implemented; execution plans, shuffle, repartition vs coalesce, data skew, AQE — covered; **join strategies documented in `spark_join_strategies.md`** — practice criteria pending | Optimization deep in progress |
| 5. Lake, Delta & Medallion | Bronze→Silver→Gold implemented; dedup + validation implemented; Delta MERGE in progress→working; partitioning/compaction, time travel — upcoming | Core done |
| 6. Incremental & reliability | Watermark design implemented; processing logs implemented; retry/recovery, idempotency proof, late-arriving data — pending | In progress |
| 7. Databricks | Workspace/notebooks/dbutils/UC covered; volumes in progress (checkpoints); Jobs, scheduling, monitoring — upcoming | Partial |
| 8. Warehousing & dimensional modeling | **Core COMPLETED** + star schema BUILT; advanced topics now documented in `advanced_data_warehouse_topics.md` — mastery via its practice checklist pending | Core done, advanced documented |
| 9. Glue/Athena/Redshift | Glue catalog/crawlers explored; Athena, Lake Formation planned; Redshift conceptually covered | Explored |
| 10. Orchestration & streaming | Databricks Jobs pending; Kafka/Confluent practiced (crypto-streaming-platform); Airflow, Terraform, CI/CD planned | Upcoming |
| 11. Analytics & capstone | Returns, moving averages, volatility SQL, dashboard, portfolio — upcoming | Upcoming |

---

## 4. Prioritized 10-Week Roadmap (job readiness, 2h/day, AWS + Databricks) — adjusted to actual state

| Week | Focus | Status / adjustment |
| --- | --- | --- |
| 1 | Gold star schema (dims + fact) | ✅ **DONE** (2026-10-16, `load_warehouse` in production) |
| 2 | Incremental ETL & reliability (watermarks, MERGE, processing logs) | ✅ **Largely done** — remaining: rerun/failure-recovery test + fix Monday `previous_day_close` null |
| 3 | **NEXT: Data validation & reliability proof** — run the two known-issue fixes, rerun/duplication tests, reconcile counts silver↔gold↔warehouse, rotate credentials | Start here |
| 4 | Productionize: Databricks Job for the full chain (ingest → silver → gold → warehouse), retries, alerts, email notification | |
| 5 | SQL for interviews: window functions (LAG/LEAD/ROW_NUMBER), CTEs, conditional aggregation — 20 problems from your own Gold/warehouse tables | |
| 6 | Spark internals & optimization: explain() plans, shuffles, broadcast joins, caching — diagnose one real query | |
| 7 | AWS + Databricks architecture review: IAM least privilege, S3 layout, UC governance; fix file-event ingestion config issue | |
| 8 | Data quality & performance: automated checks, reconciliation, partition pruning, small files | |
| 9 | CI/CD & deployment: unit/integration tests (pytest against transform modules — repo already has `tests/`), GitHub Actions, README/architecture diagram | |
| 10 | Interview prep & capstone: 5-min architecture walkthrough, mock interviews, dashboard (AI/BI) as portfolio deliverable | |

**Rule going forward**: no new data sources, no Kafka, no new stacks until the current pipeline is scheduled, validated, and queryable end-to-end.

**Learning focus update (2026-10-22, post-DW checklist)**: DW fundamentals + core dimensional modeling are **completed** — do not repeat them. Next learning focus: **Spark join strategies** (broadcast / sort-merge / shuffled hash, skew joins), then the advanced DW topics interleaved into the project (Kimball vs Inmon → aggregates + semantic layer → warehouse testing + security → SCD in incremental pipelines → Data Vault last). Full gap map with priorities: `data_warehouse_topic_checklist.md`.

---

## 5. Immediate Next Steps (Week 3, starting now)

1. **Fix the Monday `previous_day_close = null` issue** (weekend gap in 5-day overlap) — then verify in `warehouse.fact_daily_stock`
2. **Fix stale `company_key`** in `fact_company_fundamentals` (IDENTITY key regeneration on dim_company rebuild)
3. **Prove idempotency**: rerun `load_warehouse` twice → zero duplicates in all 4 tables; reconcile row counts silver → gold → warehouse
4. **Rotate AWS credentials** → Databricks secrets scope (`dbutils.secrets`)
5. **Schedule the chain** as one Databricks Job (ingest → build silver → build gold → load warehouse) with retries + failure alert
6. **Start analytical SQL**: daily returns, 30-day moving averages, volume trends, top movers — against `warehouse.fact_daily_stock`
7. **Build the dashboard** (AI/BI) — the capstone deliverable

---

## 6. Daily Routine (2h/day)

* 30 min — concepts (interview-level explanation of the day's topic)
* 60 min — implement in the real project
* 30 min — test + document one decision into these notes + 2–3 interview questions

## 7. Interview Priority Tiers

* **Tier 1 (must)**: SQL joins/CTEs/window functions, dedup, PySpark DataFrames, incremental loads, dimensional modeling, S3/IAM basics
* **Tier 2 (strong practical)**: Spark execution, Delta Lake/MERGE, watermarks, idempotency, Databricks Jobs, data quality
* **Tier 3 (later)**: Kafka streaming, Airflow, Terraform, Iceberg internals, deep Spark tuning

---

*(Source: user-provided journey/roadmap content from session 2026-10-22, reconciled against assistant workspace memory of actual pipeline state through 2026-10-16.)