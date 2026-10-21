# Data Warehouse — Topic Checklist & Progress

> **Purpose**: Consolidated status of every Data Warehouse topic in the journey — what is covered, what remains, and the recommended learning order. Deep-dive explanations live in `data_warehousing_master.md`; this file is the progress checklist and gap map.
> **Status date**: 2026-10-22

---

## 1. Overall Progress

| Area | Status |
| --- | --- |
| Data Warehouse fundamentals | **Completed** |
| Core Dimensional Modeling | **Completed** |
| Incremental loading & data quality | **Core concepts completed** |
| Advanced warehouse design | **Recommended topics remain** (see §3) |
| Spark optimization | **In progress** — covered: execution plans, shuffle, repartition vs coalesce, data skew, AQE. Next: join strategies |

**Recommendation in force**: keep DW core marked completed; do NOT repeat fundamentals. Continue with Spark join strategies, then pick up the remaining advanced DW topics where they fit naturally into the stock-market project.

---

## 2. Covered Topic Checklist

### 2.1 Data Warehouse fundamentals — Completed

What a DWH is and why organizations need one · OLTP vs OLAP · operational vs analytical databases · Data Lake vs Data Warehouse vs **Data Lakehouse** · modern data architecture · Enterprise DW (EDW) · data marts · staging/core/presentation/**semantic** layers · transient vs persistent staging · **ETL vs ELT**

*(Deep-dive: `data_warehousing_master.md` §1–§2 — OLTP/OLAP, lakehouse positioning.)*

### 2.2 Dimensional modeling & schema design — Core completed

Dimensional modeling fundamentals · star schema · facts vs dimensions · **choosing the grain** · natural/business/surrogate keys · fact-to-dimension relationships · **conformed dimensions** · **role-playing dimensions** · **degenerate dimensions** · **junk dimensions** · **bridge tables** (many-to-many) · dimension hierarchies · **fan-out / double counting** · unknown members · **inferred members** · **late-arriving dimensions** · **late-arriving facts**

*(Deep-dive: master note §3–§6 covers star/snowflake, grain, surrogate vs business keys, conformed + degenerate. Role-playing, junk, bridge, inferred/late-arriving members are covered at concept level — revisit in project when multi-source enrichment lands.)*

### 2.3 Fact tables & measures — Completed

Transaction facts · periodic snapshots · accumulating snapshots · **factless facts** · **aggregate facts** · additive / semi-additive / non-additive measures · derived & calculated measures · NULL handling in facts · **MTD / QTD / YTD** · snapshot balances vs transaction amounts

*(Deep-dive: master note §4 — our `fact_daily_stock` = daily-grain transaction-style fact; `fact_company_fundamentals` = periodic snapshot. Project example of the rule: one row per symbol per trading day; **volume is additive, closing price is not**.)*

### 2.4 Slowly Changing Dimensions — Completed

Why dimensions change · SCD 0 / 1 / 2 / 3 · effective start & end dates · current-record flags · historical tracking with surrogate keys

*(Deep-dive: master note §10 — includes a Delta `MERGE` SCD2 implementation and our snapshot-fact alternative.)*

### 2.5 Data loading & incremental processing — Completed

Full vs incremental load · initial load & backfill · **watermarks / high-water marks** · **composite watermarks** `(updated_at, id)` · **CDC** · insert/update/delete handling · upsert & `MERGE` · deduplication · latest-record selection with window functions · **idempotent pipelines** · retry-safe processing · checkpoints & processing state

*(Deep-dive: master note §11 — our watermark → MERGE → audit pattern; `databricks_stock_market_platform.md` §5–§6 for implementation.)*

### 2.6 Data quality & reconciliation — Completed

Completeness · uniqueness · validity · accuracy · consistency · timeliness · schema + data validation · source-to-target reconciliation · record-count comparison · financial total comparison · bad-record handling · **quarantine**

*(Project evidence: `load_warehouse` validations — required columns, duplicate keys, dimension key resolution, row counts; `quarantine.daily_stock_invalid`.)*

### 2.7 Storage & query performance fundamentals — Core completed

**Data partitioning** · **partition pruning** · Spark execution partitions vs storage partitions · **small-files problem** · Parquet · row vs columnar storage · compression · **column pruning** · **predicate pushdown** · file organization for analytical queries

*(Roadmap Week 8 applies this: partitioning the warehouse facts, compaction.)*

### 2.8 Practical warehouse designs — Completed

Stock-market DW · **banking DW** (transaction facts vs daily balance snapshots, loan application lifecycle, loan payment facts, account-opening events) · conformed dims across facts · **avoiding fact-to-fact joins that double-count** · selecting grain from business questions

### 2.9 End-to-end DW pipeline — Core completed

ETL · Bronze/Silver/Gold · incremental transformation · cleansing & standardization · dedupe-before-merge · historical handling · validation before publish · reconciliation after load · partitioned analytical storage · the stock-data flow

*(Project evidence: `stock_market` catalog pipeline, `load_warehouse` orchestrator.)*

---

## 3. Advanced Topics — Remaining Gaps (with priorities)

> Deep-dives for every topic below now live in `advanced_data_warehouse_topics.md`. The remaining gap is **mastery via practice** (each deep-dive section ends with a practice criterion) — not documentation.

| Topic | Priority | One-line definition / where it fits |
| --- | --- | --- |
| Kimball vs Inmon methodology | High | Kimball = bottom-up dimensional marts (our approach); Inmon = top-down normalized EDW then dimensional marts. Be able to defend why this project is Kimball |
| Star vs snowflake | High | Covered (master note §3) — review until explainable from memory |
| Data Vault modeling | Medium | Hubs (business keys) + links (relationships) + satellites (attributes) — agile, audit-friendly staging; enterprise alternative to Kimball staging |
| Fact & dimension design methodology | High | The 4-step Kimball process: pick business process → declare grain → identify facts → identify dimensions — apply it verbally to our stock pipeline |
| Surrogate key generation & unknown-member conventions | Medium | Reserve `-1`/`0` "unknown member" rows in dims so fact rows never orphan; revisit our IDENTITY incident (master note §6) |
| Aggregate tables & pre-aggregation | Medium-High | Pre-computed monthly/sector aggregates to speed dashboards — candidate: `warehouse.agg_monthly_stock`
| Semantic layer, metrics & business definitions | High | Single definition of KPIs (what IS "daily return"?) — in Databricks: metric views / Genie space |
| Warehouse workload & query optimization | High | Query profiles, materialized views, result caching; measure before optimizing |
| Data retention, archival & backfills | Medium | Time travel + VACUUM policy, cold-data archival to cheaper storage |
| Warehouse security & access control | High | Unity Catalog grants: schema/table-level ACLs, row filters/column masks for PII-like data |
| SCD in incremental pipelines | High | Combine CDC + SCD2 MERGE — the master note §10 query is the starting point |
| Warehouse testing & deployment practices | High | pytest for transform modules (repo `tests/`), reconciliation suites, CI/CD gates |

---

## 4. Where the Gaps Fit in the Stock-Market Project

1. **Semantic layer** → define `daily_return`, `avg_volume_30d`, `top_gainer` once (metric view), reuse in dashboard + queries
2. **Aggregate tables** → `agg_monthly_stock` per symbol (avg close, total volume) feeding the dashboard
3. **Warehouse security** → grant analysts read on `warehouse.*` only, keep `bronze`/`etl` private
4. **SCD2 + CDC** → if company fundamentals history becomes a requirement beyond `snapshot_date` facts
5. **Testing** → turn `load_warehouse` validations into pytest + reconciliation suite (Week 8–9 roadmap)
6. **Kimball 4-step + unknown-member** → add `-1` unknown rows to `dim_company`/`dim_date` and rehearse the methodology verbally

---

## 5. Recommended Learning Order From Here

1. **Spark join strategies** (broadcast, sort-merge, shuffled hash; skew join handling) — **NEXT**
2. Advanced DW topics, interleaved: Kimball vs Inmon → aggregates & semantic layer → warehouse testing & security → SCD in incremental pipelines → Data Vault (last)
3. Do not re-study DW fundamentals or core dimensional modeling — they are completed; maintain via interview-question reps (master note §14)

---

*(Source: user's consolidated DW topic checklist from session 2026-10-22, mapped against deep-dive coverage in `data_warehousing_master.md`.)