# Advanced Data Warehouse Topics — Deep-Dive Notes

> **Purpose**: Deep-dive on the 12 advanced gap topics from `data_warehouse_topic_checklist.md` §3, grounded in the `stock_market.warehouse` star schema, with Databricks/Delta specifics and interview angles for each. Companions: `data_warehousing_master.md` (core topics), `journey_tracker.md` (roadmap).
>
> **Note**: notes ≠ mastery — each section ends with a practice criterion. Work them into the project.
> **Last updated**: 2026-10-22

---

## 1. Kimball vs Inmon Methodology (High)

The two classic approaches to building a DW:

| Aspect | Kimball (dimensional) | Inmon (CIF) |
| --- | --- | --- |
| Direction | **Bottom-up** — build dimensional marts, integrate via conformed dims | **Top-down** — build a normalized (3NF) enterprise DW, then dimensional marts on top |
| Core artifact | Star schemas + **bus matrix** | Corporate Information Factory, ER-modelled EDW |
| Time to first value | Fast — first mart delivers quickly | Slow — EDW must exist before marts |
| Best fit | Small teams, analytics-first, agile | Large enterprises, single source of truth mandate, heavy governance |
| Weakness | Governance discipline required to keep dims conformed | Slow, expensive, requires strong data modeling team |

**Our project is Kimball**: we went straight to dimensional (Silver as the "staging/integrated" layer, then stars), with conformed `dim_company` / `dim_date` shared by both facts.

*Interview angle*: "Which would you choose and why?" — answer by team size, time-to-value, and governance needs; say Kimball + a disciplined staging layer (Silver) is the pragmatic default; Inmon fits when many source systems must be reconciled under central governance before analytics.

*Practice criterion*: draw our bus matrix (facts × dimensions) from memory and defend the Kimball choice in 2 minutes.

---

## 2. Star vs Snowflake — Extension (High)

Core comparison is in `data_warehousing_master.md` §3. What to add at advanced level:

* **When snowflake is justified**: very large dimensions where denormalized attributes explode row width (e.g., billions of customer rows with full address hierarchy), storage-constrained environments, or compliance models requiring normalized reference data
* **Cost model**: star trades storage (denormalized attributes) for fewer joins at query time; snowflake trades joins for storage — BI tools (and humans) almost always prefer star
* **Hybrid**: star with one snowflamed dimension where only that dimension is huge

*Practice criterion*: explain when you would snowflake `dim_company` (answer: if we joined thousands of attributes from multiple sources into a multi-GB dim with many repeated hierarchies — sector → industry → sub-industry as separate tables).

---

## 3. Data Vault Modeling (Medium)

A third methodology designed for enterprise **staging/auditing**, not final marts:

* **Hubs** — one row per business key + load timestamp: `hub_company(company_key, symbol, load_date)`
* **Links** — relationships between hubs: `link_company_exchange(company_key, exchange_key, load_date)`
* **Satellites** — descriptive attributes, split by source and rate of change, fully historized: `sat_company_overview(symbol, sector, industry, ..., load_date, record_source)`

Strengths: **insert-only** (no updates → auditable, parallel loading, schema-resilient — add a satellite without touching others), ideal for many volatile sources.
Costs: many tables and joins, slow for direct BI — you build **information marts** (stars) on top via views.

**When it fits**: large enterprises ingesting dozens of changing sources with audit requirements. **Not needed** for our single-source stock project (Silver already plays the staging role), but know it exists and why.

*Practice criterion*: sketch how Alpha Vantage + Finnhub + Massive would land in hubs/links/satellites, and why that beats one big Silver table in a multi-source world.

---

## 4. Fact & Dimension Design Methodology — the Kimball 4-Step Process (High)

1. **Select the business process** — what event are we measuring? (ours: daily stock market activity; second process: company fundamentals snapshots)
2. **Declare the grain** — one row per symbol per trading date (before any DDL!)
3. **Identify the facts** — measures true at that grain: open, high, low, close, volume, daily_change
4. **Identify the dimensions** — who/what/when/where context: dim_date (when), dim_company (what)

Applied bus matrix for our warehouse:

| ↓ dims / facts → | fact_daily_stock | fact_company_fundamentals |
| --- | --- | --- |
| dim_date | ✅ (conformed) | ✅ (snapshot_date, conformed) |
| dim_company | ✅ (conformed) | ✅ (conformed) |

Grain-first design is what prevents fan-out and double counting — if a column is not true at the grain, it belongs in another fact/dim, not this one.

*Practice criterion*: run the 4 steps verbally for a NEW process (e.g., dividend payments: grain = 1 row per symbol per ex-dividend date; facts = dividend amount; dims = company, date).

---

## 5. Surrogate Key Generation & Unknown-Member Conventions (Medium)

Generation options compared:

| Method | Stable across rebuild? | Pros | Cons |
| --- | --- | --- | --- |
| `GENERATED ALWAYS AS IDENTITY` | ❌ (rebuild reassigns) | Simple, compact | Our stale-key incident (`data_warehousing_master.md` §6) |
| Hash key (`sha2(concat_ws('||', business_key), 256)`) | ✅ | Deterministic, idempotent, merge-friendly | Wide (string), less human-readable |
| Sequence (`NEXT VALUE FOR seq`) | ❌ | Readable | Same rebuild hazard as IDENTITY |
| Natural business key | ✅ if source is stable | No mapping | Source key changes break history |

**Unknown-member conventions** (Kimball standard): every dimension reserves special surrogate rows so fact rows with missing/invalid keys never orphan and never drop:

* `-1` = Unknown / not provided (e.g., company_overview missing for a symbol)
* `-2` = Not applicable (e.g., no sector for an index instrument)
* `0` (some teams) = error/not yet inferred

Application to our warehouse: seed `dim_company` with `(company_key = -1, symbol = 'UNKNOWN', name = 'Unknown Company', sector = 'Unknown', ...)`; `load_warehouse` key resolution defaults unresolvable symbols to `-1` instead of failing or dropping rows. Note our current validation **rejects** unresolvable keys — a stricter choice; the unknown-member pattern is the Kimball-standard alternative. Both are defensible — know the trade-off: strict = data quality gate, unknown-member = completeness with a quarantine-worthy audit flag.

**Inferred members**: if a fact arrives before its dim row, insert a skeleton dim row ("inferred" flag) and backfill attributes later.

*Practice criterion*: write the MERGE that upserts the `-1` unknown member and the skeleton-inferred row logic; explain when you would choose rejection vs unknown-member.

---

## 6. Aggregate Tables & Pre-Aggregation (High)

Why: dashboards hit the same aggregations repeatedly; scanning the full fact table each time wastes compute and latency. Pre-compute once, read many.

Rules:

* Aggregates must be **derivable from base facts** (recompute from source of truth — never the other way round)
* Same metric definitions as the semantic layer (§7)
* Documented lineage: `agg_monthly_stock` ← `fact_daily_stock`
* Incremental refresh via MERGE on the aggregate key — not full overwrite

Candidate for our project:

```text
stock_market.warehouse.agg_monthly_stock
  grain: 1 row per symbol per month
  key: (symbol, month_key)
  measures: avg_close, min_low, max_high, sum_volume,
            sum_daily_change, trading_days, max_daily_return_pct
```

Databricks options: manual agg table + scheduled MERGE (full control — our pattern), or **materialized views** (auto-refresh, cluster-managed). Prefer manual while learning what MVs abstract away.

*Practice criterion*: build `agg_monthly_stock` with incremental MERGE and verify it reconciles exactly against a windowed query over the base fact.

---

## 7. Semantic Layer, Metrics & Business Definitions (High)

Problem it solves: "two dashboards show different revenue" — the same name, different SQL. A semantic layer defines each metric **once**, with an owner and a formula:

| Metric | Definition (single source of truth) |
| --- | --- |
| `daily_return_pct` | `(close − previous_day_close) / previous_day_close × 100`, null when previous close null — never averaged across nulls |
| `avg_volume_30d` | `AVG(volume)` over the trailing 30 **trading** days (not calendar days!) |
| `top_gainer` | highest `daily_return_pct` per day, ties by lower volume |

Databricks implementation: **metric views** (Unity Catalog, `MEASURE()` aggregations) or a Genie space for natural-language access. Publish metrics in UC so every dashboard, query, and agent uses the same definition.

*Practice criterion*: define the three metrics above as a metric view and prove a dashboard and an ad-hoc query return identical numbers from it.

---

## 8. Warehouse Workload & Query Optimization (High)

Toolbox, in the order you should apply it (measure first!):

1. **Statistics** — `ANALYZE TABLE ... COMPUTE STATISTICS` (you already ran this on Silver) so the optimizer knows row counts/distincts
2. **Query profiles** — Databricks SQL profile tab: bytes scanned, shuffle, spill; fix the biggest line first
3. **Partition pruning & partitioning** — match filters to partition columns (`trade_date`-partitioned facts prune instantly on date filters)
4. **Liquid clustering** — modern alternative to Hive-style partitioning/Z-ORDER; cluster on `(symbol, trade_date)` for both symbol and date predicates; no partition-per-value explosion
5. **Materialized views / result caching** — repeated dashboard queries; DFC caches results for identical SQL
6. **File compaction** — small files kill scan parallelism; `OPTIMIZE` on a schedule
7. **Data skipping** — min/max stats per file; wide `WHERE symbol IN (...)` benefits from clustered layout

Anti-pattern: tuning compute (bigger warehouse) before tuning the query. Always: profile → prune → cluster → cache → scale.

*Practice criterion*: take one slow dashboard query on `fact_daily_stock`, read its profile, and name the bottleneck (scan vs shuffle vs join) and one fix for it.

---

## 9. Data Retention, Archival & Historical Backfills (Medium)
* **Time travel** — every Delta table keeps version history (default 7 days, `delta.deletedFileRetentionDuration` / table properties to extend); point-in-time recovery: `RESTORE TABLE ... TO VERSION AS OF n`
* **VACUUM** — actually removes old files; retention must exceed time-travel needs
* **Retention policy by layer**: Bronze raw — longest (replayability), Silver/Gold — as long as analytics need, warehouse — forever (it's small)
* **Archival**: cold partitions (e.g., year > 5) to cheaper storage; S3 lifecycle classes if on cloud storage
* **Backfills**: reprocess a window without touching the rest — reset watermark to window start, MERGE absorbs duplicates (idempotency makes backfills safe). In our project: set `watermark_value` back N days in `etl.pipeline_watermarks`, rerun the chain — MERGE dedupes
*Practice criterion*: dry-run a 30-day backfill of `fact_daily_stock` and prove row counts don't change for already-loaded dates.

---

## 10. Data Warehouse Security & Access Control (High)
Unity Catalog model:
* **Grants** — `GRANT SELECT ON SCHEMA stock_market.warehouse TO `analysts``; securables: catalog → schema → table/column
* **Principle of least privilege**: analysts get read on `warehouse` (+ curated `gold` views) only; `bronze`, `etl`, `quarantine` stay pipeline-only
* **Row filters / column masks** — e.g., mask `analyst_target_price` for external users; row filter internal-only symbols
* **Service principals** for pipelines (never personal credentials in jobs)
* **Governed tags** + lineage in UC for governance reporting
*Practice criterion*: write the grant set for three personas (analyst, data engineer, external stakeholder) on our catalog.

---

## 11. SCD in Incremental Pipelines (High)
`data_warehousing_master.md` §10 has the base SCD2 MERGE. Production concerns on top:
* **Source must be ordered** — dedupe updates per key by `updated_at DESC` before the SCD MERGE, or an older event can overwrite a newer one
* **Idempotency** — SCD2 must handle re-delivered events: match on `(business_key, updated_at)`; only insert a new version when a change actually occurred
* **Out-of-order/late events** — late updates create version rows out of sequence; decide to accept (valid_from = event time) or quarantine
* **CDC integration** — deletes need tombstone handling (close current version, insert nothing)
* **Delta CDF** (`readChangeFeed`) supplies the change stream; or Lakeflow SDP `APPLY CHANGES INTO` handles SCD1/2 automatically
*Practice criterion: hand-write an SCD2 MERGE (dedupe → close → insert) for `dim_company` sector changes and prove a rerun creates zero duplicate versions.

---

## 12. Warehouse Testing & Deployment Practices (High)
Test pyramid for a warehouse:
* **Unit tests** — pure transform functions: fake-null normalization, price rounding, movement classification (repo `tests/` + pytest; patterns already exist in `datalake-pipeline/tests/`)
* **Integration tests** — small fixtures → run transform → assert output rows/types
* **Data/reconciliation tests** — the checks `load_warehouse` already does, promoted to a suite: counts (Silver→Gold→Warehouse), duplicate keys, orphan keys, null measures, fresh data (`max(trade_date)` within N days)
* **Contract tests** — schema drift detection: expected schema vs `DESCRIBE` (you are literally doing this in your exploration notebook — automate it)
* **Deployment**: dev → prod promotion (separate catalogs `stock_market_dev` / `stock_market`), CI gate = tests + reconciliation suite green before merge; GitHub Actions runs pytest on the repo
*Practice criterion*: convert `load_warehouse`'s built-in validations into a pytest reconciliation suite and run it in CI on a push.

---

## 13. Interview Questions (advanced DW)
1. Kimball vs Inmon — which for a 5-person startup? A 5000-person bank?
2. When would you snowflake a dimension?
3. What is Data Vault and why would you — or not — use it over Kimball staging?
4. Walk the 4-step Kimball process for dividend payments
5. How do you handle a fact row whose dimension member doesn't exist yet (inferred members) or is unknown (-1 member)?
6. Aggregate table design: how do you keep it incremental and reconciled?
7. Semantic layer: how do you stop two teams computing "revenue" differently?
8. Query is slow on the fact table — your diagnostic order?
9. How do you backfill 30 days of facts without duplicating data?
10. SCD2 MERGE arrives twice with the same event — what happens, and how do you prevent a double version row?
11. Describe your warehouse test pyramid and what runs in CI

---

## 14. Mastery Checklist (notes ≠ mastery)
- [ ] Bus matrix + Kimball defense drawn from memory
- [ ] Unknown-member (-1) row MERGEd into dim_company
- [ ] `agg_monthly_stock` built and reconciled
- [ ] 3 metrics defined as a metric view
- [ ] One slow query profiled and fixed
- [ ] 30-day backfill dry-run proved idempotent
- [ ] Grant set written for 3 personas
- [ ] SCD2 MERGE rerun-proven
- [ ] Reconciliation pytest suite in CI

---

*(Source: gap list from `data_warehouse_topic_checklist.md` §3 (user-provided, 2026-10-22), elaborated with Databricks/Delta specifics and project-grounded examples.)