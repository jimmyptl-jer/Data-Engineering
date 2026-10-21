# Data Warehousing & Dimensional Modeling — Master Notes

> **Purpose**: Deep-dive notes for every data-warehousing topic in the journey tracker (Phase 8, topics 72–82), grounded in the actual `stock_market.warehouse` star schema built in this project. Companion notes: `databricks_stock_market_platform.md` (pipeline state), `gold_layer_master.md` (Gold layer design), `journey_tracker.md` (roadmap).
>
> **Last updated**: 2026-10-22

---

## 1. Data Warehouse Fundamentals

A **data warehouse (DW)** is an analytical store purpose-built for reporting and analysis, not for running transactions.

Classic definition (Inmon): **subject-oriented, integrated, time-variant, non-volatile**:

* **Subject-oriented** — organized around business subjects (stocks, companies) not application tables
* **Integrated** — data from multiple sources is cleaned into one consistent model (our Silver layer)
* **Time-variant** — history is preserved (every trade_date, snapshot_date)
* **Non-volatile** — data is loaded and upserted deliberately, not mutated by end users

**In this project**: the warehouse is schema `stock_market.warehouse` in Unity Catalog — Delta Lake tables arranged as a star schema, loaded by the `load_warehouse` orchestrator from `stock_market.gold.stock_enriched`. A lakehouse-style warehouse: Delta/UC instead of a traditional RDBMS.

Key layers recap:

```text
Bronze (raw, preserved)  →  Silver (clean, typed, validated)  →  Gold (business model)
→  Warehouse (star schema: dimensions + facts, analytics-ready)
```

---

## 2. OLTP vs OLAP

| Aspect | OLTP (operational) | OLAP (analytical) |
| --- | --- | --- |
| Purpose | Run the business (transactions) | Analyze the business (reporting) |
| Workload | Many small reads/writes | Large scans, aggregations |
| Schema | Normalized (3NF), row store | Dimensional (star), columnar-friendly |
| Granularity | Current row state | Historical, aggregated views |
| Users | Applications | Analysts, BI tools, dashboards |
| Example in project | (External) market data APIs, source systems | `warehouse.fact_daily_stock` + dims |

**Why it matters here**: querying Silver for every dashboard request re-computes joins and scans wide tables. The warehouse pre-joins business keys into a narrow star, gives stable surrogate keys, and lets BI tools prune and aggregate cheaply. That is also why analytics should hit `warehouse.*`/`gold.*`, never `bronze.*`.

---

## 3. Star Schema vs Snowflake (vs 3NF)

* **3NF (normalized)** — entities broken into many tables to remove redundancy. Good for transaction systems and staging; bad for analytics (too many joins).
* **Star schema (Kimball)** — a central **fact table** referencing denormalized **dimension tables**. Fewer joins, BI-friendly, the industry default for DW.
* **Snowflake** — dimensions further normalized into sub-dimensions (e.g., dim_company → dim_sector). Fewer duplicates stored, more joins at query time.

**Our choice: star schema.** Current shape:

```text
                    ┌──────────────────┐        ┌───────────────────────────────┐
                    │   dim_date       │        │         dim_company           │
                    │ (date_key PK,    │        │ (company_key PK IDENTITY,      │
                    │  21 calendar     │        │  symbol BK, sector, industry, │
                    │  attributes)     │        │  country, exchange, ...)      │
                    └────────┬─────────┘        └──────────┬─────────┬────────┘
                             │                             │         │
              ┌──────────────┴─────────┐   ┌───────────────┴─────┐   │
              │  fact_daily_stock       │   │ fact_company_       │   │
              │  grain: symbol +        │   │ fundamentals        │   │
              │  trade_date             │◄──┤  grain: symbol +    │   │
              └────────────────────────┘   │  snapshot_date      │   │
                                           └───────────────────────┘
```

Trade-off to be able to explain in interviews: star denormalizes dimension attributes (company name repeated per company — cheap, dimensions are small); snowflake would save storage but cost joins. For a fact table of millions/billions of rows with small dims, star wins.

---

## 4. Fact Tables

A fact table stores **measurements/events** at a declared **grain**, with foreign keys to dimensions.

**The grain rule (most important)**: declare "one row = …" before writing any DDL. Every column must be true at that grain.

Fact table types:

* **Transaction fact** — one row per business event (an order, a trade)
* **Periodic snapshot** — one row per entity per period (account balance each month)
* **Accumulating snapshot** — one row per process instance, columns updated as milestones happen (order pipeline)
* **Factless fact** — no measures, records that something occurred (attendance)

**Our facts**:

* `fact_daily_stock` — **periodic/transaction-style fact**, grain: **one row per symbol per trading date**. Measures: open, high, low, close, volume (additive); daily_change (additive per symbol-date); previous_day_close / previous_day_open (semi-additive context — derived); market_movement (non-additive, descriptive).
* `fact_company_fundamentals` — **periodic snapshot**, grain: **one row per symbol per snapshot_date** (fundamentals history without SCD2 dimension versioning).

Additivity (interview favorite):

* **Additive** — summable across all dims (volume, daily_change)
* **Semi-additive** — summable across some dims but not time (a close price; balances)
* **Non-additive** — cannot be summed (market_movement; ratios like P/E — average or recompute instead)

**Degenerate dimension**: a dimension-like key stored in the fact with no dimension table (our `symbol` and `trade_date` live directly in `fact_daily_stock` alongside the surrogate keys — intentional, keeps queries simple and the fact self-describing).

---

## 5. Dimension Tables

Dimensions provide the **context/attributes** for filtering and grouping facts.

* **Conformed dimensions** — same dimension used consistently across facts (both our facts use `dim_company`; `dim_date` shared)
* Attributes should be descriptive text, not codes, for BI usability

**Our dimensions**:

* `dim_company` — built from Alpha Vantage OVERVIEW data (Silver `company_overview_data`): name, sector, industry, country, exchange, currency, market cap, P/E, dividend yield, 52-week range, analyst target, and more. MERGE on `symbol`.
* `dim_date` — calendar attributes, fully generated, independent of any source (see §7).

---

## 6. Surrogate Keys vs Business Keys

* **Business key** — the natural source identifier (`symbol`, `date`). Meaningful, but unstable: sources rename, merge, or reuse keys.
* **Surrogate key** — an artificial key with no business meaning (integer IDENTITY, or a hash), generated by the warehouse. Insulates the fact table from source key changes and enables SCD2 versioning.

Databricks options:

1. `GENERATED ALWAYS AS IDENTITY` — what `dim_company` uses
2. **Hash keys** — `sha2(concat_ws('||', symbol), 256)` — deterministic, stable across rebuilds
3. **Natural business key as the key** — simplest; fine for stable grains like date

**⚠ Project lesson (interview-worthy failure story)**: `dim_company` uses `GENERATED ALWAYS AS IDENTITY`. A **full rebuild** of the dim reassigns new `company_key` values, so historical `fact_company_fundamentals` rows keep pointing at stale keys — orphaned references. Fixes:

* (a) **Only MERGE, never full-rebuild dims** (current `load_warehouse` approach — new symbols get new keys, existing ones keep theirs)
* (b) Resolve keys at query time via `symbol`
* (c) Replace IDENTITY with a **stable hash key** so rebuilds are idempotent

Rule: surrogate-key assignment must be idempotent if facts keep history.

---

## 7. dim_date (Deep Dive)

Why a dedicated date dimension: calendar attributes (quarter, month name, flags) live in one place, enable time intelligence in BI, and avoid recomputing `date_format()` in every query.

**Our design**: `date_key` as `INT` in `yyyyMMdd` form (e.g. `20261022`): readable, sortable, join-friendly. MERGE on `date_key`.

21-column schema (from `load_dim_date` / `load_warehouse`): `date` (DATE), `date_key`, `year`, `year_short`, `quarter_number`, `year_quarter` (e.g. 2026Q4), `month_number`, `month_name`, `month_short`, `week_of_year`, `day_of_week`, `day_name`, `day_of_month`, `is_weekend`, `is_month_start`, `is_month_end`, `is_quarter_start`, `is_quarter_end`, `is_year_start`, `is_year_end`, `day_of_year`.

Best practices we follow:

* **Generate a contiguous range** (sequence from earliest trade date to +1 year) — not only dates that appear in data, so outer-joined calendars never have gaps
* Pure function of the calendar — no source dependency
* Late-arriving dates are covered without a rebuild

Generation pattern:

```python
from pyspark.sql import functions as F
rows = spark.sql(f"""
  SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date
""")
# then derive all calendar attributes + yyyyMMdd date_key
```

---

## 8. dim_stock / dim_company (Deep Dive)

Built from `silver.company_overview_data` (Alpha Vantage `OVERVIEW`, 52+ fundamentals):

* **Grain**: one row per listed company
* **Business key**: `symbol`; **surrogate**: `company_key`
* **Load**: MERGE on `symbol` — new companies insert, changed attributes update in place (SCD1)

Design points:

* Keep **volatile market metrics** (market cap, P/E, 52-week range, analyst target) in the **fact snapshot** (`fact_company_fundamentals` on `snapshot_date`) rather than the dim, unless the dim is SCD2 — otherwise history is lost
* Dim keeps **slowly-changing descriptive attributes** (name, sector, industry, country, exchange)

---

## 9. fact_daily_stock (fact_daily_stock_prices) (Deep Dive)

**Grain**: one row per stock per trading date. Verified schema: `symbol (string), trade_date (date), company_key (bigint), date_key (int), open/high/low/close (double), previous_day_close, previous_day_open (double), volume (double), daily_change decimal(13,2), market_movement (string)`.

Load steps (`load_warehouse`):

1. Read `gold.stock_enriched` (silver daily LEFT JOIN overview)
2. Resolve `company_key` from `dim_company`, `date_key` from `dim_date` (validation: every fact row must resolve both — no orphan keys)
3. MERGE on the business key `(symbol, trade_date)` — idempotent upsert

**Known issue — Monday null**: `previous_day_close` / `previous_day_open` are computed in the incremental batch via window `lag()` with a 5-day watermark overlap. Over a weekend the prior trading day falls outside the batch → first Monday gets `null`. Fix options:

* (a) **Backfill from the target fact itself**: left-join previous trading day's close from `fact_daily_stock` when the batch `lag()` is null (preferred — makes the fact self-healing)
* (b) Seed Silver with a prior window before computing

`daily_change = close - previous_day_close`; `market_movement` = Bull/Bear/Neutral derived from daily_change.

---

## 10. Slowly Changing Dimensions (SCD)

| Type | Behavior | When to use | Our status |
| --- | --- | --- | --- |
| SCD1 | Overwrite attribute — no history | Corrections, irrelevant history | `dim_company`, `dim_date` (current) |
| SCD2 | New version row (`valid_from`, `valid_to`, `is_current`) | Full attribute history | Not implemented |
| SCD3 | Add `previous_value` column | One-step history | Not implemented |
| SCD6 / hybrid | Combination | Complex cases | Not implemented |

**Our workaround for fundamentals history**: `fact_company_fundamentals` (periodic snapshot per `snapshot_date`) preserves how fundamentals changed over time **without** SCD2 dimension versioning — a common Kimball-accepted pattern for volatile measures.

SCD2 in Databricks (when needed):

```sql
MERGE INTO dim_company_scd2 t
USING updates s
ON t.symbol = s.symbol AND t.is_current = true
WHEN MATCHED AND t.sector <> s.sector THEN
  UPDATE SET t.valid_to = current_date(), t.is_current = false
WHEN NOT MATCHED THEN
  INSERT (symbol, sector, ..., valid_from, is_current)
  VALUES (s.symbol, s.sector, ..., current_date(), true)
-- plus a second insert pass for the new version rows
```

Alternatives: Delta Change Data Feed + `MERGE`, or Lakeflow SDP `APPLY CHANGES INTO`.

---

## 11. Incremental Fact Loading & Reconciliation

Reliable incremental load pattern (what we implemented):

```text
read watermark → select eligible rows (trade_date > watermark − overlap)
→ dedupe on business key → validate (required columns, dup keys)
→ resolve surrogate keys → MERGE on business key
→ write audit row + advance watermark ONLY after success
```

Idempotency: MERGE on `(symbol, trade_date)` means a rerun of the same batch produces zero duplicates — **safe to retry**.

Reconciliation checks (in `load_warehouse` / to run manually):

* Row counts: Silver → Gold → Warehouse per symbol/date window
* Duplicate key count per fact table (`GROUP BY business key HAVING count(*) > 1` = 0)
* Orphan key check: facts keys all resolve to dims (`LEFT JOIN dim … WHERE dim.key IS NULL` = 0)
* Null measure check on non-nullable measures
* `etl.pipeline_watermarks` audit history per run (status, rows read/written, started/completed, error)

---

## 12. Gold / Warehouse Analytical SQL (starters + interview practice)

Daily returns per stock (window function):

```sql
SELECT symbol, trade_date, close,
       ROUND((close - previous_day_close) / previous_day_close * 100, 4) AS daily_return_pct
FROM stock_market.warehouse.fact_daily_stock
WHERE previous_day_close IS NOT NULL;
```

30-day moving average + 52-week high:

```sql
SELECT symbol, trade_date, close,
       AVG(close) OVER (PARTITION BY symbol ORDER BY trade_date
                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS sma_30,
       MAX(close) OVER (PARTITION BY symbol
                        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS high_52w
FROM stock_market.warehouse.fact_daily_stock;
```

Sector performance (join to conformed dim):

```sql
SELECT c.sector, d.year_quarter,
       COUNT(DISTINCT f.symbol) AS companies,
       AVG(f.daily_change) AS avg_daily_change
FROM stock_market.warehouse.fact_daily_stock f
JOIN stock_market.warehouse.dim_company c ON f.company_key = c.company_key
JOIN stock_market.warehouse.dim_date d ON f.date_key = d.date_key
GROUP BY c.sector, d.year_quarter;
```

Also practice: top 10 movers per day (`RANK() OVER (PARTITION BY trade_date ORDER BY daily_change DESC)`), volume trend vs own 30-day average, biggest gainer streaks (LAG-based flag + cumulative SUM).

---

## 13. Redshift (optional track)

`redshift_warehouse_pipeline` (reference notebook, not production) sketches the classic-cloud-DW path: same star schema pushed to Amazon Redshift via JDBC/boto3. When Redshift instead of Databricks SQL: existing Redshift BI estate, Redshift Spectrum over S3, strict separate compute costs. Keep optional — the Delta/UC warehouse already serves analytics.

---

## 14. Interview Questions on This Topic

1. Star vs snowflake — trade-offs? Why star for BI?
2. Why surrogate keys? What breaks if a dimension is rebuilt with new surrogate keys? (Answer with our IDENTITY incident)
3. Types 1/2/3 SCD — how would you implement SCD2 in Delta?
4. Declare a fact grain for daily stock prices. What is the business key?
5. Additive vs semi-additive vs non-additive measures — classify close, volume, P/E
6. How do you make an incremental load idempotent? What happens if it fails mid-run?
7. Why is a date dimension generated rather than derived from data?
8. How do you reconcile a load (counts, orphans, duplicates)?

---

*(Source: project implementation (`stock_market.warehouse`, load_warehouse, watermark engine) + journey-tracker Phase-8 topics; verified against the DESCRIBE of fact_daily_stock, 2026-10-22.)