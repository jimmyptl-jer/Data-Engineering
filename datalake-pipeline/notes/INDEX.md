# Notes Index — Data Engineering "Brain"

> All project knowledge for the stock market data platform lives in this folder (`datalake-pipeline/notes/`). This index is the entry point. Production pipeline code lives in the Databricks project `stock-market-data-platform` (never modified without explicit user instruction).
>
> **Last updated**: 2026-10-22

---

## Architecture & Master Documents

* `data_flow_architecture.md` — complete technical data flow, medallion architecture, component responsibilities, watermark engine, ops run book (S3/Lambda version)
* `project_status.md` — master technical architecture, end-to-end flow diagrams, DDL schemas, roadmap
* `gold_layer_master.md` — Gold layer specification, gap analysis, and full implementation roadmap (sector analysis, technical indicators, signals)
* `INDEX.md` — this file

## Schema & Write Discipline

* `schema_handling.md` — production schema design: grain, business key, three schema layers, explicit StructTypes, 10-step schema engineering workflow
* `databricks_delta_write_modes.md` — Delta write modes (`append`/`overwrite`/`error`/`ignore`), `mergeSchema`, validate-before-write, MERGE policy on Databricks

## Databricks Platform (current production)

* `databricks_stock_market_platform.md` — **master note for the Databricks pipeline**: project layout, Unity Catalog (`stock_market`), naming conventions, watermark/incremental design, write-mode matrix, warehouse star schema, security issues, open items

## Data Warehousing & Dimensional Modeling

* `advanced_data_warehouse_topics.md` — **deep-dive on the 12 advanced gap topics**: Kimball vs Inmon, star vs snowflake extension, Data Vault, Kimball 4-step + bus matrix, surrogate-key generation + unknown-member conventions, aggregate tables, semantic layer/metrics, query-optimization toolbox, retention/backfills, UC security, SCD2 in incremental pipelines, testing & CI/CD — each with a practice criterion and interview questions

* `data_warehouse_topic_checklist.md` — **progress checklist & gap map**: status of all DW topics covered (fundamentals → dimensional modeling → SCD → loading/quality → storage/performance → practical designs), the advanced-gap topics table with priorities and one-line definitions, where each gap fits in the stock-market project, and the recommended learning order (next: Spark join strategies)

* `data_warehousing_master.md` — **deep-dive on every Phase-8 topic**: DW fundamentals, OLTP vs OLAP, star vs snowflake, fact/dimension design, grain, surrogate vs business keys (IDENTITY-key incident + fixes), dim_date and dim_company specs, fact_daily_stock deep dive (incl. Monday-null fix), SCD 1/2/3 (+ Delta MERGE implementation), incremental loading & reconciliation, analytical SQL starters, Redshift track, interview questions

## Dataset Specifications (Silver layer)

* `silver_layer_architecture.md` — silver layer design principles
* `silver_daily_time_series.md` — 19-step daily OHLCV transform spec
* `silver_weekly_time_series.md` — 15-step weekly transform spec
* `silver_company_overview.md` — 10-step company overview transform spec (57 output columns)
* `silver_ticker_reference.md` — Finnhub ticker reference spec
* `silver_exchanges.md` — Massive/Polygon exchanges spec

## Source Integration

* `alpha_vantage+massive_merge.md` — Alpha Vantage + Massive (Polygon) endpoint integration notes

---

## Spark & PySpark

* `spark_join_strategies.md` — **deep-dive on Spark joins**: logical join types (incl. semi/anti), the 4 physical strategies (broadcast hash, shuffled hash, sort-merge, nested-loop/cartesian), Catalyst's decision tree, hints & config knobs, join skew (AQE skew join, salting), diagnostics via explain()/query profiles, strategy-by-strategy map of our pipeline's actual joins, best practices, interview questions, practice criteria

## Learning Journey & Roadmap

* `journey_tracker.md` — **master journey tracker**: timeline of milestones, actual status vs roadmap (star schema already built), condensed 11-phase topic tracker, adjusted 10-week job-readiness roadmap (AWS + Databricks, 2h/day), immediate next steps, interview priority tiers

## Operations

* `running_pipeline_locally.md` — **local runbook for the repo's S3/Lambda pipeline**: prerequisites (Python 3.12, Java, AWS, API keys), venv + pinned install, `.env` variable reference, `python -m src.stock_pipeline.app` walkthrough with the active vs commented-out pipeline steps, S3 bronze/silver/gold + watermark layout, YouTube pipeline and Twelve Data SAM Lambda usage, deployment via SAM, troubleshooting table, and a no-AWS smoke test

## Change Policy

All new knowledge, decisions, and conventions get added as `.md` files here (or appended to the relevant existing note). The `stock-market-data-platform` Databricks project is production code — assistant changes to it require explicit user instruction.