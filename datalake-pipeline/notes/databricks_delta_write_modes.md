# Delta Lake Write Modes & Schema Control — Databricks Notes

> **Purpose**: How `append` / `overwrite` / `error` / `ignore` write modes interact with Delta table schemas, and the schema-control rules this project follows. Companion to `schema_handling.md` (generic Spark schema design) — this note covers the Databricks/Delta-specific side.
>
> **Last updated**: 2026-10-22

---

## 1. Write Modes and Schema Behavior

In Databricks, writing to a Delta table with `append` or `overwrite` does **not** by itself guarantee the schema stays unchanged.

| Write mode | Behavior | Does it prevent schema changes? |
| --- | --- | --- |
| `append` | Adds new rows | Not by itself |
| `overwrite` | Replaces existing table data | Not by itself |
| `error` / `errorifexists` | Fails if the table already exists | Prevents accidental replacement of an existing table |
| `ignore` | Skips writing if the table exists | Prevents that write from changing the existing table |

Notes:

* Delta **is** stricter than generic Spark writers on `append`: mismatched column names or incompatible types fail by default. But compatible differences can still slip through, so it is not a complete schema-validation mechanism.
* For `overwrite` on an existing Delta table, the default keeps the existing schema; replacing it requires explicitly opting in with `option("overwriteSchema", "true")`. Treat `overwriteSchema=true` as a deliberate, rare action (it rewrites table metadata).

---

## 2. Disabling Automatic Schema Evolution

For this pipeline, use an explicit schema and avoid schema evolution unless intentionally needed:

```python
df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "false") \
    .saveAsTable("stock_market.silver.daily_stock_data")
```

* `format("delta")` — write using Delta Lake
* `mode("append")` — add records without replacing existing data
* `mergeSchema = false` — disables automatic schema merging for this write (this is the default, but stating it documents intent)

`mergeSchema=false` alone is **not** a complete schema-validation mechanism: Delta rejects incompatible schemas, but some compatible differences may still be accepted.

---

## 3. Recommended: Validate Before Writing

Strict comparison of the DataFrame schema against the target table schema before every write:

```python
table_name = "stock_market.silver.daily_stock_data"
target_schema = spark.table(table_name).schema

if df.schema != target_schema:
    raise ValueError("Schema mismatch! Write aborted.")

df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(table_name)
```

This is a **strict** comparison — even differences in column order or metadata can cause rejection. For a name/type-only check, compare the field lists instead of the full schema objects.

---

## 4. How This Project Applies the Rules (as of 2026-10-16)

Layer-based write-mode policy for the `stock_market` catalog:

* **Overwrite** — bronze raw tables (full re-ingest from S3 each run) and quarantine snapshots
* **Append** — watermark/audit log (`etl.pipeline_watermarks`); every run is a new event
* **MERGE (upsert)** — silver, gold, and warehouse dims/facts, with conditional initial load (only overwrite when the target table does not yet exist)

MERGE never evolves the table schema implicitly — target columns must exist. That makes MERGE the safest option for incremental updates and corrections.

Full notebook → table → mode matrix: see `databricks_stock_market_platform.md` §6.

---

## 5. Key Takeaways

1. Explicitly define schemas; never rely on `inferSchema` in production (see `schema_handling.md`)
2. Validate DataFrame schema against the target table schema before writes
3. Keep `mergeSchema` disabled unless a schema change is intentional — then do it deliberately and document it in these notes
4. Use `MERGE` for incremental updates on business keys
5. Treat `overwriteSchema=true` / `CREATE OR REPLACE TABLE` as deliberate, review-gated actions

---

*(Source: user-provided Delta write-mode guidance + project write-mode fixes applied on 2026-10-16.)