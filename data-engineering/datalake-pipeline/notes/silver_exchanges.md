# Silver Dataset Specification — Massive Exchanges (`exchanges`)

## 1. Overview & Data Purpose
- **Dataset Identifier**: `exchanges`
- **Data Source**: Massive (Polygon.io) REST API (`v3/reference/exchanges`)
- **Ingestion Frequency**: Weekly / Monthly reference refresh
- **Target Medallion Layer**: Silver Data Lake (`s3a://graywolf--data--lake/stock/silver/datasource=massive/dataset=exchanges/`)
- **Downstream Consumers**: Exchange identifier lookup tables, MIC resolution, Asset class reference maps.

---

## 2. Data Contract & Identity

### Business Key
The primary identifier for every record in this dataset is:
```text
id
```
(Alternative business key: `operating_mic`)

### Partitioning Strategy
Written to S3 as a flat reference table (not time-partitioned):
```text
datasource=massive/dataset=exchanges/
```

---

## 3. Schema & Field Specifications

| # | Field Name | Data Type | Nullable | Canonical Format / Rule | Description |
|---|---|---|---|---|---|
| 1 | `asset_class` | `STRING` | No | Raw text (e.g. `stocks`, `crypto`) | Asset classification category |
| 2 | `id` | `LONG` | No | Numeric integer identifier | Unique exchange integer ID |
| 3 | `locale` | `STRING` | No | Lowercase via `lower()` (e.g. `us`, `global`) | Geographic market locale |
| 4 | `name` | `STRING` | No | Raw text description | Exchange official name |
| 5 | `operating_mic` | `STRING` | No | Uppercase MIC code (e.g. `XNYS`) | Operating Market Identifier Code |
| 6 | `participant_id` | `STRING` | No | Raw text identifier | Exchange participant code |
| 7 | `type` | `STRING` | No | Uppercase via `upper()` (`EXCHANGE`, `TRF`) | Exchange venue type |
| 8 | `validation_status` | `STRING` | No | `VALID` or `INVALID` | Data Quality tag |
| 9 | `validation_reason` | `STRING` | Yes | Comma-separated missing column reason via `concat_ws()` | Quarantine audit trail (NULL if VALID) |
| 10 | `mic` | `STRING` | No | Uppercase copy of `operating_mic` via `upper()` | Standardized MIC alias column |

---

## 4. Transformation Sequence (Bronze → Silver)

The following 9 steps are executed in order by `transform_massive_exchanges_dataset()`:

| Step | Code Banner | Operation | Detail |
|------|-------------|-----------|--------|
| 1 | `MASSIVE EXCHANGES — BRONZE → SILVER` | **Start** | Log transformation start. |
| 2 | `SELECT + CAST EXCHANGE DATA` | **Select & Cast** | Select 7 source columns (`asset_class`, `id`, `locale`, `name`, `operating_mic`, `participant_id`, `type`). Then cast `id` to `LongType()` and all text fields to `StringType()`. |
| 3 | `TRIM WHITESPACE` | **Whitespace Trimming** | Trim leading/trailing whitespace across 6 string columns (`asset_class`, `locale`, `name`, `operating_mic`, `participant_id`, `type`). |
| 4 | `NORMALIZE FAKE NULL VALUES` | **Fake Null Normalization** | Map `""`, `"n/a"`, `"na"`, `"null"`, `"none"`, `"-"` → PySpark `NULL` across the 6 string columns. |
| 5 | `APPLY EXCHANGE VALIDATION` | **Validation Status** | Tag each row `VALID` or `INVALID` based on 7 non-null rules: `asset_class`, `id`, `locale`, `name`, `operating_mic`, `participant_id`, `type` must all be non-null. |
| 6 | `GENERATE VALIDATION REASONS` | **Validation Reason** | Use `concat_ws(", ", ...)` to list all failing fields: `"MISSING ASSET_CLASS"`, `"MISSING ID"`, `"MISSING LOCALE"`, `"MISSING NAME"`, `"MISSING OPERATING_MIC"`, `"MISSING PARTICIPANT_ID"`, `"MISSING TYPE"`. |
| 7 | `STANDARDIZE EXCHANGE CASING` | **Casing Standardization** | Create `mic` column as `upper(operating_mic)`. Convert `type` to uppercase. Convert `locale` to lowercase. |
| 8 | `MASSIVE EXCHANGES ROW COUNT METRICS` | **Row Count Metrics** | Calculate and log `total_in`, `total_out`, `valid`, `invalid` counts. |
| 9 | `MASSIVE EXCHANGES TRANSFORMATION COMPLETED` | **Return** | Return Silver DataFrame. |

---

## 5. Data Quality Validation Rules

| # | Rule | Condition | Tag |
|---|------|-----------|-----|
| 1 | Asset class required | `asset_class IS NULL` | INVALID |
| 2 | ID required | `id IS NULL` | INVALID |
| 3 | Locale required | `locale IS NULL` | INVALID |
| 4 | Name required | `name IS NULL` | INVALID |
| 5 | Operating MIC required | `operating_mic IS NULL` | INVALID |
| 6 | Participant ID required | `participant_id IS NULL` | INVALID |
| 7 | Type required | `type IS NULL` | INVALID |

---

## 6. Sample Silver Output Data

| asset_class | id | locale | name | operating_mic | participant_id | type | mic | validation_status |
|---|---|---|---|---|---|---|---|---|
| stocks | 1 | us | New York Stock Exchange | XNYS | NYSE | EXCHANGE | XNYS | VALID |
| stocks | 2 | us | NYSE American, LLC (AMEX) | XASE | AMEX | EXCHANGE | XASE | VALID |
| stocks | 10 | us | IEX Market Inc. | IEXG | IEXG | EXCHANGE | IEXG | VALID |
| crypto | 6 | global | Coinbase | COINBASE | COINBASE | EXCHANGE | COINBASE | VALID |
