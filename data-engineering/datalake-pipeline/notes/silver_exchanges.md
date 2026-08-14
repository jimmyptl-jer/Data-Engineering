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

---

## 3. Schema & Field Specifications

| Field Name | Data Type | Nullable | Canonical Format / Rule | Description |
|---|---|---|---|---|
| `asset_class` | `STRING` | No | Lowercase (e.g. `stocks`, `crypto`) | Asset classification category |
| `id` | `LONG` | No | Numeric integer identifier | Unique exchange integer ID |
| `locale` | `STRING` | No | Lowercase (e.g. `us`, `global`) | Geographic market locale |
| `name` | `STRING` | No | Raw text description | Exchange official name |
| `operating_mic` | `STRING` | No | Uppercase MIC code (e.g. `XNYS`) | Operating Market Identifier Code |
| `participant_id` | `STRING` | No | Uppercase identifier | Exchange participant code |
| `type` | `STRING` | No | Uppercase type (`EXCHANGE`, `TRF`) | Exchange venue type |
| `validation_status`| `STRING` | No | `VALID` or `INVALID` | Data Quality tag |
| `validation_reason`| `STRING` | Yes | Comma-separated missing column reason | Quarantine audit trail |
| `mic` | `STRING` | No | Uppercase (`operating_mic` alias) | Standardized MIC code |

---

## 4. Transformation Sequence (Bronze → Silver)

1. **Column Selection**: Extract `asset_class`, `id`, `locale`, `name`, `operating_mic`, `participant_id`, `type` from raw Bronze JSON.
2. **Explicit Type Casting**: Cast `id` to `LongType()`; cast text fields to `StringType()`.
3. **Whitespace Trimming**: Trim spaces across string columns.
4. **Fake Null Normalization**: Map empty strings or placeholder text (`"n/a"`, `"none"`, `"-"`) to PySpark `NULL`.
5. **Data Quality Validation**: Evaluate non-null assertions across all 7 fields. Tag `validation_status` (`VALID` / `INVALID`).
6. **Validation Reason Generation**: Generate detailed missing field tags via `concat_ws(", ", ...)`.
7. **Casing Standardization**: Standardize codes (`operating_mic`, `type`) to uppercase; standardize `locale` to lowercase. Create `mic` alias column.
8. **Metrics Logging**: Compute `total_in`, `total_out`, `valid`, `invalid` counts.

---

## 5. Sample Silver Output Data

| asset_class | id | locale | name | operating_mic | participant_id | type | mic | validation_status |
|---|---|---|---|---|---|---|---|---|
| stocks | 1 | us | New York Stock Exchange | XNYS | NYSE | EXCHANGE | XNYS | VALID |
| stocks | 2 | us | NYSE American, LLC (AMEX) | XASE | AMEX | EXCHANGE | XASE | VALID |
| stocks | 10 | us | IEX Market Inc. | IEXG | IEXG | EXCHANGE | IEXG | VALID |
| crypto | 6 | global | Coinbase | COINBASE | COINBASE | EXCHANGE | COINBASE | VALID |
