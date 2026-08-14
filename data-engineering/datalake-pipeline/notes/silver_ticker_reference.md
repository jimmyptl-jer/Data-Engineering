# Silver Dataset Specification — Finnhub Ticker Reference (`ticker_reference`)

## 1. Overview & Data Purpose
- **Dataset Identifier**: `ticker_reference`
- **Data Source**: Finnhub REST API (`/stock/symbol`)
- **Ingestion Frequency**: Daily batch
- **Target Medallion Layer**: Silver Data Lake (`s3a://graywolf--data--lake/stock/silver/datasource=finnhub/dataset=ticker_reference/`)
- **Downstream Consumers**: Global ticker cross-referencing, FIGI mapping lookup tables, Multi-exchange symbol resolution.

---

## 2. Data Contract & Identity

### Business Key
The composite primary key for every record in this reference dataset is:
```text
(symbol, mic)
```
A stock ticker can be listed on multiple exchanges (different MIC codes), so the unique key is the combination of **Symbol + Market Identifier Code (MIC)**.

### Partitioning Strategy
Written to S3 as a flat reference table (not time-partitioned):
```text
datasource=finnhub/dataset=ticker_reference/
```

---

## 3. Schema & Field Specifications

| # | Field Name | Data Type | Nullable | Canonical Format / Rule | Description |
|---|---|---|---|---|---|
| 1 | `currency` | `STRING` | Yes | ISO 4217 uppercase (`USD`, `EUR`) | Trading currency |
| 2 | `description` | `STRING` | Yes | InitCap text formatting via `initcap()` | Security name / description |
| 3 | `display_symbol` | `STRING` | Yes | Uppercase string (renamed from `displaySymbol`) | Ticker symbol displayed in trading software |
| 4 | `figi` | `STRING` | Yes | Uppercase Bloomberg FIGI code | Global Instrument Identifier |
| 5 | `figi_composite` | `STRING` | Yes | Uppercase composite FIGI (renamed from `figiComposite`) | Composite Bloomberg FIGI |
| 6 | `isin` | `STRING` | Yes | Uppercase ISIN code | International Securities Identification Number |
| 7 | `mic` | `STRING` | Yes | Uppercase MIC code (e.g. `XNAS`, `OOTC`) | Operating Market Identifier Code |
| 8 | `share_class_figi` | `STRING` | Yes | Uppercase share class FIGI (renamed from `shareClassFIGI`) | Share Class FIGI identifier |
| 9 | `symbol` | `STRING` | Yes | Uppercase ticker code (e.g. `AAPL`, `TLPH`) | Finnhub unique stock symbol |
| 10 | `symbol2` | `STRING` | Yes | Uppercase alternate ticker | Secondary / alternative ticker symbol |
| 11 | `type` | `STRING` | Yes | InitCap text via `initcap()` (`Common Stock`, `Etp`, `Adr`) | Security instrument type |
| 12 | `validation_status` | `STRING` | No | `VALID` or `INVALID` | Data Quality assertion tag |
| 13 | `validation_reason` | `STRING` | Yes | Comma-separated error reason string via `concat_ws()` | Quarantine audit details (NULL if VALID) |

---

## 4. Transformation Sequence (Bronze → Silver)

The following 12 steps are executed in order by `transform_finnhub_stock_tickers_dataset()`:

| Step | Code Banner | Operation | Detail |
|------|-------------|-----------|--------|
| 1 | `FINNHUB STOCK TICKERS — BRONZE → SILVER` | **Start** | Log transformation start. |
| 2 | `VALIDATE FINNHUB BRONZE SCHEMA` | **Schema Drift Guard** | Check all 11 expected columns (`currency`, `description`, `displaySymbol`, `figi`, `figiComposite`, `isin`, `mic`, `shareClassFIGI`, `symbol`, `symbol2`, `type`) exist in `data_df.columns`. Raise `ValueError` if any missing. |
| 3 | `SELECT + CAST FINNHUB STOCK TICKER DATA` | **Select & Cast** | Select all 11 columns and explicitly cast each to `StringType()`. |
| 4 | `TRIM WHITESPACE` | **Whitespace Trimming** | Trim leading/trailing whitespace across all 11 string columns. |
| 5 | `NORMALIZE FAKE NULL VALUES` | **Fake Null Normalization** | Map `""`, `"n/a"`, `"na"`, `"null"`, `"-"` → PySpark `NULL` using `lower(col).isin(fake_null_values)`. |
| 6 | `DEDUPLICATE FINNHUB STOCK TICKERS` | **Deduplication** | `dropDuplicates(["symbol", "mic"])` on composite business key. |
| 7 | `STANDARDIZE FINNHUB STOCK TICKER CASING` | **Casing Standardization** | Uppercase 9 identifier columns (`currency`, `displaySymbol`, `figi`, `figiComposite`, `isin`, `mic`, `shareClassFIGI`, `symbol`, `symbol2`). InitCap 2 text columns (`description`, `type`). |
| 8 | `APPLY FINNHUB STOCK TICKER VALIDATION` | **Validation Status** | Tag rows `VALID` or `INVALID` based on 5 rules: `currency`, `description`, `displaySymbol`, `symbol`, `type` must not be NULL. |
| 9 | `GENERATE VALIDATION REASONS` | **Validation Reason** | Use `concat_ws(", ", ...)` to list all failing fields: `"Currency Invalid"`, `"Description Invalid"`, `"Display Symbol Invalid"`, `"Symbol Invalid"`, `"Type Invalid"`. |
| 10 | `RENAME COLUMNS TO SILVER SNAKE_CASE` | **Snake Case Renaming** | `displaySymbol` → `display_symbol`, `figiComposite` → `figi_composite`, `shareClassFIGI` → `share_class_figi`. |
| 11 | `FINNHUB STOCK TICKER ROW COUNT METRICS` | **Row Count Metrics** | Calculate and log `total_in`, `total_out`, `valid`, `invalid` counts. |
| 12 | `FINNHUB STOCK TICKER TRANSFORMATION COMPLETED` | **Return** | Return Silver DataFrame. |

---

## 5. Data Quality Validation Rules

| # | Rule | Condition | Tag |
|---|------|-----------|-----|
| 1 | Currency required | `currency IS NULL` | INVALID |
| 2 | Description required | `description IS NULL` | INVALID |
| 3 | Display symbol required | `displaySymbol IS NULL` | INVALID |
| 4 | Symbol required | `symbol IS NULL` | INVALID |
| 5 | Type required | `type IS NULL` | INVALID |

---

## 6. Column Renaming Map (camelCase → snake_case)

| Bronze Column (API) | Silver Column (Database) |
|---|---|
| `displaySymbol` | `display_symbol` |
| `figiComposite` | `figi_composite` |
| `shareClassFIGI` | `share_class_figi` |

All other columns (`currency`, `description`, `figi`, `isin`, `mic`, `symbol`, `symbol2`, `type`) are already lowercase single-word names and require no renaming.

---

## 7. Sample Silver Output Data

| currency | description | display_symbol | figi | figi_composite | isin | mic | share_class_figi | symbol | type | validation_status |
|---|---|---|---|---|---|---|---|---|---|---|
| USD | Asante Gold Corp | ASGOF | BBG002W947B8 | BBG002W947B8 | | OOTC | BBG002CPHB76 | ASGOF | Common Stock | VALID |
| USD | Talphera Inc | TLPH | BBG0018YYFX7 | BBG0018YYFX7 | | XNAS | BBG001TFZCK5 | TLPH | Common Stock | VALID |
| USD | Inverse Vix St Fu... | VYLD | BBG01SXN1MT9 | BBG01SXN1MT9 | | ARCX | BBG01SXN1NN3 | VYLD | Etp | VALID |
