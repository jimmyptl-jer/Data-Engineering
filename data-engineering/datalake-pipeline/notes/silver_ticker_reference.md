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

---

## 3. Schema & Field Specifications

| Field Name | Data Type | Nullable | Canonical Format / Rule | Description |
|---|---|---|---|---|
| `currency` | `STRING` | Yes | ISO 4217 uppercase (`USD`, `EUR`) | Trading currency |
| `description` | `STRING` | Yes | InitCap text formatting | Security name / description |
| `display_symbol` | `STRING` | Yes | Uppercase string | Ticker symbol displayed in trading software |
| `figi` | `STRING` | Yes | Uppercase Bloomberg FIGI code | Global Instrument Identifier |
| `figi_composite` | `STRING` | Yes | Uppercase composite FIGI | Composite Bloomberg FIGI |
| `isin` | `STRING` | Yes | Uppercase ISIN code | International Securities Identification Number |
| `mic` | `STRING` | Yes | Uppercase MIC code (e.g. `XNAS`, `OOTC`) | Operating Market Identifier Code |
| `share_class_figi`| `STRING` | Yes | Uppercase share class FIGI | Share Class FIGI identifier |
| `symbol` | `STRING` | Yes | Uppercase ticker code (e.g. `AAPL`, `TLPH`) | Finnhub unique stock symbol |
| `symbol2` | `STRING` | Yes | Uppercase alternate ticker | Secondary / alternative ticker symbol |
| `type` | `STRING` | Yes | InitCap text (`Common Stock`, `ETP`, `ADR`) | Security instrument type |
| `validation_status`| `STRING` | No | `VALID` or `INVALID` | Data Quality assertion tag |
| `validation_reason`| `STRING` | Yes | Comma-separated error reason string | Quarantine audit details |

---

## 4. Transformation Sequence (Bronze → Silver)

1. **Schema Validation Guard**: Verify all 11 expected Finnhub columns exist in raw Bronze payload (`EXPECTED_BRONZE_COLUMNS`); fail fast on schema drift.
2. **Explicit Type Casting**: Cast all fields explicitly to `StringType()`.
3. **Whitespace Trimming**: Trim leading and trailing whitespace across all string columns.
4. **Fake Null Normalization**: Map placeholder strings (`""`, `"n/a"`, `"na"`, `"null"`, `"-"`) to PySpark `NULL`.
5. **Deduplication**: Drop duplicates based on the composite business key `(symbol, mic)`.
6. **Casing Standardization**: Convert identifiers (`currency`, `displaySymbol`, `figi`, `figiComposite`, `isin`, `mic`, `shareClassFIGI`, `symbol`, `symbol2`) to uppercase; convert text descriptions (`description`, `type`) to `InitCap`.
7. **Validation Tagging**: Evaluate mandatory field rules (`currency`, `description`, `displaySymbol`, `symbol`, `type`). Set `validation_status` to `VALID` or `INVALID`.
8. **Validation Reason Generation**: Use `concat_ws(", ", ...)` to list all missing required fields for quarantined records.
9. **Snake Case Renaming**: Rename camelCase Bronze attributes to Silver snake_case (`displaySymbol` → `display_symbol`, `figiComposite` → `figi_composite`, `shareClassFIGI` → `share_class_figi`).
10. **Metrics Logging**: Calculate and log `total_in`, `total_out`, `valid`, and `invalid` row counts.

---

## 5. Sample Silver Output Data

| currency | description | display_symbol | figi | figi_composite | isin | mic | share_class_figi | symbol | type | validation_status |
|---|---|---|---|---|---|---|---|---|---|---|
| USD | Asante Gold Corp | ASGOF | BBG002W947B8 | BBG002W947B8 | | OOTC | BBG002CPHB76 | ASGOF | Common Stock | VALID |
| USD | Talphera Inc | TLPH | BBG0018YYFX7 | BBG0018YYFX7 | | XNAS | BBG001TFZCK5 | TLPH | Common Stock | VALID |
| USD | Inverse Vix St Fu... | VYLD | BBG01SXN1MT9 | BBG01SXN1MT9 | | ARCX | BBG01SXN1NN3 | VYLD | ETP | VALID |
