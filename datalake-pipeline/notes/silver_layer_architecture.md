# Silver Layer Architecture & Engineering Guide

> **Core Philosophy**:
> - **Bronze** = Source Truth (Preserve original API payload without distortion)
> - **Silver** = Trusted Data (Cleaned, normalized, typed, deduplicated, and validated)
> - **Gold** = Business Truth (Aggregations, KPIs, technical indicators, analytics)

---

## 1. Data Contracts & Schema Hygiene

Silver establishes a predictable data contract so downstream analytics systems can rely on fixed data types and canonical naming.

```text
symbol       STRING    (ISO ticker symbol, uppercase)
trading_date DATE      (ISO 8601 date YYYY-MM-DD)
open         DOUBLE    (Opening price rounded to 2 decimals)
high         DOUBLE    (Intraday high price)
low          DOUBLE    (Intraday low price)
close        DOUBLE    (Closing price rounded to 2 decimals)
volume       LONG      (Trading volume integer count)
```

### Schema Drift Guards
Before processing raw JSON, Silver validates that required columns are present:
```python
missing_columns = [col for col in EXPECTED_BRONZE_COLUMNS if col not in data_df.columns]
if missing_columns:
    raise ValueError(f"Schema drift detected: missing {missing_columns}")
```

---

## 2. Text Normalization & Fake-Null Tokens

Data APIs often return missing values as string placeholders rather than JSON `null`. Silver normalizes all placeholder tokens (`"n/a"`, `"na"`, `"null"`, `"none"`, `"-"`, `""`) to true PySpark `NULL`.

- Codes & identifiers (`symbol`, `currency`, `mic`): `upper(trim(col))`
- Names & descriptions (`company_name`, `type`): `initcap(trim(col))`

---

## 3. Business Keys & Deterministic Deduplication

Every dataset must define a **Business Key** representing its real-world identity:
- **Daily Time Series**: `(symbol, day_date)`
- **Weekly Time Series**: `(symbol, week_date)`
- **Stock Ticker Reference**: `(symbol, mic)`
- **Company Overview**: `symbol`
- **Exchanges**: `id`

### Latest-Wins Windowed Deduplication
To resolve conflicts when multiple records arrive for the same business key, Silver orders records chronologically by `ingestion_ts` and selects `row_number == 1`:

```python
window_spec = Window.partitionBy("symbol", "day_date").orderBy(col("ingestion_ts").desc())
deduped_df = (
    df.withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)
```

---

## 4. Quality Rules & The Quarantine Pattern

Instead of silently deleting invalid records (`df.filter(close >= 0)`), Silver applies the **Quarantine Pattern**:

```text
                     Bronze DataFrame
                            │
                    Validation Audit
                      /          \
          status==VALID          status==INVALID
                    │                      │
                    ▼                      ▼
              Silver Storage       Quarantine Storage
          (s3://.../silver/)     (s3://.../silver_quarantine/)
```

Records failing quality assertions (`symbol IS NULL`, `close <= 0`, `high < low`, `volume < 0`) are tagged with `validation_status = 'INVALID'` and a comma-separated `validation_reason` string for audit investigation.

---

## 5. Data Lineage Metadata

Every Silver record is enriched with 4 lineage columns for 100% auditability:

| Field | Type | Description |
|---|---|---|
| `source` | `STRING` | Originating provider (`alphavantage`, `finnhub`, `massive`) |
| `batch_id` | `STRING` | Pipeline execution batch ID (`batch_YYYYMMDD_HHMMSS`) |
| `ingestion_ts` | `TIMESTAMP` | Extraction timestamp when Bronze received the raw payload |
| `processed_at` | `TIMESTAMP` | Transformation execution UTC timestamp |

---

## 6. Implementation Roadmap

1. **Phase 1**: Data Contracts & Schema Drift Guards
2. **Phase 2**: Text Normalization & Explicit Data Type Casting
3. **Phase 3**: Business Identity & Deterministic Deduplication
4. **Phase 4**: Data Quality Tagging & Quarantine Split Routing
5. **Phase 5**: Data Lineage Metadata Integration
6. **Phase 6**: Incremental Watermarking & Idempotent Partition Writes
