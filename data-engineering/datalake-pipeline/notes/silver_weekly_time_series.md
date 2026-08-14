# Silver Dataset Specification — Weekly Time Series (`weekly_time_series`)

## 1. Overview & Data Purpose
- **Dataset Identifier**: `weekly_time_series`
- **Data Source**: Alpha Vantage REST API (`TIME_SERIES_WEEKLY`)
- **Ingestion Frequency**: Weekly batch (Fridays at 11:59 PM UTC)
- **Target Medallion Layer**: Silver Data Lake (`s3a://graywolf--data--lake/stock/silver/datasource=alphavantage/dataset=weekly_time_series/`)
- **Downstream Consumers**: Long-term trend analysis, Macro trend reports, Multi-week volatility metrics.

---

## 2. Data Contract & Identity

### Business Key
The composite unique identifier for every record in this dataset is:
```text
(symbol, week_date)
```
Every stock symbol has at most **one price record per weekly session**.

### Partitioning Strategy
Written to S3 partitioned by pipeline execution timestamp:
```text
year=YYYY/month=MM/day=DD/hour=HH/minute=MM/format={parquet|csv}/
```

---

## 3. Schema & Field Specifications

| Field Name | Data Type | Nullable | Canonical Format / Rule | Description |
|---|---|---|---|---|
| `symbol` | `STRING` | No | Uppercase (e.g., `IBM`, `AAPL`) | Stock ticker code |
| `week_date` | `DATE` | No | ISO 8601 `YYYY-MM-DD` | Ending date of weekly trading period |
| `open` | `DOUBLE` | No | positive `> 0`, 2 decimal places | Weekly opening stock price |
| `high` | `DOUBLE` | No | positive `> 0`, `high >= low` | Weekly peak price |
| `low` | `DOUBLE` | No | positive `> 0`, `high >= low` | Weekly trough price |
| `close` | `DOUBLE` | No | positive `> 0`, 2 decimal places | Weekly closing stock price |
| `volume` | `LONG` | No | non-negative `>= 0` | Total weekly trading volume |
| `weekly_change` | `DOUBLE` | Yes | `close - open` (2 decimals) | Dollar change over week |
| `weekly_change_percentage` | `DOUBLE` | Yes | `((close - open) / open) * 100` | Percentage change over week |
| `market_movement` | `STRING` | Yes | `Bull`, `Bear`, or `Neutral` | Overall weekly trend direction |
| `year` | `INTEGER` | No | `YYYY` derived from `week_date` | Partition year |
| `month` | `INTEGER` | No | `MM` (1-12) | Partition month |
| `week_number` | `INTEGER` | No | Calendar week number (1-53) | Week of year |
| `last_refreshed_date` | `DATE` | Yes | ISO 8601 `YYYY-MM-DD` | API refresh date |
| `validation_status` | `STRING` | No | `VALID` or `INVALID` | Data Quality tag |
| `validation_reason` | `STRING` | Yes | Human-readable explanation | Reason for quarantine tag |
| `processed_at` | `TIMESTAMP`| No | UTC timestamp | Transformation execution time |

---

## 4. Transformation Sequence (Bronze → Silver)

1. **JSON Flattening**: Explode raw `Weekly Time Series` map into key (`week_date`) and struct value (`open`, `high`, `low`, `close`, `volume`).
2. **Whitespace Trimming & Uppercase**: Trim leading/trailing spaces; uppercase `symbol`.
3. **Fake Null Normalization**: Replace empty or placeholder text (`"n/a"`, `"-"`, `"none"`) with PySpark `NULL`.
4. **Data Type Casting**: Cast prices to `DoubleType`, volume to `LongType`, dates to `DateType`.
5. **Data Quality Audit**: Validate non-null mandatory fields, positive prices, and `high >= low`. Mark `VALID` / `INVALID`.
6. **Deduplication**: Drop duplicate rows on composite key `(symbol, week_date)`.
7. **Rounding**: Round price metrics (`open`, `high`, `low`, `close`) to 2 decimal places.
8. **Metric Enrichment**: Compute `weekly_change`, `weekly_change_percentage`, market movement sentiment (`Bull`/`Bear`), and calendar `week_number`.

---

## 5. Sample Silver Output Data

| symbol | week_date | open | high | low | close | volume | weekly_change | weekly_change_percentage | market_movement | validation_status |
|---|---|---|---|---|---|---|---|---|---|---|
| IBM | 2026-08-14 | 190.50 | 195.20 | 189.80 | 194.10 | 5250000 | 3.60 | 1.89 | Bull | VALID |
| AAPL | 2026-08-14 | 172.30 | 178.90 | 171.50 | 177.20 | 9100000 | 4.90 | 2.84 | Bull | VALID |
