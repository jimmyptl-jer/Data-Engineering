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

| # | Field Name | Data Type | Nullable | Canonical Format / Rule | Description |
|---|---|---|---|---|---|
| 1 | `symbol` | `STRING` | No | Uppercase (e.g., `IBM`, `AAPL`) | Stock ticker code |
| 2 | `week_date` | `DATE` | No | ISO 8601 `YYYY-MM-DD` | Ending date of weekly trading period |
| 3 | `open` | `DOUBLE` | No | positive `> 0`, 2 decimal places | Weekly opening stock price |
| 4 | `high` | `DOUBLE` | No | positive `> 0`, `high >= low` | Weekly peak price |
| 5 | `low` | `DOUBLE` | No | positive `> 0`, `high >= low` | Weekly trough price |
| 6 | `close` | `DOUBLE` | No | positive `> 0`, 2 decimal places | Weekly closing stock price |
| 7 | `volume` | `LONG` | No | non-negative `>= 0` | Total weekly trading volume |
| 8 | `weekly_change` | `DOUBLE` | Yes | `close - open` (2 decimals) | Dollar change over week |
| 9 | `weekly_change_percentage` | `DOUBLE` | Yes | `((close - open) / open) * 100` | Percentage change over week |
| 10 | `market_movement` | `STRING` | Yes | `Bull`, `Bear`, or `Neutral` | Overall weekly trend direction |
| 11 | `year` | `INTEGER` | No | `YYYY` derived from `week_date` | Partition year |
| 12 | `month` | `INTEGER` | No | `MM` (1-12) from `week_date` | Partition month |
| 13 | `week_number` | `INTEGER` | No | Calendar week number (1-53) via `weekofyear()` | ISO week of year |
| 14 | `last_refreshed_date` | `DATE` | Yes | ISO 8601 `YYYY-MM-DD` (renamed from `last_refreshed`) | API refresh date |
| 15 | `validation_status` | `STRING` | No | `VALID` or `INVALID` | Data Quality tag |
| 16 | `validation_reason` | `STRING` | Yes | Human-readable explanation | Reason for quarantine (NULL if VALID) |
| 17 | `processed_at` | `TIMESTAMP`| No | UTC → `Asia/Kolkata` timezone | Transformation execution timestamp |

---

## 4. Transformation Sequence (Bronze → Silver)

The following 15 steps are executed in order by `transform_weekly_timeseries()`:

| Step | Code Banner | Operation | Detail |
|------|-------------|-----------|--------|
| 1 | `WEEKLY TIME SERIES — BRONZE → SILVER` | **Start** | Log transformation start. |
| 2 | `FLATTEN JSON MAP` | **JSON Flattening** | Extract `Meta Data.2. Symbol` → `symbol`, `Meta Data.3. Last Refreshed` → `last_refreshed`. Explode `Weekly Time Series` map into key=`week_date` and struct=`weekly_data`. |
| 3 | `SELECT + ALIAS OHLCV FIELDS` | **Field Selection** | Extract `weekly_data.1. open` → `open`, `weekly_data.2. high` → `high`, `weekly_data.3. low` → `low`, `weekly_data.4. close` → `close`, `weekly_data.5. volume` → `volume`. |
| 4 | `TRIM WHITESPACE & UPPERCASE SYMBOL` | **Whitespace Trimming** | Trim all 8 string columns. Convert `symbol` to uppercase via `upper()`. |
| 5 | `NORMALIZE FAKE NULL VALUES` | **Fake Null Normalization** | Map `""`, `"n/a"`, `"na"`, `"null"`, `"none"`, `"-"` → PySpark `NULL`. |
| 6 | `EXPLICIT DATA TYPE CASTING` | **Type Casting** | `week_date` → `DateType`, `last_refreshed` → `DateType`, `open/high/low/close` → `DoubleType`, `volume` → `LongType`. |
| 7 | `APPLY DATA QUALITY VALIDATION` | **Validation Status** | Tag each row `VALID` or `INVALID` based on 14 rules: non-null checks + positive prices + `volume >= 0` + `high >= low`. |
| 8 | `GENERATE VALIDATION REASONS` | **Validation Reason** | Generate human-readable failure reason string for invalid rows (e.g. `"Missing symbol"`, `"High price cannot be lower than low price"`). |
| 9 | `FILTER VALID ROWS FOR SILVER LAYER` | **Valid Row Filtering** | Keep only `validation_status == 'VALID'`. Count and log quarantined `INVALID` records. |
| 10 | `DEDUPLICATE WEEKLY TIME SERIES` | **Deduplication** | `dropDuplicates(["symbol", "week_date"])` on composite business key. |
| 11 | `ROUND NUMERIC PRICES` | **Rounding** | Round `open`, `high`, `low`, `close` to 2 decimal places. |
| 12 | `METRIC ENRICHMENT & PARTITION METADATA` | **Enrichment + Partitions** | Compute `weekly_change = close - open`, `weekly_change_percentage`, `market_movement` (Bull/Bear/Neutral). Extract `year`, `month`, `week_number` from `week_date`. Add `processed_at` timestamp. Rename `last_refreshed` → `last_refreshed_date`. |
| 13 | `FINAL SELECTION & ORDERING` | **Final Select + Sort** | Select 17 output columns in canonical order. Sort by `symbol ASC`, `week_date DESC`. |
| 14 | `WEEKLY TIME SERIES ROW COUNT METRICS` | **Row Count Metrics** | Log `total_in`, `total_out`, `valid`, `invalid` counts. |
| 15 | `WEEKLY TIME SERIES TRANSFORMATION COMPLETED` | **Return** | Return Silver DataFrame. |

---

## 5. Data Quality Validation Rules

| # | Rule | Condition | Tag |
|---|------|-----------|-----|
| 1 | Symbol required | `symbol IS NULL` | INVALID |
| 2 | Week date required | `week_date IS NULL` | INVALID |
| 3 | Last refreshed required | `last_refreshed IS NULL` | INVALID |
| 4 | Open required | `open IS NULL` | INVALID |
| 5 | High required | `high IS NULL` | INVALID |
| 6 | Low required | `low IS NULL` | INVALID |
| 7 | Close required | `close IS NULL` | INVALID |
| 8 | Volume required | `volume IS NULL` | INVALID |
| 9 | Positive open | `open <= 0` | INVALID |
| 10 | Positive high | `high <= 0` | INVALID |
| 11 | Positive low | `low <= 0` | INVALID |
| 12 | Positive close | `close <= 0` | INVALID |
| 13 | Non-negative volume | `volume < 0` | INVALID |
| 14 | High >= Low | `high < low` | INVALID |

---

## 6. Sample Silver Output Data

| symbol | week_date | open | high | low | close | volume | weekly_change | weekly_change_percentage | market_movement | validation_status |
|---|---|---|---|---|---|---|---|---|---|---|
| IBM | 2026-08-14 | 190.50 | 195.20 | 189.80 | 194.10 | 5250000 | 3.60 | 1.89 | Bull | VALID |
| AAPL | 2026-08-14 | 172.30 | 178.90 | 171.50 | 177.20 | 9100000 | 4.90 | 2.84 | Bull | VALID |
