# Silver Dataset Specification — Daily Time Series (`daily_time_series`)

## 1. Overview & Data Purpose
- **Dataset Identifier**: `daily_time_series`
- **Data Source**: Alpha Vantage REST API (`TIME_SERIES_DAILY`)
- **Ingestion Frequency**: Weekday daily batch (2:00 PM UTC)
- **Target Medallion Layer**: Silver Data Lake (`s3a://graywolf--data--lake/stock/silver/datasource=alphavantage/dataset=daily_time_series/`)
- **Downstream Consumers**: Gold Company Dataset (`gold/company_dataset`), Technical Analysis dashboards, Intraday price movement models.

---

## 2. Data Contract & Identity

### Business Key
The composite unique identifier for every record in this dataset is:
```text
(symbol, day_date)
```
Every stock symbol has at most **one price record per trading day**.

### Incremental Strategy
Date-based watermark comparison (`WATERMARK_STRATEGIES['daily_time_series'] = 'date_based'`). Filters on `day_date > watermark_value`.

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
| 2 | `day_date` | `DATE` | No | ISO 8601 `YYYY-MM-DD` | Market trading date |
| 3 | `open` | `DOUBLE` | No | positive `> 0`, 2 decimal places | Opening stock price |
| 4 | `high` | `DOUBLE` | No | positive `> 0`, `high >= low` | Intraday peak price |
| 5 | `low` | `DOUBLE` | No | positive `> 0`, `high >= low` | Intraday trough price |
| 6 | `close` | `DOUBLE` | No | positive `> 0`, 2 decimal places | Closing stock price |
| 7 | `volume` | `LONG` | No | non-negative `>= 0` | Total shares traded |
| 8 | `daily_change` | `DOUBLE` | Yes | `close - open` (2 decimals) | Dollar change intraday |
| 9 | `daily_change_percentage` | `DOUBLE` | Yes | `((close - open) / open) * 100` | Percentage change intraday |
| 10 | `market_movement` | `STRING` | Yes | `Bull`, `Bear`, or `Neutral` | Intraday market sentiment |
| 11 | `thirty_day_avg_open` | `DOUBLE` | Yes | Rolling 30-day window avg | Average opening price (last 30 days) |
| 12 | `thirty_day_avg_close` | `DOUBLE` | Yes | Rolling 30-day window avg | Average closing price (last 30 days) |
| 13 | `fifty_two_week_high` | `DOUBLE` | Yes | Rolling 365-day max high | 52-week peak price |
| 14 | `fifty_two_week_low` | `DOUBLE` | Yes | Rolling 365-day min low | 52-week minimum price |
| 15 | `all_time_high` | `DOUBLE` | Yes | Historical max high price | All-time peak price |
| 16 | `all_time_low` | `DOUBLE` | Yes | Historical min low price | All-time minimum price |
| 17 | `last_refreshed_date` | `DATE` | Yes | ISO 8601 `YYYY-MM-DD` | API last refreshed date (renamed from `last_refreshed`) |
| 18 | `validation_status` | `STRING` | No | `VALID` or `INVALID` | Data Quality tag |
| 19 | `validation_reason` | `STRING` | Yes | Human-readable explanation | Reason for quarantine (NULL if VALID) |
| 20 | `year` | `INTEGER` | No | `YYYY` derived from `day_date` | Partition year |
| 21 | `month` | `INTEGER` | No | `MM` (1-12) from `day_date` | Partition month |
| 22 | `day` | `INTEGER` | No | `DD` (1-31) from `day_date` | Partition day |
| 23 | `processed_at` | `TIMESTAMP`| No | UTC → `Asia/Kolkata` timezone | Transformation execution timestamp |
| 24 | `previous_close` | `DOUBLE` | Yes | `lag(close, 1)` over `(symbol, day_date)` | Prior session closing price |
| 25 | `previous_open` | `DOUBLE` | Yes | `lag(open, 1)` over `(symbol, day_date)` | Prior session opening price |
| 26 | `previous_high` | `DOUBLE` | Yes | `lag(high, 1)` over `(symbol, day_date)` | Prior session high price |
| 27 | `previous_low` | `DOUBLE` | Yes | `lag(low, 1)` over `(symbol, day_date)` | Prior session low price |

---

## 4. Transformation Sequence (Bronze → Silver)

The following 19 steps are executed in order by `silver_transform_daily_timeseries()`:

| Step | Code Banner | Operation | Detail |
|------|-------------|-----------|--------|
| 1 | `DAILY TIME SERIES — BRONZE → SILVER` | **Start** | Log transformation start. |
| 2 | `FLATTEN NESTED JSON STRUCTURE` | **JSON Flattening** | Extract `Meta Data.2. Symbol` as `symbol`, `Meta Data.3. Last Refreshed` as `last_refreshed`. Explode `Time Series (Daily)` map into key=`day_date` and struct=`daily_data`. |
| 3 | `SELECT + ALIAS OHLCV FIELDS` | **Field Selection** | Extract `daily_data.1. open` → `open`, `daily_data.2. high` → `high`, `daily_data.3. low` → `low`, `daily_data.4. close` → `close`, `daily_data.5. volume` → `volume`. |
| 4 | `TRIM WHITESPACE` | **Whitespace Trimming** | Trim leading/trailing whitespace on all 8 string columns. Convert `symbol` to uppercase (`upper()`). |
| 5 | `NORMALIZE FAKE NULL VALUES` | **Fake Null Normalization** | Map `""`, `"n/a"`, `"na"`, `"null"`, `"none"`, `"-"` → PySpark `NULL` using `lower(col).isin(fake_null_values)`. |
| 6 | `DATA TYPE CONVERSIONS` | **Type Casting** | `day_date` → `DateType`, `last_refreshed` → `DateType`, `open/high/low/close` → `DoubleType`, `volume` → `LongType`. |
| 7 | `APPLY DATA QUALITY VALIDATION` | **Validation Status** | Tag each row `VALID` or `INVALID` based on 14 rules: non-null checks for 8 fields + positive price checks + `volume >= 0` + `high >= low`. |
| 8 | `GENERATE VALIDATION REASONS` | **Validation Reason** | Generate human-readable failure reason string for invalid rows (e.g. `"Missing symbol"`, `"Open price must be greater than zero"`). |
| 9 | `FILTER VALID ROWS FOR SILVER LAYER` | **Valid Row Filtering** | Keep only `validation_status == 'VALID'` rows. Count and log quarantined `INVALID` records. |
| 10 | `DEDUPLICATE DAILY TIME SERIES` | **Deduplication** | `dropDuplicates(["symbol", "day_date"])` on composite business key. |
| 11 | `ROUND FINANCIAL PRICES` | **Rounding** | Round `open`, `high`, `low`, `close` to 2 decimal places. |
| 12 | `FINANCIAL METRIC ENRICHMENT` | **Metric Enrichment** | Compute `daily_change = close - open`, `daily_change_percentage = ((close - open) / open) * 100`, `market_movement` (Bull/Bear/Neutral). Add `processed_at` timestamp. Rename `last_refreshed` → `last_refreshed_date`. |
| 13 | `ROLLING 30-DAY AVERAGE OPEN & CLOSE` | **30-Day Rolling Average** | Filter last 30 days. `groupBy("symbol").agg(avg("close"), avg("open"))`. Left join back to main DataFrame. |
| 14 | `ROLLING 52-WEEK HIGH & LOW` | **52-Week Range** | Filter last 365 days. `groupBy("symbol").agg(max("high"), min("low"))`. Left join back. |
| 15 | `ALL-TIME HIGH & LOW` | **All-Time Range** | `groupBy("symbol").agg(max("high"), min("low"))` over entire dataset history. Left join back. |
| 16 | `PARTITION KEYS & LAG FEATURES` | **Partition Keys + Window Lags** | Extract `year`, `month`, `day` from `day_date`. Compute `previous_close`, `previous_open`, `previous_high`, `previous_low` via `lag(col, 1).over(Window.partitionBy("symbol").orderBy("day_date"))`. |
| 17 | `FINAL SILVER SCHEMA SELECTION & SORTING` | **Final Select + Sort** | Select 27 output columns in canonical order. Sort by `symbol ASC`, `day_date DESC`. |
| 18 | `DAILY TIME SERIES ROW COUNT METRICS` | **Row Count Metrics** | Log `total_in`, `total_out`, `valid`, `invalid` counts. |
| 19 | `DAILY TIME SERIES TRANSFORMATION COMPLETED` | **Return** | Return Silver DataFrame. |

---

## 5. Data Quality Validation Rules

| # | Rule | Condition | Tag |
|---|------|-----------|-----|
| 1 | Symbol required | `symbol IS NULL` | INVALID |
| 2 | Date required | `day_date IS NULL` | INVALID |
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

| symbol | day_date | open | high | low | close | volume | daily_change | daily_change_percentage | market_movement | validation_status |
|---|---|---|---|---|---|---|---|---|---|---|
| IBM | 2026-08-14 | 192.50 | 195.20 | 191.80 | 194.10 | 1250000 | 1.60 | 0.83 | Bull | VALID |
| IBM | 2026-08-13 | 191.00 | 193.50 | 190.20 | 192.50 | 980000 | 1.50 | 0.79 | Bull | VALID |
| AAPL | 2026-08-14 | 175.30 | 178.90 | 174.50 | 177.20 | 2100000 | 1.90 | 1.08 | Bull | VALID |
