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
| `day_date` | `DATE` | No | ISO 8601 `YYYY-MM-DD` | Market trading date |
| `open` | `DOUBLE` | No | positive `> 0`, 2 decimal places | Opening stock price |
| `high` | `DOUBLE` | No | positive `> 0`, `high >= low` | Intraday peak price |
| `low` | `DOUBLE` | No | positive `> 0`, `high >= low` | Intraday trough price |
| `close` | `DOUBLE` | No | positive `> 0`, 2 decimal places | Closing stock price |
| `volume` | `LONG` | No | non-negative `>= 0` | Total shares traded |
| `daily_change` | `DOUBLE` | Yes | `close - open` (2 decimals) | Dollar change intraday |
| `daily_change_percentage` | `DOUBLE` | Yes | `((close - open) / open) * 100` | Percentage change intraday |
| `market_movement` | `STRING` | Yes | `Bull`, `Bear`, or `Neutral` | Intraday market sentiment |
| `thirty_day_avg_open` | `DOUBLE` | Yes | Rolling 30-day window avg | Average opening price |
| `thirty_day_avg_close` | `DOUBLE` | Yes | Rolling 30-day window avg | Average closing price |
| `fifty_two_week_high` | `DOUBLE` | Yes | Rolling 365-day max high | 52-week peak price |
| `fifty_two_week_low` | `DOUBLE` | Yes | Rolling 365-day min low | 52-week minimum price |
| `all_time_high` | `DOUBLE` | Yes | Historical max high price | All-time peak price |
| `all_time_low` | `DOUBLE` | Yes | Historical min low price | All-time minimum price |
| `last_refreshed_date` | `DATE` | Yes | ISO 8601 `YYYY-MM-DD` | API refresh date |
| `validation_status` | `STRING` | No | `VALID` or `INVALID` | Data Quality tag |
| `validation_reason` | `STRING` | Yes | Human-readable explanation | Reason for quarantine tag |
| `year` | `INTEGER` | No | `YYYY` derived from `day_date` | Partition year |
| `month` | `INTEGER` | No | `MM` (1-12) | Partition month |
| `day` | `INTEGER` | No | `DD` (1-31) | Partition day |
| `processed_at` | `TIMESTAMP`| No | UTC timestamp | Transformation execution time |
| `previous_close` | `DOUBLE` | Yes | Window lag `lag(close, 1)` | Prior session closing price |
| `previous_open` | `DOUBLE` | Yes | Window lag `lag(open, 1)` | Prior session opening price |
| `previous_high` | `DOUBLE` | Yes | Window lag `lag(high, 1)` | Prior session high price |
| `previous_low` | `DOUBLE` | Yes | Window lag `lag(low, 1)` | Prior session low price |

---

## 4. Transformation Sequence (Bronze → Silver)

1. **JSON Flattening**: Explode raw Alpha Vantage `Time Series (Daily)` map into key (`date`) and struct value (`open`, `high`, `low`, `close`, `volume`).
2. **Whitespace Trimming & Uppercase**: Trim spaces from string values; convert `symbol` to uppercase.
3. **Fake Null Normalization**: Map tokens (`""`, `"n/a"`, `"none"`, `"-"`) to PySpark `NULL`.
4. **Data Type Casting**: Cast price columns to `DoubleType`, volume to `LongType`, dates to `DateType`.
5. **Data Quality Assertions**: Evaluate assertions (non-null essentials, positive prices, `high >= low`). Mark rows `VALID` / `INVALID`.
6. **Deduplication**: Drop duplicates on composite business key `(symbol, day_date)`.
7. **Rounding**: Round financial metrics (`open`, `high`, `low`, `close`) to 2 decimal places.
8. **Metric Enrichment**: Compute intraday change, change percentage, Bull/Bear movement, and windowed lag metrics.

---

## 5. Sample Silver Output Data

| symbol | day_date | open | high | low | close | volume | daily_change | daily_change_percentage | market_movement | validation_status |
|---|---|---|---|---|---|---|---|---|---|---|
| IBM | 2026-08-14 | 192.50 | 195.20 | 191.80 | 194.10 | 1250000 | 1.60 | 0.83 | Bull | VALID |
| IBM | 2026-08-13 | 191.00 | 193.50 | 190.20 | 192.50 | 980000 | 1.50 | 0.79 | Bull | VALID |
| AAPL | 2026-08-14 | 175.30 | 178.90 | 174.50 | 177.20 | 2100000 | 1.90 | 1.08 | Bull | VALID |
