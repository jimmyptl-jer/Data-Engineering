# Silver Dataset Specification — Company Overview (`company_overview`)

## 1. Overview & Data Purpose
- **Dataset Identifier**: `company_overview`
- **Data Source**: Alpha Vantage REST API (`OVERVIEW`)
- **Ingestion Frequency**: Daily snapshot / On-Demand refresh
- **Target Medallion Layer**: Silver Data Lake (`s3a://graywolf--data--lake/stock/silver/datasource=alphavantage/dataset=company_overview/`)
- **Downstream Consumers**: Gold Company Dataset (`gold/company_dataset`), Sector valuation reports, P/E ratio screening models, Fundamental investment analysis.

---

## 2. Data Contract & Identity

### Business Key
The primary identity key for every record in this dataset is:
```text
symbol
```
Every company has at most **one master fundamental record** per snapshot.

### Incremental Strategy
Hash-based comparison using SHA-256 digest on 52 business columns (`WATERMARK_STRATEGIES['company_overview'] = 'hash_based'`).

---

## 3. Key Schema & Field Specifications (Selected Core Attributes)

This dataset standardizes 52 fundamental business attributes from PascalCase to snake_case:

| Field Name | Data Type | Nullable | Canonical Format / Rule | Description |
|---|---|---|---|---|
| `symbol` | `STRING` | No | Uppercase (e.g., `IBM`, `AAPL`) | Stock ticker code |
| `asset_type` | `STRING` | Yes | Standardized text (`Stock`, `ETF`) | Security asset classification |
| `company_name` | `STRING` | Yes | Lowercase text | Legal corporation name |
| `cik` | `INTEGER` | Yes | Numeric CIK code | SEC Central Index Key |
| `exchange` | `STRING` | Yes | Uppercase (e.g., `NYSE`, `NASDAQ`) | Listing exchange |
| `currency` | `STRING` | Yes | ISO 4217 uppercase (`USD`) | Financial currency |
| `country` | `STRING` | Yes | Default: `Unknown` | Country of domicile |
| `sector` | `STRING` | Yes | Lowercase text, Default: `Unknown` | Industry sector classification |
| `industry` | `STRING` | Yes | Lowercase text, Default: `Unknown` | Specific industry specialization |
| `official_site` | `STRING` | Yes | URL string, Default: `Not Available` | Corporate website URL |
| `market_cap` | `LONG` | Yes | Numeric integer | Total market capitalization ($) |
| `ebitda` | `LONG` | Yes | Numeric integer | Earnings before interest, taxes, dep. |
| `pe_ratio` | `DOUBLE` | Yes | Floating point number | Price-to-Earnings ratio |
| `peg_ratio` | `DOUBLE` | Yes | Floating point number | Price/Earnings-to-Growth ratio |
| `book_value` | `DOUBLE` | Yes | Floating point currency | Book value per share |
| `dividend_per_share` | `DOUBLE` | Yes | Floating point currency | Annual dividend payment per share |
| `dividend_yield` | `DOUBLE` | Yes | Percentage decimal | Annual dividend yield |
| `eps` | `DOUBLE` | Yes | Floating point currency | Earnings Per Share (TTM) |
| `profit_margin` | `DOUBLE` | Yes | Floating point percentage | Profit margin ratio |
| `revenue_ttm` | `LONG` | Yes | Numeric integer | Total trailing 12-month revenue |
| `shares_outstanding` | `LONG` | Yes | Total share count | Shares outstanding |
| `fifty_two_week_high` | `DOUBLE` | Yes | Floating point currency | 52-week peak stock price |
| `fifty_two_week_low` | `DOUBLE` | Yes | Floating point currency | 52-week trough stock price |
| `fifty_day_moving_average`| `DOUBLE` | Yes | Floating point currency | 50-day moving average price |
| `two_hundred_day_moving_average`| `DOUBLE` | Yes | Floating point currency | 200-day moving average price |
| `processed_at` | `TIMESTAMP`| No | UTC timestamp | Transformation execution time |

---

## 4. Transformation Sequence (Bronze → Silver)

1. **Column Selection**: Extract 52 standard business fields from raw PascalCase Bronze payload.
2. **Column Renaming**: Convert API PascalCase keys to standard Python `snake_case` (e.g. `MarketCapitalization` → `market_cap`, `52WeekHigh` → `fifty_two_week_high`).
3. **String Cleaning**: Trim whitespace, remove special non-alphanumeric noise (`regexp_replace`), uppercase code attributes (`symbol`, `exchange`, `currency`), and lowercase descriptive text (`company_name`, `sector`, `industry`).
4. **Fake Null Normalization**: Convert placeholder values (`"n/a"`, `"none"`, `"-"`) to PySpark `NULL`.
5. **Default Imputation**: Fill missing text fields with sensible defaults (`country` → `"Unknown"`, `sector` → `"Unknown"`, `official_site` → `"Not Available"`).
6. **Explicit Type Casting**: Cast text representations to explicit PySpark types (`IntegerType`, `LongType`, `DoubleType`, `DateType`).
7. **Partition Keys & Timestamp**: Generate execution partition metadata (`year`, `month`, `day`) and `processed_at` timestamp.

---

## 5. Sample Silver Output Data

| symbol | asset_type | company_name | exchange | currency | country | sector | industry | market_cap | pe_ratio | eps |
|---|---|---|---|---|---|---|---|---|---|---|
| IBM | Stock | international business machines corp | NYSE | USD | USA | technology | it services | 185000000000 | 21.45 | 9.12 |
| AAPL | Stock | apple inc | NASDAQ | USD | USA | technology | consumer electronics | 3200000000000 | 33.10 | 6.58 |
