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

## 3. Complete Schema & Field Specifications (All 53 Attributes)

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
| `fiscal_year_end` | `STRING` | Yes | Lowercase text (e.g., `december`) | Fiscal year-end month |
| `latest_quarter` | `DATE` | Yes | ISO 8601 `YYYY-MM-DD` | End date of latest reported fiscal quarter |
| `market_cap` | `LONG` | Yes | Numeric integer | Total market capitalization ($) |
| `ebitda` | `LONG` | Yes | Numeric integer | Earnings before interest, taxes, dep. |
| `pe_ratio` | `DOUBLE` | Yes | Floating point number | Price-to-Earnings ratio |
| `peg_ratio` | `DOUBLE` | Yes | Floating point number | Price/Earnings-to-Growth ratio |
| `book_value` | `DOUBLE` | Yes | Floating point currency | Book value per share |
| `dividend_per_share` | `DOUBLE` | Yes | Floating point currency | Annual dividend payment per share |
| `dividend_yield` | `DOUBLE` | Yes | Percentage decimal | Annual dividend yield |
| `eps` | `DOUBLE` | Yes | Floating point currency | Earnings Per Share (TTM) |
| `revenue_per_share_ttm` | `DOUBLE` | Yes | Floating point currency | Revenue per share (TTM) |
| `profit_margin` | `DOUBLE` | Yes | Floating point percentage | Profit margin ratio |
| `operating_margin_ttm` | `DOUBLE` | Yes | Floating point percentage | Operating margin ratio (TTM) |
| `return_on_assets_ttm` | `DOUBLE` | Yes | Floating point percentage | Return on assets (TTM) |
| `return_on_equity_ttm` | `DOUBLE` | Yes | Floating point percentage | Return on equity (TTM) |
| `revenue_ttm` | `LONG` | Yes | Numeric integer | Total trailing 12-month revenue |
| `gross_profit_ttm` | `LONG` | Yes | Numeric integer | Gross profit (TTM) |
| `diluted_eps_ttm` | `DOUBLE` | Yes | Floating point currency | Diluted earnings per share (TTM) |
| `quarterly_earnings_growth_yoy` | `DOUBLE` | Yes | Floating point percentage | YoY quarterly earnings growth |
| `quarterly_revenue_growth_yoy` | `DOUBLE` | Yes | Floating point percentage | YoY quarterly revenue growth |
| `analyst_target_price` | `DOUBLE` | Yes | Floating point currency | Wall Street consensus target price |
| `analyst_rating_strong_buy` | `INTEGER` | Yes | Integer count | Analyst Strong Buy rating count |
| `analyst_rating_buy` | `INTEGER` | Yes | Integer count | Analyst Buy rating count |
| `analyst_rating_hold` | `INTEGER` | Yes | Integer count | Analyst Hold rating count |
| `analyst_rating_sell` | `INTEGER` | Yes | Integer count | Analyst Sell rating count |
| `analyst_rating_strong_sell` | `INTEGER` | Yes | Integer count | Analyst Strong Sell rating count |
| `trailing_pe` | `DOUBLE` | Yes | Floating point number | Trailing 12-month P/E ratio |
| `forward_pe` | `DOUBLE` | Yes | Floating point number | Forward 12-month projected P/E ratio |
| `price_to_sales_ratio_ttm` | `DOUBLE` | Yes | Floating point number | Price-to-Sales ratio (TTM) |
| `price_to_book_ratio` | `DOUBLE` | Yes | Floating point number | Price-to-Book ratio |
| `ev_to_revenue` | `DOUBLE` | Yes | Floating point number | Enterprise Value-to-Revenue ratio |
| `ev_to_ebitda` | `DOUBLE` | Yes | Floating point number | Enterprise Value-to-EBITDA ratio |
| `beta` | `DOUBLE` | Yes | Floating point number | Stock volatility Beta relative to market |
| `fifty_two_week_high` | `DOUBLE` | Yes | Floating point currency | 52-week peak stock price |
| `fifty_two_week_low` | `DOUBLE` | Yes | Floating point currency | 52-week trough stock price |
| `fifty_day_moving_average` | `DOUBLE` | Yes | Floating point currency | 50-day moving average price |
| `two_hundred_day_moving_average` | `DOUBLE` | Yes | Floating point currency | 200-day moving average price |
| `shares_outstanding` | `LONG` | Yes | Total share count | Total shares outstanding |
| `shares_float` | `LONG` | Yes | Total share count | Publicly tradable float share count |
| `percent_insiders` | `DOUBLE` | Yes | Floating point percentage | Percentage held by corporate insiders |
| `percent_institutions` | `DOUBLE` | Yes | Floating point percentage | Percentage held by institutional investors |
| `dividend_date` | `DATE` | Yes | ISO 8601 `YYYY-MM-DD` | Dividend payment date |
| `ex_dividend_date` | `DATE` | Yes | ISO 8601 `YYYY-MM-DD` | Ex-dividend date |
| `year` | `INTEGER` | No | `YYYY` derived execution year | Partition year |
| `month` | `INTEGER` | No | `MM` (1-12) derived execution month | Partition month |
| `day` | `INTEGER` | No | `DD` (1-31) derived execution day | Partition day |
| `processed_at` | `TIMESTAMP` | No | UTC timestamp | Transformation execution timestamp |

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
