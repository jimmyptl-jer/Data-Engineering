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

### Partitioning Strategy
Written to S3 partitioned by pipeline execution date:
```text
year=YYYY/month=MM/day=DD/hour=HH/minute=MM/format={parquet|csv}/
```

---

## 3. Complete Schema & Field Specifications (All 57 Attributes)

### Identity & Classification Fields

| # | Field Name | Data Type | Nullable | Canonical Format / Rule | Description |
|---|---|---|---|---|---|
| 1 | `symbol` | `STRING` | No | Uppercase (e.g., `IBM`, `AAPL`) | Stock ticker code |
| 2 | `asset_type` | `STRING` | Yes | Second word extracted via `split(" ")[1]` (e.g. `Stock`) | Security asset classification |
| 3 | `company_name` | `STRING` | Yes | Lowercase text | Legal corporation name |
| 4 | `cik` | `INTEGER` | Yes | Numeric CIK code | SEC Central Index Key |
| 5 | `exchange` | `STRING` | Yes | Uppercase (e.g., `NYSE`, `NASDAQ`) | Listing exchange |
| 6 | `currency` | `STRING` | Yes | ISO 4217 uppercase (`USD`) | Financial currency |
| 7 | `country` | `STRING` | Yes | Default: `Unknown` | Country of domicile |
| 8 | `sector` | `STRING` | Yes | Lowercase text, Default: `Unknown` | Industry sector classification |
| 9 | `industry` | `STRING` | Yes | Lowercase text, Default: `Unknown` | Specific industry specialization |
| 10 | `official_site` | `STRING` | Yes | URL string, Default: `Not Available` | Corporate website URL |
| 11 | `fiscal_year_end` | `STRING` | Yes | Lowercase text (e.g., `december`) | Fiscal year-end month |

### Financial Metrics & Valuation Fields

| # | Field Name | Data Type | Nullable | Canonical Format / Rule | Description |
|---|---|---|---|---|---|
| 12 | `latest_quarter` | `DATE` | Yes | ISO 8601 `YYYY-MM-DD` | End date of latest reported fiscal quarter |
| 13 | `market_cap` | `LONG` | Yes | Numeric integer | Total market capitalization ($) |
| 14 | `ebitda` | `LONG` | Yes | Numeric integer | Earnings before interest, taxes, depreciation |
| 15 | `pe_ratio` | `DOUBLE` | Yes | Floating point number | Price-to-Earnings ratio |
| 16 | `peg_ratio` | `DOUBLE` | Yes | Floating point number | Price/Earnings-to-Growth ratio |
| 17 | `book_value` | `DOUBLE` | Yes | Floating point currency | Book value per share |
| 18 | `dividend_per_share` | `DOUBLE` | Yes | Floating point currency | Annual dividend payment per share |
| 19 | `dividend_yield` | `DOUBLE` | Yes | Percentage decimal | Annual dividend yield |
| 20 | `eps` | `DOUBLE` | Yes | Floating point currency | Earnings Per Share (TTM) |
| 21 | `revenue_per_share_ttm` | `DOUBLE` | Yes | Floating point currency | Revenue per share (TTM) |
| 22 | `profit_margin` | `DOUBLE` | Yes | Floating point percentage | Profit margin ratio |
| 23 | `operating_margin_ttm` | `DOUBLE` | Yes | Floating point percentage | Operating margin ratio (TTM) |
| 24 | `return_on_assets_ttm` | `DOUBLE` | Yes | Floating point percentage | Return on assets (TTM) |
| 25 | `return_on_equity_ttm` | `DOUBLE` | Yes | Floating point percentage | Return on equity (TTM) |
| 26 | `revenue_ttm` | `LONG` | Yes | Numeric integer | Total trailing 12-month revenue |
| 27 | `gross_profit_ttm` | `LONG` | Yes | Numeric integer | Gross profit (TTM) |
| 28 | `diluted_eps_ttm` | `DOUBLE` | Yes | Floating point currency | Diluted earnings per share (TTM) |
| 29 | `quarterly_earnings_growth_yoy` | `DOUBLE` | Yes | Floating point percentage | YoY quarterly earnings growth |
| 30 | `quarterly_revenue_growth_yoy` | `DOUBLE` | Yes | Floating point percentage | YoY quarterly revenue growth |

### Analyst Rating Fields

| # | Field Name | Data Type | Nullable | Canonical Format / Rule | Description |
|---|---|---|---|---|---|
| 31 | `analyst_target_price` | `DOUBLE` | Yes | Floating point currency | Wall Street consensus target price |
| 32 | `analyst_rating_strong_buy` | `INTEGER` | Yes | Integer count | Analyst Strong Buy rating count |
| 33 | `analyst_rating_buy` | `INTEGER` | Yes | Integer count | Analyst Buy rating count |
| 34 | `analyst_rating_hold` | `INTEGER` | Yes | Integer count | Analyst Hold rating count |
| 35 | `analyst_rating_sell` | `INTEGER` | Yes | Integer count | Analyst Sell rating count |
| 36 | `analyst_rating_strong_sell` | `INTEGER` | Yes | Integer count | Analyst Strong Sell rating count |

### Advanced Valuation & Technical Fields

| # | Field Name | Data Type | Nullable | Canonical Format / Rule | Description |
|---|---|---|---|---|---|
| 37 | `trailing_pe` | `DOUBLE` | Yes | Floating point number | Trailing 12-month P/E ratio |
| 38 | `forward_pe` | `DOUBLE` | Yes | Floating point number | Forward 12-month projected P/E ratio |
| 39 | `price_to_sales_ratio_ttm` | `DOUBLE` | Yes | Floating point number | Price-to-Sales ratio (TTM) |
| 40 | `price_to_book_ratio` | `DOUBLE` | Yes | Floating point number | Price-to-Book ratio |
| 41 | `ev_to_revenue` | `DOUBLE` | Yes | Floating point number | Enterprise Value-to-Revenue ratio |
| 42 | `ev_to_ebitda` | `DOUBLE` | Yes | Floating point number | Enterprise Value-to-EBITDA ratio |
| 43 | `beta` | `DOUBLE` | Yes | Floating point number | Stock volatility Beta vs. market |
| 44 | `fifty_two_week_high` | `DOUBLE` | Yes | Floating point currency | 52-week peak stock price |
| 45 | `fifty_two_week_low` | `DOUBLE` | Yes | Floating point currency | 52-week trough stock price |
| 46 | `fifty_day_moving_average` | `DOUBLE` | Yes | Floating point currency | 50-day moving average price |
| 47 | `two_hundred_day_moving_average` | `DOUBLE` | Yes | Floating point currency | 200-day moving average price |

### Ownership & Share Structure Fields

| # | Field Name | Data Type | Nullable | Canonical Format / Rule | Description |
|---|---|---|---|---|---|
| 48 | `shares_outstanding` | `LONG` | Yes | Total share count | Total shares outstanding |
| 49 | `shares_float` | `LONG` | Yes | Total share count | Publicly tradable float share count |
| 50 | `percent_insiders` | `DOUBLE` | Yes | Floating point percentage | Percentage held by corporate insiders |
| 51 | `percent_institutions` | `DOUBLE` | Yes | Floating point percentage | Percentage held by institutional investors |

### Dividend & Calendar Fields

| # | Field Name | Data Type | Nullable | Canonical Format / Rule | Description |
|---|---|---|---|---|---|
| 52 | `dividend_date` | `DATE` | Yes | ISO 8601 `YYYY-MM-DD` | Dividend payment date |
| 53 | `ex_dividend_date` | `DATE` | Yes | ISO 8601 `YYYY-MM-DD` | Ex-dividend date |

### Execution Metadata Fields

| # | Field Name | Data Type | Nullable | Canonical Format / Rule | Description |
|---|---|---|---|---|---|
| 54 | `year` | `INTEGER` | No | `YYYY` execution year via `year(current_date())` | Partition year |
| 55 | `month` | `INTEGER` | No | `MM` (1-12) via `month(current_date())` | Partition month |
| 56 | `day` | `INTEGER` | No | `DD` (1-31) via `dayofmonth(current_date())` | Partition day |
| 57 | `processed_at` | `TIMESTAMP` | No | UTC → `Asia/Kolkata` timezone | Transformation execution timestamp |

---

## 4. Transformation Sequence (Bronze → Silver)

The following 10 steps are executed in order by `silver_transform_overview()`:

| Step | Code Banner | Operation | Detail |
|------|-------------|-----------|--------|
| 1 | `COMPANY OVERVIEW — BRONZE → SILVER` | **Start** | Log transformation start. |
| 2 | `SELECT 52 BUSINESS COLUMNS` | **Column Selection** | Select 52 standard business columns from raw Alpha Vantage PascalCase Bronze payload using `OVERVIEW_BUSINESS_COLUMNS` constant. |
| 3 | `RENAME COLUMNS TO SNAKE_CASE` | **Column Renaming** | Convert all 52 API PascalCase field names to Python `snake_case` using `OVERVIEW_COLUMN_MAPPING` dict (e.g. `MarketCapitalization` → `market_cap`, `52WeekHigh` → `fifty_two_week_high`). |
| 4 | `TRIM WHITESPACE & STANDARDIZE CASING` | **String Cleaning** | Trim whitespace on 9 string columns. Remove non-alphanumeric characters via `regexp_replace("[^A-Za-z &]", "")`. Uppercase code fields (`symbol`, `exchange`, `currency`). Extract secondary word from `asset_type` via `split(" ")[1]`. Lowercase text fields (`company_name`, `industry`, `sector`, `fiscal_year_end`). |
| 5 | `NORMALIZE FAKE NULL VALUES` | **Fake Null Normalization** | Map `""`, `"n/a"`, `"na"`, `"null"`, `"none"`, `"-"` → PySpark `NULL` across all 52 `OVERVIEW_HASH_COLUMNS`. |
| 6 | `DEFAULT VALUE IMPUTATION` | **Default Fill** | Fill missing text with sensible defaults: `country` → `"Unknown"`, `sector` → `"Unknown"`, `industry` → `"Unknown"`, `official_site` → `"Not Available"`. |
| 7 | `EXPLICIT DATA TYPE CASTING` | **Type Casting** | Cast 6 integer columns to `IntegerType`, 6 long columns to `LongType`, 28 double columns to `DoubleType`, 3 date columns to `DateType` via `to_date("yyyy-MM-dd")`. |
| 8 | `PARTITION KEYS & PROCESSING TIMESTAMP` | **Partition Metadata** | Generate `year`, `month`, `day` from `current_date()`. Add `processed_at` via `from_utc_timestamp(current_timestamp(), "Asia/Kolkata")`. |
| 9 | `COMPANY OVERVIEW ROW COUNT METRICS` | **Row Count Metrics** | Log `total_out` count. |
| 10 | `COMPANY OVERVIEW TRANSFORMATION COMPLETED` | **Return** | Return Silver DataFrame. |

---

## 5. Type Casting Groups (from code)

| Group | PySpark Type | Column Count | Columns |
|-------|-------------|-------------|---------|
| **Integer** | `IntegerType()` | 6 | `cik`, `analyst_rating_strong_buy`, `analyst_rating_buy`, `analyst_rating_hold`, `analyst_rating_sell`, `analyst_rating_strong_sell` |
| **Long** | `LongType()` | 6 | `market_cap`, `ebitda`, `revenue_ttm`, `gross_profit_ttm`, `shares_outstanding`, `shares_float` |
| **Double** | `DoubleType()` | 28 | `pe_ratio`, `peg_ratio`, `book_value`, `dividend_per_share`, `dividend_yield`, `eps`, `revenue_per_share_ttm`, `profit_margin`, `operating_margin_ttm`, `return_on_assets_ttm`, `return_on_equity_ttm`, `diluted_eps_ttm`, `quarterly_earnings_growth_yoy`, `quarterly_revenue_growth_yoy`, `analyst_target_price`, `trailing_pe`, `forward_pe`, `price_to_sales_ratio_ttm`, `price_to_book_ratio`, `ev_to_revenue`, `ev_to_ebitda`, `beta`, `fifty_two_week_high`, `fifty_two_week_low`, `fifty_day_moving_average`, `two_hundred_day_moving_average`, `percent_insiders`, `percent_institutions` |
| **Date** | `DateType()` | 3 | `latest_quarter`, `dividend_date`, `ex_dividend_date` |

---

## 6. Sample Silver Output Data

| symbol | asset_type | company_name | exchange | currency | country | sector | industry | market_cap | pe_ratio | eps |
|---|---|---|---|---|---|---|---|---|---|---|
| IBM | Stock | international business machines corp | NYSE | USD | USA | technology | it services | 185000000000 | 21.45 | 9.12 |
| AAPL | Stock | apple inc | NASDAQ | USD | USA | technology | consumer electronics | 3200000000000 | 33.10 | 6.58 |
