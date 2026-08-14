# Gold Layer — Master Specification & Preparation Guide

> **Core Philosophy**:
> - **Bronze** = Source Truth (Raw API payload, preserved exactly as received)
> - **Silver** = Trusted Data (Cleaned, typed, validated, deduplicated, auditable)
> - **Gold** = Business Truth (Joined, aggregated, enriched — ready for analytics and dashboards)

---

## 1. What Is the Gold Layer?

Gold is where Silver datasets are **joined together** and **enriched with business logic** to answer real business questions.

Silver answers: *"What was IBM's closing price on August 14th?"*
Gold answers: *"How did IBM's daily price compare to its sector average P/E ratio, 52-week range, and analyst consensus target — and was it a buy signal?"*

```text
                    Silver Datasets                         Gold Datasets
         ┌──────────────────────────┐
         │  daily_time_series       │──┐
         │  (OHLCV per day)         │  │
         └──────────────────────────┘  │
         ┌──────────────────────────┐  │     ┌──────────────────────────────────┐
         │  weekly_time_series      │──┼────►│  company_dataset (CURRENT)       │
         │  (OHLCV per week)        │  │     │  Daily prices + Company overview │
         └──────────────────────────┘  │     └──────────────────────────────────┘
         ┌──────────────────────────┐  │     ┌──────────────────────────────────┐
         │  company_overview        │──┤     │  sector_analysis (FUTURE)        │
         │  (52 fundamentals)       │  │     │  Sector-level aggregations       │
         └──────────────────────────┘  │     └──────────────────────────────────┘
         ┌──────────────────────────┐  │     ┌──────────────────────────────────┐
         │  ticker_reference        │──┤     │  technical_indicators (FUTURE)   │
         │  (FIGI/MIC cross-ref)    │  │     │  Moving averages, RSI, signals   │
         └──────────────────────────┘  │     └──────────────────────────────────┘
         ┌──────────────────────────┐  │     ┌──────────────────────────────────┐
         │  exchanges               │──┘     │  exchange_universe (FUTURE)      │
         │  (MIC reference)         │        │  Tickers + Exchange metadata     │
         └──────────────────────────┘        └──────────────────────────────────┘
```

---

## 2. Current Gold Implementation (`company_dataset`)

### What Exists Today

The pipeline currently builds **one Gold dataset** called `company_dataset` inside `_build_gold_layer()` in `pipeline.py`.

**Join Logic:**
```text
Silver daily_time_series  LEFT JOIN  Silver company_overview  ON  symbol
```

**Columns Selected from Silver Daily:**
- `symbol`, `day_date`, `open`, `high`, `low`, `close`, `volume`
- `daily_change`, `daily_change_percentage`, `market_movement`
- `thirty_day_avg_open`, `thirty_day_avg_close`
- `all_time_high`, `all_time_low`
- `processed_at`

**Columns from Silver Overview:**
- All 52+ company fundamental columns **except** `processed_at` (dropped to avoid column conflict)

**Output Formats:**
- Parquet → `s3a://graywolf--data--lake/stock/gold/source=alphavantage/dataset=company_dataset/.../format=parquet/`
- CSV → `s3a://graywolf--data--lake/stock/gold/source=alphavantage/dataset=company_dataset/.../format=csv/`

---

## 3. What's Missing from the Current Gold Layer

### Gap Analysis

| # | Gap | Problem | Impact |
|---|-----|---------|--------|
| 1 | **Technical indicators live in Silver** | `daily.py` computes 30-day averages, 52-week ranges, all-time high/low, and lag features inside the Silver transform. These are analytic computations that belong in Gold. | Silver is bloated with analytics that downstream consumers may not need. Rerunning Silver forces recomputation of expensive window functions. |
| 2 | **No weekly data in Gold** | Weekly time series is transformed to Silver but never joined into any Gold dataset. | Weekly trend analysis requires manual ad-hoc queries against Silver. |
| 3 | **No ticker reference enrichment** | FIGI codes, ISIN identifiers, and security type metadata from Finnhub are not joined into Gold. | Gold consumers cannot cross-reference Bloomberg FIGIs or filter by security type (Common Stock vs. ETF vs. ADR). |
| 4 | **No exchange metadata** | Exchange names, locales, and MIC codes from Massive/Polygon are not joined. | Cannot enrich Gold records with exchange display names or geographic locale. |
| 5 | **No sector-level aggregations** | No aggregated metrics at sector or industry level (avg P/E, total market cap by sector). | Cannot answer "How does IBM compare to other technology stocks?" |
| 6 | **No buy/sell signal logic** | No computed columns like `is_undervalued`, `analyst_consensus`, `price_vs_target`. | Requires downstream consumers to implement their own signal logic. |

---

## 4. Gold Layer Preparation Steps — Full Implementation Roadmap

### Phase 1: Clean the Silver/Gold Boundary

**Goal**: Move analytics out of Silver, keep Silver focused on clean data contracts.

| Step | Action | Detail |
|------|--------|--------|
| 1.1 | **Remove rolling aggregations from `daily.py`** | Move Steps 13-15 (30-day avg, 52-week range, all-time high/low) out of Silver into a dedicated Gold transform module. |
| 1.2 | **Remove lag features from `daily.py`** | Move `previous_close`, `previous_open`, `previous_high`, `previous_low` window computations to Gold. |
| 1.3 | **Slim the Silver daily schema** | Silver daily should output only: `symbol`, `day_date`, `open`, `high`, `low`, `close`, `volume`, `last_refreshed_date`, `validation_status`, `validation_reason`, `processed_at`, `year`, `month`, `day`. |

### Phase 2: Build Gold Transform Modules

**Goal**: Create dedicated Gold transform modules in `src/stock_pipeline/transform/gold/`.

#### 2.1 — `gold_company_dataset.py` (Enhance Current)

Join Silver Daily + Silver Overview + Silver Ticker Reference

```text
Step 1: Read Silver daily_time_series (clean OHLCV data)
Step 2: Read Silver company_overview (52 fundamental attributes)
Step 3: Read Silver ticker_reference (FIGI, ISIN, security type)
Step 4: Join daily LEFT JOIN overview ON symbol
Step 5: Join result LEFT JOIN ticker_reference ON symbol
Step 6: Compute rolling aggregations:
          - 30-day average open/close
          - 52-week high/low
          - All-time high/low
Step 7: Compute windowed lag features:
          - previous_close, previous_open, previous_high, previous_low
Step 8: Compute daily technical metrics:
          - daily_change = close - open
          - daily_change_percentage = ((close - open) / open) * 100
          - market_movement = Bull / Bear / Neutral
Step 9: Compute valuation signals:
          - price_vs_analyst_target = close / analyst_target_price
          - is_above_50d_ma = close > fifty_day_moving_average
          - is_above_200d_ma = close > two_hundred_day_moving_average
Step 10: Select final Gold schema
Step 11: Write to S3 Gold (Parquet + CSV)
```

#### 2.2 — `gold_sector_analysis.py` (New)

Aggregate company fundamentals at sector level

```text
Step 1: Read Silver company_overview
Step 2: Group by sector
Step 3: Compute sector-level aggregations:
          - sector_avg_pe_ratio = avg(pe_ratio) per sector
          - sector_avg_market_cap = avg(market_cap) per sector
          - sector_total_market_cap = sum(market_cap) per sector
          - sector_company_count = count(*) per sector
          - sector_avg_dividend_yield = avg(dividend_yield) per sector
Step 4: Join back to company-level data for relative positioning:
          - pe_vs_sector_avg = pe_ratio / sector_avg_pe_ratio
Step 5: Write to S3 Gold
```

#### 2.3 — `gold_technical_indicators.py` (New)

Compute technical analysis indicators from Silver daily/weekly data

```text
Step 1: Read Silver daily_time_series
Step 2: Compute Simple Moving Averages (SMA):
          - sma_10 = avg(close) over Window(10 days)
          - sma_20 = avg(close) over Window(20 days)
          - sma_50 = avg(close) over Window(50 days)
          - sma_200 = avg(close) over Window(200 days)
Step 3: Compute Exponential Moving Average (EMA):
          - ema_12, ema_26
Step 4: Compute MACD:
          - macd_line = ema_12 - ema_26
          - signal_line = ema(macd_line, 9)
          - macd_histogram = macd_line - signal_line
Step 5: Compute RSI (Relative Strength Index):
          - 14-day RSI based on avg gain / avg loss
Step 6: Compute Bollinger Bands:
          - upper_band = sma_20 + (2 * stddev(close, 20))
          - lower_band = sma_20 - (2 * stddev(close, 20))
Step 7: Compute Volume Weighted Average Price (VWAP)
Step 8: Generate trading signals:
          - golden_cross = sma_50 crosses above sma_200
          - death_cross = sma_50 crosses below sma_200
          - rsi_overbought = rsi > 70
          - rsi_oversold = rsi < 30
Step 9: Write to S3 Gold
```

#### 2.4 — `gold_exchange_universe.py` (New)

Join ticker reference with exchange metadata

```text
Step 1: Read Silver ticker_reference
Step 2: Read Silver exchanges
Step 3: Join ticker_reference LEFT JOIN exchanges ON mic = operating_mic
Step 4: Enrich with exchange display name, locale, asset_class
Step 5: Compute exchange-level ticker counts:
          - tickers_per_exchange = count(symbol) per operating_mic
          - tickers_per_type = count(symbol) per type (Common Stock, ETF, ADR)
Step 6: Write to S3 Gold
```

### Phase 3: Update Pipeline Orchestration

| Step | Action | Detail |
|------|--------|--------|
| 3.1 | **Update `pipeline.py`** | Replace `_build_gold_layer()` with modular Gold transform calls. |
| 3.2 | **Add Gold step logging** | Add `print()` banners and `logger.info()` calls matching the Silver pattern. |
| 3.3 | **Add Gold config paths** | Add `GOLD_COMPANY_DATASET_PATH`, `GOLD_SECTOR_ANALYSIS_PATH`, `GOLD_TECHNICAL_PATH`, `GOLD_EXCHANGE_UNIVERSE_PATH` to `config.py`. |

---

## 5. Proposed Gold Output Datasets

### 5.1 — `company_dataset` (Enhanced)

**S3 Path**: `s3a://graywolf--data--lake/stock/gold/source=alphavantage/dataset=company_dataset/`

| Field Category | Key Fields | Source |
|---|---|---|
| Price Data | `symbol`, `day_date`, `open`, `high`, `low`, `close`, `volume` | Silver `daily_time_series` |
| Daily Metrics | `daily_change`, `daily_change_percentage`, `market_movement` | Computed in Gold |
| Rolling Aggregations | `thirty_day_avg_open`, `thirty_day_avg_close`, `fifty_two_week_high`, `fifty_two_week_low`, `all_time_high`, `all_time_low` | Computed in Gold |
| Lag Features | `previous_close`, `previous_open`, `previous_high`, `previous_low` | Computed in Gold |
| Company Fundamentals | `company_name`, `sector`, `industry`, `market_cap`, `pe_ratio`, `eps`, `dividend_yield`, ... | Silver `company_overview` |
| Ticker Metadata | `figi`, `isin`, `type`, `display_symbol` | Silver `ticker_reference` |
| Valuation Signals | `price_vs_analyst_target`, `is_above_50d_ma`, `is_above_200d_ma` | Computed in Gold |

### 5.2 — `sector_analysis` (New)

**S3 Path**: `s3a://graywolf--data--lake/stock/gold/source=alphavantage/dataset=sector_analysis/`

| Field | Description |
|---|---|
| `sector` | Industry sector name |
| `sector_company_count` | Number of companies in sector |
| `sector_total_market_cap` | Sum of all market caps in sector |
| `sector_avg_pe_ratio` | Average P/E ratio across sector |
| `sector_avg_dividend_yield` | Average dividend yield across sector |

### 5.3 — `technical_indicators` (New)

**S3 Path**: `s3a://graywolf--data--lake/stock/gold/source=alphavantage/dataset=technical_indicators/`

| Field | Description |
|---|---|
| `symbol`, `day_date` | Identity key |
| `sma_10`, `sma_20`, `sma_50`, `sma_200` | Simple Moving Averages |
| `ema_12`, `ema_26` | Exponential Moving Averages |
| `macd_line`, `signal_line`, `macd_histogram` | MACD indicator |
| `rsi_14` | 14-day Relative Strength Index |
| `upper_band`, `lower_band` | Bollinger Bands |
| `golden_cross`, `death_cross` | SMA crossover signals |
| `rsi_overbought`, `rsi_oversold` | RSI boundary signals |

### 5.4 — `exchange_universe` (New)

**S3 Path**: `s3a://graywolf--data--lake/stock/gold/source=massive/dataset=exchange_universe/`

| Field | Description |
|---|---|
| `symbol` | Stock ticker |
| `mic` | Operating Market Identifier Code |
| `exchange_name` | Exchange display name |
| `locale` | Geographic locale |
| `asset_class` | Asset classification |
| `security_type` | Common Stock, ETF, ADR, etc. |
| `figi` | Bloomberg FIGI |
| `tickers_per_exchange` | Count of tickers on this exchange |

---

## 6. Implementation Priority Order

```text
Priority 1 (Immediate):
   └── Phase 1: Clean Silver/Gold boundary
        └── Move rolling aggregations + lag features out of daily.py

Priority 2 (Next Session):
   └── Phase 2.1: Enhance gold_company_dataset.py
        └── Join Daily + Overview + Ticker Reference
        └── Compute all analytics in Gold

Priority 3 (Following Session):
   └── Phase 2.3: Build gold_technical_indicators.py
        └── SMA, EMA, MACD, RSI, Bollinger Bands

Priority 4 (Future):
   └── Phase 2.2: Build gold_sector_analysis.py
   └── Phase 2.4: Build gold_exchange_universe.py
   └── Phase 3: Update pipeline orchestration
```

---

## 7. Summary Matrix: Silver → Gold Transformation Map

| Silver Source Dataset | Gold Target Dataset | Join Key | Join Type | What Gets Computed |
|---|---|---|---|---|
| `daily_time_series` | `company_dataset` | `symbol` | Base table | Rolling averages, lag features, daily metrics |
| `company_overview` | `company_dataset` | `symbol` | `LEFT JOIN` | Fundamentals enrichment (P/E, EPS, sector) |
| `ticker_reference` | `company_dataset` | `symbol` | `LEFT JOIN` | FIGI, ISIN, security type enrichment |
| `company_overview` | `sector_analysis` | `sector` | `GROUP BY` | Sector-level aggregations |
| `daily_time_series` | `technical_indicators` | `symbol`, `day_date` | Window functions | SMA, EMA, MACD, RSI, Bollinger |
| `ticker_reference` | `exchange_universe` | `mic` = `operating_mic` | `LEFT JOIN` | Exchange metadata enrichment |
| `exchanges` | `exchange_universe` | `operating_mic` | `LEFT JOIN` | Exchange names, locales, counts |
