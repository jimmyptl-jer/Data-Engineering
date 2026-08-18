"""
Company Overview — Bronze-to-Silver Transformation Module.

Transforms and merges company reference data from:

    Alpha Vantage Company Overview
                +
    Massive Stock Overview
                ↓
        Unified Company Overview
                ↓
              Silver

The transformation preserves useful company fundamentals from Alpha Vantage
while enriching them with company reference and market metadata from Massive.

Source Datasets:

    Alpha Vantage:
        datasource = alphavantage
        dataset    = company_overview

    Massive:
        datasource = massive
        dataset    = stock_overview

Transformation Flow:

    1. Extract Alpha Vantage Company Overview from Bronze.
    2. Extract Massive Stock Overview from Bronze.
    3. Select required business columns from both sources.
    4. Rename source-specific API fields to standardized snake_case names.
    5. Normalize company identifiers and common attributes.
    6. Normalize exchange information.
    7. Normalize currency and other text fields.
    8. Normalize fake-null values.
    9. Resolve overlapping fields between Alpha Vantage and Massive.
    10. Merge datasets using the stock symbol as the primary business key.
    11. Preserve Alpha Vantage financial and valuation metrics.
    12. Enrich company reference information using Massive.
    13. Apply explicit data type casting.
    14. Add partition columns.
    15. Add Silver processing timestamp.
    16. Validate the resulting company overview records.
    17. Calculate row-count metrics.
    18. Return the unified Silver DataFrame.

Primary Business Key:

    symbol

The stock symbol is used as the primary business key when matching
Alpha Vantage and Massive company records.

Example:

    Alpha Vantage:
        Symbol   = IBM
        Exchange = NYSE

    Massive:
        ticker           = IBM
        primary_exchange = XNYS

The exchange values are source-specific and must not be treated as
identical strings.

Exchange normalization:

    Alpha Vantage:
        NYSE

    Massive:
        XNYS

    Canonical Silver representation:
        exchange_name = New York Stock Exchange
        exchange_mic  = XNYS

Company Identity Fields:

    symbol
    company_name
    asset_type
    type
    cik
    composite_figi
    share_class_figi

Company Reference Fields:

    description
    sector
    industry
    sic_code
    sic_description
    total_employees

Company Location / Contact Fields:

    address1
    address2
    city
    state
    country
    postal_code
    phone_number
    homepage_url

Market / Exchange Fields:

    exchange_name
    exchange_mic
    market
    locale
    currency

Listing Fields:

    list_date
    delisted_utc
    active

Share Information:

    shares_outstanding
    shares_float
    weighted_shares_outstanding
    share_class_shares_outstanding

Alpha Vantage Financial Metrics:

    market_cap
    ebitda
    revenue_ttm
    gross_profit_ttm
    eps
    diluted_eps_ttm
    book_value

Alpha Vantage Profitability Metrics:

    profit_margin
    operating_margin_ttm
    return_on_assets_ttm
    return_on_equity_ttm

Alpha Vantage Valuation Metrics:

    pe_ratio
    peg_ratio
    trailing_pe
    forward_pe
    price_to_sales_ratio_ttm
    price_to_book_ratio
    ev_to_revenue
    ev_to_ebitda

Alpha Vantage Growth Metrics:

    quarterly_earnings_growth_yoy
    quarterly_revenue_growth_yoy

Alpha Vantage Dividend Metrics:

    dividend_per_share
    dividend_yield
    dividend_date
    ex_dividend_date

Alpha Vantage Market Metrics:

    beta
    fifty_two_week_high
    fifty_two_week_low
    fifty_day_moving_average
    two_hundred_day_moving_average

Alpha Vantage Analyst Metrics:

    analyst_target_price
    analyst_rating_strong_buy
    analyst_rating_buy
    analyst_rating_hold
    analyst_rating_sell
    analyst_rating_strong_sell

Alpha Vantage Ownership Metrics:

    percent_insiders
    percent_institutions

Source Resolution:

    When the same business attribute exists in both APIs, the
    transformation applies an explicit source-priority rule.

    Source-specific values should not be blindly overwritten.

    Example:

        Alpha Vantage:
            exchange = NYSE

        Massive:
            primary_exchange = XNYS

        Silver:
            exchange_name = New York Stock Exchange
            exchange_mic  = XNYS

Important Design Principle:

    Bronze preserves the complete raw API responses.

    Silver contains the curated, standardized, and unified company
    overview model.

    Therefore, fields that are not required in the unified Silver
    model should remain available in Bronze rather than being carried
    unnecessarily into Silver.

Output Dataset:

    company_overview

Output Layer:

    Silver

Output Format:

    Parquet
    CSV

"""
