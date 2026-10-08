# Databricks notebook source


# COMMAND ----------

from nselib import capital_market

# Or specify a custom date range
df = capital_market.price_volume_and_deliverable_position_data(
    symbol='SBIN',
    period='1M',
)

display(df)