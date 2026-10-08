# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists stock_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC use stock_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists stock_catalog.bronze;
# MAGIC create schema if not exists stock_catalog.silver;
# MAGIC create schema if not exists stock_catalog.gold;

# COMMAND ----------

