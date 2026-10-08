# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE `datalake-graywolf`.`default`.`stock` (
# MAGIC     id BIGINT,
# MAGIC     symbol STRING,
# MAGIC     open_price DECIMAL(18,4),
# MAGIC     high_price DECIMAL(18,4),
# MAGIC     low_price DECIMAL(18,4),
# MAGIC     close_price DECIMAL(18,4),
# MAGIC     volume BIGINT,
# MAGIC     trading_date DATE,
# MAGIC     source STRING,
# MAGIC     ingestion_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS `datalake-graywolf`.`default`.`stock_schema_test`;
# MAGIC
# MAGIC CREATE TABLE `datalake-graywolf`.`default`.`stock_schema_test` (
# MAGIC     id BIGINT,
# MAGIC     symbol STRING,
# MAGIC     price DECIMAL(18,4)
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE `datalake-graywolf`.`default`.`stock_schema_test`;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO `datalake-graywolf`.`default`.`stock_schema_test`
# MAGIC VALUES
# MAGIC     (1, 'AAPL', 250.50),
# MAGIC     (2, 'MSFT', 500.25);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM `datalake-graywolf`.`default`.`stock_schema_test`;

# COMMAND ----------

from pyspark.sql import Row

new_data = [
    (3, "GOOG", 280.50, 40000000),
    (4, "AMZN", 230.75, 35000000)
]

new_df = spark.createDataFrame(
    new_data,
    ["id", "symbol", "price", "volume"]
)

display(new_df)

# COMMAND ----------

new_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("deltavolumne")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

new_df = new_df.withColumn(
    "last_refreshed",
    current_timestamp()
)

# COMMAND ----------

new_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("deltavolumne")

# COMMAND ----------

new_df.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("deltavolumne")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY `datalake-graywolf`.`default`.`stock`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO `datalake-graywolf`.`default`.`stock`
# MAGIC VALUES
# MAGIC (
# MAGIC     1,
# MAGIC     'AAPL',
# MAGIC     220.5000,
# MAGIC     225.7500,
# MAGIC     218.2500,
# MAGIC     224.5000,
# MAGIC     50000000,
# MAGIC     '2026-09-15',
# MAGIC     'alphavantage',
# MAGIC     current_timestamp()
# MAGIC ),
# MAGIC (
# MAGIC     2,
# MAGIC     'MSFT',
# MAGIC     450.2500,
# MAGIC     455.5000,
# MAGIC     448.0000,
# MAGIC     453.7500,
# MAGIC     30000000,
# MAGIC     '2026-09-15',
# MAGIC     'alphavantage',
# MAGIC     current_timestamp()
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE `datalake-graywolf`.`default`.`stock`
# MAGIC SET close_price = 226.0000
# MAGIC WHERE symbol = 'AAPL'
# MAGIC   AND trading_date = '2026-09-15';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW stock_updates AS
# MAGIC
# MAGIC SELECT
# MAGIC     1 AS id,
# MAGIC     'AAPL' AS symbol,
# MAGIC     CAST(220.5000 AS DECIMAL(18,4)) AS open_price,
# MAGIC     CAST(227.0000 AS DECIMAL(18,4)) AS high_price,
# MAGIC     CAST(218.2500 AS DECIMAL(18,4)) AS low_price,
# MAGIC     CAST(226.5000 AS DECIMAL(18,4)) AS close_price,
# MAGIC     52000000 AS volume,
# MAGIC     CAST('2026-09-15' AS DATE) AS trading_date,
# MAGIC     'alphavantage' AS source,
# MAGIC     current_timestamp() AS ingestion_timestamp;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO `datalake-graywolf`.`default`.`stock` AS target
# MAGIC
# MAGIC USING stock_updates AS source
# MAGIC
# MAGIC ON target.symbol = source.symbol
# MAGIC AND target.trading_date = source.trading_date
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET
# MAGIC         target.open_price = source.open_price,
# MAGIC         target.high_price = source.high_price,
# MAGIC         target.low_price = source.low_price,
# MAGIC         target.close_price = source.close_price,
# MAGIC         target.volume = source.volume,
# MAGIC         target.source = source.source,
# MAGIC         target.ingestion_timestamp = source.ingestion_timestamp
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         id,
# MAGIC         symbol,
# MAGIC         open_price,
# MAGIC         high_price,
# MAGIC         low_price,
# MAGIC         close_price,
# MAGIC         volume,
# MAGIC         trading_date,
# MAGIC         source,
# MAGIC         ingestion_timestamp
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.id,
# MAGIC         source.symbol,
# MAGIC         source.open_price,
# MAGIC         source.high_price,
# MAGIC         source.low_price,
# MAGIC         source.close_price,
# MAGIC         source.volume,
# MAGIC         source.trading_date,
# MAGIC         source.source,
# MAGIC         source.ingestion_timestamp
# MAGIC     );

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY `datalake-graywolf`.`default`.`stock`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC RESTORE TABLE `datalake-graywolf`.`default`.`stock`
# MAGIC TO VERSION AS OF 4;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY `datalake-graywolf`.`default`.`stock`;

# COMMAND ----------

# MAGIC %md
# MAGIC