# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     order_id,
# MAGIC     order_purchase_timestamp,
# MAGIC
# MAGIC     COALESCE(
# MAGIC         CAST(
# MAGIC             LAG(order_purchase_timestamp) OVER (
# MAGIC                 PARTITION BY customer_id
# MAGIC                 ORDER BY order_purchase_timestamp
# MAGIC             ) AS STRING
# MAGIC         ),
# MAGIC         'N/A'
# MAGIC     ) AS previous_order_date
# MAGIC
# MAGIC FROM data_engineering.olist_orders_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC     o.customer_id,
# MAGIC     o.order_id,
# MAGIC     o.order_purchase_timestamp,
# MAGIC     p.payment_value,
# MAGIC
# MAGIC     COALESCE(
# MAGIC         CAST(
# MAGIC             LAG(p.payment_value) OVER (
# MAGIC                 PARTITION BY o.customer_id
# MAGIC                 ORDER BY o.order_purchase_timestamp
# MAGIC             ) AS DOUBLE
# MAGIC         ),
# MAGIC         0
# MAGIC     ) AS previous_payment
# MAGIC
# MAGIC FROM data_engineering.olist_orders_dataset o
# MAGIC
# MAGIC LEFT JOIN data_engineering.olist_order_payments_dataset p
# MAGIC     ON o.order_id = p.order_id;
# MAGIC
# MAGIC
# MAGIC
# MAGIC SELECT 
# MAGIC     o.customer_id,
# MAGIC     o.order_id,
# MAGIC     o.order_purchase_timestamp,
# MAGIC     p.payment_value,
# MAGIC
# MAGIC     COALESCE(
# MAGIC         LAG(p.payment_value) OVER (
# MAGIC             PARTITION BY o.customer_id
# MAGIC             ORDER BY o.order_purchase_timestamp
# MAGIC         ),
# MAGIC         0
# MAGIC     ) AS previous_payment
# MAGIC
# MAGIC FROM data_engineering.olist_orders_dataset o
# MAGIC
# MAGIC LEFT JOIN data_engineering.olist_order_payments_dataset p
# MAGIC     ON o.order_id = p.order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     o.customer_id,
# MAGIC     o.order_id,
# MAGIC     o.order_purchase_timestamp,
# MAGIC     p.payment_value,
# MAGIC
# MAGIC     COALESCE(
# MAGIC         LAG(p.payment_value) OVER (
# MAGIC             PARTITION BY o.customer_id
# MAGIC             ORDER BY o.order_purchase_timestamp
# MAGIC         ),
# MAGIC         0
# MAGIC     ) AS previous_payment
# MAGIC
# MAGIC FROM data_engineering.olist_orders_dataset o
# MAGIC
# MAGIC LEFT JOIN data_engineering.olist_order_payments_dataset p
# MAGIC     ON o.order_id = p.order_id;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE_TRUNC('month', order_purchase_timestamp)   AS month,
# MAGIC     COUNT(*)                                         AS orders,
# MAGIC     ROUND(SUM(op.payment_value)::NUMERIC, 2)                   AS revenue
# MAGIC FROM data_engineering.olist_orders_dataset o
# MAGIC join data_engineering.olist_order_payments_dataset op
# MAGIC  on o.order_id = op.order_id
# MAGIC GROUP BY DATE_TRUNC('month', order_purchase_timestamp)
# MAGIC ORDER BY month;