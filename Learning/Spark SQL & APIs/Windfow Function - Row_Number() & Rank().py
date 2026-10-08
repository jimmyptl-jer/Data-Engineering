# Databricks notebook source
# MAGIC %sql
# MAGIC WITH ranked AS (
# MAGIC     SELECT
# MAGIC         order_id,
# MAGIC         customer_id,
# MAGIC         order_purchase_timestamp,
# MAGIC         order_status,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY customer_id
# MAGIC             ORDER BY order_purchase_timestamp DESC  -- newest first
# MAGIC         ) AS row_num
# MAGIC     FROM data_engineering.olist_orders_dataset
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     row_num,
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     order_purchase_timestamp,
# MAGIC     order_status
# MAGIC FROM ranked; 
# MAGIC  -- keep only newest per customer

# COMMAND ----------

# MAGIC %sql
# MAGIC describe data_engineering.olist_orders_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC with customer_orders as (
# MAGIC     select 
# MAGIC         order_id,
# MAGIC         customer_id,
# MAGIC         order_status,
# MAGIC         row_number() over(
# MAGIC             partition by customer_id
# MAGIC             order by order_purchase_timestamp desc
# MAGIC         ) as row_number
# MAGIC     from data_engineering.olist_orders_dataset
# MAGIC )
# MAGIC
# MAGIC select * from customer_orders where row_number = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe data_engineering.olist_order_payments_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC explain select * from data_engineering.olist_order_payments_dataset limit 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH order_payments AS (
# MAGIC
# MAGIC     SELECT 
# MAGIC         order_id,
# MAGIC         payment_type,
# MAGIC         payment_value,
# MAGIC         payment_installments,
# MAGIC
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY order_id
# MAGIC             ORDER BY payment_value DESC
# MAGIC         ) AS highest_payment
# MAGIC
# MAGIC     FROM data_engineering.olist_order_payments_dataset
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM order_payments;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH order_payments AS (
# MAGIC
# MAGIC     SELECT 
# MAGIC         order_id,
# MAGIC         payment_type,
# MAGIC         payment_value,
# MAGIC         payment_installments,
# MAGIC
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY order_id
# MAGIC             ORDER BY payment_value DESC
# MAGIC         ) AS payment_rank
# MAGIC
# MAGIC     FROM data_engineering.olist_order_payments_dataset
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM order_payments;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     seller_id, 
# MAGIC     round(SUM(price),2) AS revenue
# MAGIC FROM data_engineering.olist_order_items_dataset
# MAGIC GROUP BY seller_id
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC     seller_id,
# MAGIC     RANK() OVER (
# MAGIC         ORDER BY ROUND(SUM(price), 2) DESC
# MAGIC     ) AS seller_ranking
# MAGIC
# MAGIC FROM data_engineering.olist_order_items_dataset
# MAGIC
# MAGIC GROUP BY seller_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC     seller_id,
# MAGIC     ROUND(SUM(price), 2) AS revenue,
# MAGIC     RANK() OVER (
# MAGIC         ORDER BY ROUND(SUM(price), 2) DESC
# MAGIC     ) AS seller_ranking
# MAGIC FROM data_engineering.olist_order_items_dataset
# MAGIC GROUP BY seller_id
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC     seller_id,
# MAGIC     ROUND(SUM(price), 2) AS revenue,
# MAGIC     RANK() OVER (
# MAGIC         ORDER BY ROUND(SUM(price), 2) DESC
# MAGIC     ) AS seller_ranking
# MAGIC FROM data_engineering.olist_order_items_dataset
# MAGIC GROUP BY seller_id
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe data_engineering.olist_orders_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc data_engineering.olist_sellers_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC     oi.seller_id,
# MAGIC     s.seller_city,
# MAGIC     s.seller_state,
# MAGIC
# MAGIC     ROUND(SUM(price), 2) AS revenue,
# MAGIC
# MAGIC     RANK() OVER (
# MAGIC         PARTITION BY s.seller_city, s.seller_state
# MAGIC         ORDER BY ROUND(SUM(price), 2) DESC
# MAGIC     ) AS seller_ranking
# MAGIC
# MAGIC FROM data_engineering.olist_order_items_dataset oi 
# MAGIC
# MAGIC LEFT JOIN data_engineering.olist_sellers_dataset s 
# MAGIC     ON oi.seller_id = s.seller_id
# MAGIC
# MAGIC GROUP BY 
# MAGIC     s.seller_city,
# MAGIC     s.seller_state,
# MAGIC     oi.seller_id
# MAGIC
# MAGIC order by s.seller_state;

# COMMAND ----------

# MAGIC %sql
# MAGIC with seller_ranking as(
# MAGIC     SELECT 
# MAGIC     oi.seller_id as `Seller ID`,
# MAGIC     s.seller_city as `Seller City`,
# MAGIC     s.seller_state as `Seller State`,
# MAGIC
# MAGIC     ROUND(SUM(price), 2) AS `Seller Revenue`,
# MAGIC
# MAGIC     RANK() OVER (
# MAGIC         PARTITION BY s.seller_city, s.seller_state
# MAGIC         ORDER BY ROUND(SUM(price), 2) DESC
# MAGIC     ) AS `Seller Ranking`
# MAGIC
# MAGIC FROM data_engineering.olist_order_items_dataset oi 
# MAGIC
# MAGIC LEFT JOIN data_engineering.olist_sellers_dataset s 
# MAGIC     ON oi.seller_id = s.seller_id
# MAGIC
# MAGIC GROUP BY 
# MAGIC     s.seller_city,
# MAGIC     s.seller_state,
# MAGIC     oi.seller_id
# MAGIC )
# MAGIC
# MAGIC select * from seller_ranking where `Seller Ranking` <= 3 order by `Seller State`;