# Databricks notebook source
# MAGIC %sql
# MAGIC select * from data_engineering.olist_customers_dataset limit 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_engineering.olist_orders_dataset limit 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH delivered_orders AS (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         order_id,
# MAGIC         order_purchase_timestamp,
# MAGIC         order_approved_at,
# MAGIC         order_delivered_carrier_date,
# MAGIC         order_delivered_customer_date,
# MAGIC         order_estimated_delivery_date
# MAGIC     FROM data_engineering.olist_orders_dataset
# MAGIC     WHERE order_status = 'delivered'
# MAGIC ) 
# MAGIC
# MAGIC SELECT
# MAGIC     d.order_id,
# MAGIC     d.order_purchase_timestamp,
# MAGIC     d.order_delivered_customer_date,
# MAGIC     c.customer_city,
# MAGIC     c.customer_state
# MAGIC FROM delivered_orders d
# MAGIC LEFT JOIN data_engineering.olist_customers_dataset c
# MAGIC     ON d.customer_id = c.customer_id
# MAGIC ORDER BY d.order_purchase_timestamp DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_engineering.olist_order_payments_dataset limit 20;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH order_total AS (
# MAGIC     SELECT 
# MAGIC         order_id,
# MAGIC         SUM(payment_value) AS total_per_order
# MAGIC     FROM data_engineering.olist_order_payments_dataset
# MAGIC     GROUP BY order_id
# MAGIC ),
# MAGIC customer_total AS (
# MAGIC     SELECT 
# MAGIC         o.customer_id,
# MAGIC         SUM(ot.total_per_order) AS total_spend_per_customer,
# MAGIC         COUNT(ot.order_id) AS orders_per_customer
# MAGIC     FROM data_engineering.olist_orders_dataset o
# MAGIC     JOIN order_total ot
# MAGIC         ON o.order_id = ot.order_id
# MAGIC     GROUP BY o.customer_id
# MAGIC ),
# MAGIC customer_segmentation AS (
# MAGIC     SELECT 
# MAGIC         *,
# MAGIC         CASE 
# MAGIC             WHEN total_spend_per_customer >= 1000 THEN 'Diamond'
# MAGIC             WHEN total_spend_per_customer >= 500 THEN 'Platinum'
# MAGIC             WHEN total_spend_per_customer >= 200 THEN 'Gold'
# MAGIC             WHEN total_spend_per_customer >= 100 THEN 'Silver'
# MAGIC             ELSE 'Bronze'
# MAGIC         END AS customer_segment
# MAGIC     FROM customer_total
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     cs.customer_id,
# MAGIC     cs.orders_per_customer,
# MAGIC     ROUND(cs.total_spend_per_customer, 2) AS total_spend,
# MAGIC     cs.customer_segment,
# MAGIC     c.customer_city,
# MAGIC     c.customer_state
# MAGIC FROM customer_segmentation cs
# MAGIC LEFT JOIN data_engineering.olist_customers_dataset c
# MAGIC     ON cs.customer_id = c.customer_id
# MAGIC ORDER BY total_spend DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH clean_orders AS (
# MAGIC     SELECT
# MAGIC         order_id,
# MAGIC         customer_id,
# MAGIC         order_status,
# MAGIC         order_purchase_timestamp,
# MAGIC         DATE_TRUNC('month', order_purchase_timestamp) AS order_month
# MAGIC     FROM data_engineering.olist_orders_dataset
# MAGIC     WHERE order_id IS NOT NULL
# MAGIC       AND order_purchase_timestamp IS NOT NULL
# MAGIC ),
# MAGIC order_revenue AS (
# MAGIC     SELECT
# MAGIC         co.order_id,
# MAGIC         co.customer_id,
# MAGIC         co.order_status,
# MAGIC         co.order_month,
# MAGIC         COALESCE(SUM(p.payment_value), 0)       AS order_revenue,
# MAGIC         COALESCE(MAX(p.payment_type), 'unknown') AS payment_type
# MAGIC     FROM clean_orders co
# MAGIC     LEFT JOIN data_engineering.olist_order_payments_dataset p
# MAGIC         ON co.order_id = p.order_id
# MAGIC     GROUP BY
# MAGIC         co.order_id,
# MAGIC         co.customer_id,
# MAGIC         co.order_status,
# MAGIC         co.order_month
# MAGIC ),
# MAGIC order_with_location AS (
# MAGIC     SELECT
# MAGIC         orv.order_id,
# MAGIC         orv.order_status,
# MAGIC         orv.order_month,
# MAGIC         orv.order_revenue,
# MAGIC         orv.payment_type,
# MAGIC         COALESCE(c.customer_city, 'Unknown')    AS customer_city,
# MAGIC         COALESCE(c.customer_state, 'Unknown')   AS customer_state
# MAGIC     FROM order_revenue orv
# MAGIC     LEFT JOIN data_engineering.olist_customers_dataset c
# MAGIC         ON orv.customer_id = c.customer_id
# MAGIC ),
# MAGIC monthly_state_summary AS (
# MAGIC     SELECT
# MAGIC         order_month,
# MAGIC         customer_state,
# MAGIC         COUNT(order_id)                         AS total_orders,
# MAGIC         SUM(CASE WHEN order_status = 'delivered'
# MAGIC                  THEN 1 ELSE 0 END)             AS delivered_orders,
# MAGIC         SUM(CASE WHEN order_status = 'canceled'
# MAGIC                  THEN 1 ELSE 0 END)             AS canceled_orders,
# MAGIC         ROUND(SUM(order_revenue), 2)            AS total_revenue,
# MAGIC         ROUND(AVG(order_revenue), 2)            AS avg_order_value,
# MAGIC         COUNT(DISTINCT CASE WHEN payment_type = 'credit_card'
# MAGIC                             THEN order_id END)  AS credit_card_orders
# MAGIC     FROM order_with_location
# MAGIC     GROUP BY order_month, customer_state
# MAGIC )
# MAGIC SELECT
# MAGIC     order_month,
# MAGIC     customer_state,
# MAGIC     total_orders,
# MAGIC     delivered_orders,
# MAGIC     canceled_orders,
# MAGIC     total_revenue,
# MAGIC     avg_order_value,
# MAGIC     credit_card_orders,
# MAGIC     ROUND(canceled_orders * 100.0 / NULLIF(total_orders, 0), 2) AS cancel_rate_pct,
# MAGIC     ROUND(credit_card_orders * 100.0 / NULLIF(total_orders, 0), 2) AS cc_usage_pct,
# MAGIC     CASE
# MAGIC         WHEN total_revenue >= 100000 THEN 'high'
# MAGIC         WHEN total_revenue >= 10000  THEN 'medium'
# MAGIC         ELSE 'low'
# MAGIC     END                                         AS revenue_band
# MAGIC FROM monthly_state_summary
# MAGIC ORDER BY order_month, total_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH order_total AS (
# MAGIC     SELECT 
# MAGIC         order_id,
# MAGIC         ROUND(SUM(payment_value),2) AS total_per_order
# MAGIC     FROM data_engineering.olist_order_payments_dataset
# MAGIC     GROUP BY order_id
# MAGIC     HAVING sum(payment_value) > 500
# MAGIC )
# MAGIC -- ),
# MAGIC -- customer_total AS (
# MAGIC --     SELECT 
# MAGIC --         o.customer_id,
# MAGIC --         SUM(ot.total_per_order) AS total_spend_per_customer,
# MAGIC --         COUNT(ot.order_id) AS orders_per_customer
# MAGIC --     FROM data_engineering.olist_orders_dataset o
# MAGIC --     JOIN order_total ot
# MAGIC --         ON o.order_id = ot.order_id
# MAGIC --     GROUP BY o.customer_id
# MAGIC --     HAVING COUNT(ot.order_id) > 1
# MAGIC -- )
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC     h.order_id,
# MAGIC     h.total_per_order,
# MAGIC     o.order_status,
# MAGIC     o.order_purchase_timestamp
# MAGIC FROM order_total h
# MAGIC LEFT JOIN data_engineering.olist_orders_dataset o
# MAGIC     ON h.order_id = o.order_id
# MAGIC ORDER BY h.total_per_order DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_engineering.olist_order_items_dataset limit 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct product_id from data_engineering.olist_products_dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_engineering.olist_orders_dataset limit 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH product_list AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         product_id,
# MAGIC         product_category_name
# MAGIC     FROM data_engineering.olist_products_dataset
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC     pl.product_category_name,
# MAGIC     pl.product_id
# MAGIC FROM product_list pl
# MAGIC LEFT JOIN data_engineering.olist_order_items_dataset oi
# MAGIC     ON pl.product_id = oi.product_id
# MAGIC LEFT JOIN data_engineering.olist_orders_dataset o
# MAGIC     ON oi.order_id = o.order_id
# MAGIC WHERE o.order_status != 'delivered';

# COMMAND ----------

# MAGIC %sql
# MAGIC describe data_engineering.olist_orders_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe data_engineering.olist_sellers_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe data_engineering.olist_order_items_dataset;

# COMMAND ----------

with selles

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH seller_list AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         seller_id,
# MAGIC         seller_city,
# MAGIC         seller_state
# MAGIC     FROM data_engineering.olist_sellers_dataset
# MAGIC )
# MAGIC
# MAGIC select 
# MAGIC     sl.seller_city,
# MAGIC     sl.seller_state,
# MAGIC     sl.seller_id
# MAGIC from seller_list sl
# MAGIC left join data_engineering.olist_order_items_dataset oi
# MAGIC     on sl.seller_id = oi.seller_id
# MAGIC WHERE oi.seller_id IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH seller_list AS (
# MAGIC     SELECT DISTINCT 
# MAGIC         seller_id
# MAGIC     FROM data_engineering.olist_order_items_dataset
# MAGIC )
# MAGIC
# MAGIC select 
# MAGIC     sl.seller_city,
# MAGIC     sl.seller_state,
# MAGIC     sl.seller_id
# MAGIC from data_engineering.olist_sellers_dataset sl
# MAGIC left join seller_list oi
# MAGIC     on sl.seller_id = oi.seller_id
# MAGIC WHERE oi.seller_id IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH order_total AS (
# MAGIC     SELECT 
# MAGIC         order_id,
# MAGIC         SUM(coalesce(payment_value,0)) AS total_per_order,
# MAGIC         MAX(coalesce(payment_value,0)) AS max_order_valaue,
# MAGIC         min(coalesce(payment_value,0)) AS min_order_value
# MAGIC     FROM data_engineering.olist_order_payments_dataset
# MAGIC ),
# MAGIC
# MAGIC customer_total AS (
# MAGIC     SELECT 
# MAGIC         o.customer_id,
# MAGIC         SUM(ot.total_per_order) AS total_spend_per_customer,
# MAGIC         MAX(ot.max_order_valaue) AS max_order_value,
# MAGIC         MIN(ot.min_order_value) AS min_order_value,
# MAGIC         COUNT(ot.order_id) AS orders_per_customer
# MAGIC     FROM data_engineering.olist_orders_dataset o
# MAGIC     JOIN order_total ot
# MAGIC         ON o.order_id = ot.order_id
# MAGIC     GROUP BY o.customer_id
# MAGIC ),
# MAGIC customer_segmentation AS (
# MAGIC     SELECT 
# MAGIC         *,
# MAGIC         CASE 
# MAGIC             WHEN total_spend_per_customer >= 1000 THEN 'Diamond'
# MAGIC             WHEN total_spend_per_customer >= 500 THEN 'Platinum'
# MAGIC             WHEN total_spend_per_customer >= 200 THEN 'Gold'
# MAGIC             WHEN total_spend_per_customer >= 100 THEN 'Silver'
# MAGIC             ELSE 'Bronze'
# MAGIC         END AS customer_segment
# MAGIC     FROM customer_total
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     cs.customer_id,
# MAGIC     cs.orders_per_customer,
# MAGIC     ROUND(cs.total_spend_per_customer, 2) AS total_spend,
# MAGIC     cs.customer_segment,
# MAGIC     c.customer_city,
# MAGIC     c.customer_state
# MAGIC FROM customer_segmentation cs
# MAGIC LEFT JOIN data_engineering.olist_customers_dataset c
# MAGIC     ON cs.customer_id = c.customer_id
# MAGIC ORDER BY total_spend DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe data_engineering.olist_order_payments_dataset;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH order_total AS (
# MAGIC     SELECT 
# MAGIC         order_id,
# MAGIC         SUM(coalesce(payment_value,0)) AS total_per_order,
# MAGIC         MAX(coalesce(payment_value,0)) AS max_order_valaue,
# MAGIC         min(coalesce(payment_value,0)) AS min_order_value
# MAGIC     FROM data_engineering.olist_order_payments_dataset
# MAGIC     group by order_id
# MAGIC ),
# MAGIC
# MAGIC customer_total AS (
# MAGIC     SELECT 
# MAGIC         o.customer_id,
# MAGIC         SUM(ot.total_per_order) AS total_spend_per_customer,
# MAGIC         MAX(ot.max_order_valaue) AS max_order_value,
# MAGIC         MIN(ot.min_order_value) AS min_order_value,
# MAGIC         COUNT(ot.order_id) AS orders_per_customer
# MAGIC     FROM data_engineering.olist_orders_dataset o
# MAGIC     JOIN order_total ot
# MAGIC         ON o.order_id = ot.order_id
# MAGIC     GROUP BY o.customer_id
# MAGIC )
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC     ct.*,
# MAGIC     c.*
# MAGIC FROM customer_total ct
# MAGIC LEFT JOIN data_engineering.olist_customers_dataset c
# MAGIC     ON ct.customer_id = c.customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC     c.customer_id,
# MAGIC     COUNT(o.order_id) AS total_orders
# MAGIC FROM data_engineering.olist_orders_dataset o
# MAGIC LEFT JOIN data_engineering.olist_customers_dataset c
# MAGIC     ON o.customer_id = c.customer_id
# MAGIC GROUP BY c.customer_id
# MAGIC ORDER BY total_orders DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH customer_orders AS (
# MAGIC     SELECT
# MAGIC         o.customer_id,
# MAGIC         COUNT(o.order_id)               AS total_orders,
# MAGIC         MIN(o.order_purchase_timestamp) AS first_order_date,
# MAGIC         MAX(o.order_purchase_timestamp) AS latest_order_date
# MAGIC     FROM data_engineering.olist_orders_dataset o
# MAGIC     GROUP BY o.customer_id
# MAGIC ),
# MAGIC customer_payments AS (
# MAGIC     SELECT
# MAGIC         o.customer_id,
# MAGIC         COALESCE(SUM(p.payment_value), 0)   AS total_spend,
# MAGIC         COALESCE(AVG(p.payment_value), 0)   AS avg_order_value
# MAGIC     FROM data_engineering.olist_orders_dataset o
# MAGIC     LEFT JOIN data_engineering.olist_order_payments_dataset p
# MAGIC         ON o.order_id = p.order_id
# MAGIC     GROUP BY o.customer_id
# MAGIC )
# MAGIC SELECT
# MAGIC     c.customer_id,
# MAGIC     c.customer_city,
# MAGIC     c.customer_state,
# MAGIC     co.total_orders,
# MAGIC     co.first_order_date,
# MAGIC     co.latest_order_date,
# MAGIC     cp.total_spend,
# MAGIC     cp.avg_order_value,
# MAGIC     CASE
# MAGIC         WHEN cp.total_spend >= 1000 THEN 'platinum'
# MAGIC         WHEN cp.total_spend >= 500  THEN 'gold'
# MAGIC         WHEN cp.total_spend >= 100  THEN 'silver'
# MAGIC         ELSE 'bronze'
# MAGIC     END                                     AS customer_tier
# MAGIC FROM data_engineering.olist_customers_dataset c
# MAGIC LEFT JOIN customer_orders co
# MAGIC     ON c.customer_id = co.customer_id
# MAGIC LEFT JOIN customer_payments cp
# MAGIC     ON c.customer_id = cp.customer_id
# MAGIC ORDER BY cp.total_spend DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_engineering.olist_orders_dataset where customer_id="926b6a6fb8b6081e00b335edaf578d35";