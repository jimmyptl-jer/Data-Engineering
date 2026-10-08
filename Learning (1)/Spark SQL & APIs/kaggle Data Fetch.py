# Databricks notebook source
import kagglehub
import os
import shutil

# ============================================================
# STEP 1: Download Dataset
# ============================================================

print("📌 STEP 1: Downloading dataset from Kaggle")

# kagglehub downloads to its own cache — we capture that path
downloaded_path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
print("Downloaded to:", downloaded_path)

# ============================================================
# STEP 2: Check What Files Were Downloaded
# ============================================================

print("📌 STEP 2: Listing downloaded files")
files = os.listdir(downloaded_path)
print("Files:", files)


# ============================================================
# STEP 3: Copy Files to Your Custom Volume Path
# ============================================================
print("📌 STEP 3: Copying files to Databricks Volume")

custom_path = "/Volumes/workspace/default/data_engineering/olist-ecommerce"

os.makedirs(custom_path, exist_ok=True)
print(f"✅ Folder ready: {custom_path}")

for file in files:
    src = os.path.join(downloaded_path, file)
    dest = os.path.join(custom_path,file)
    shutil.copy(src, dest)
    print(f"✅ File copied: {file} from {src} to {dest}")

print("📌 STEP 3: Listing downloaded files")
files = os.listdir(custom_path)
print("Files:", files)

# ============================================================
# STEP 4: Read & Explore Each CSV
# ============================================================
print("📌 STEP 4: Reading files from Volume")
copied_files = os.listdir(custom_path)
print("Files:", copied_files)

for file in copied_files:
    if not file.endswith(".csv"):  # ✅ Skip non-CSV files
        print(f"⏭️ Skipping non-CSV file: {file}")
        continue

    print(f"\n{'='*60}")
    print(f"📄 Currently processing: {file}")  # ✅ Clear label before display()
    print(f"{'='*60}")

    df = (spark.read.format("csv")
          .option("header", "true")
          .option("inferSchema", "true")          # ✅ camelCase is correct
          .load(os.path.join(custom_path, file)))

    print("Schema:")
    df.printSchema()
    print("Columns:", df.columns)
    print("Data types:", df.dtypes)
    df.describe().show()
    display(df)  

# ============================================================
# STEP 5: Write CSVs as Delta Tables
# ============================================================
print("📌 STEP 5: Writing files as Delta tables")

# ✅ Create database if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS data_engineering")

for file in copied_files:
    if not file.endswith(".csv"):
        print(f"⏭️ Skipping non-CSV file: {file}")
        continue

    print(f"\n{'='*60}")
    print(f"📄 Currently processing: {file}")
    print(f"{'='*60}")

    df = (spark.read.format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(os.path.join(custom_path, file)))

    table_name = file.replace(".csv", "").replace("-", "_")  # ✅ clean name

    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"data_engineering.{table_name}")  # ✅ proper 2/3-part name

    print(f"✅ Table created: data_engineering.{table_name}")

print("\n🎉 All tables created successfully!")

    


# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC ============================================================
# MAGIC 📄 Currently processing: olist_order_payments_dataset.csv
# MAGIC ============================================================
# MAGIC ✅ Table created: data_engineering.olist_order_payments_dataset
# MAGIC
# MAGIC ============================================================
# MAGIC 📄 Currently processing: olist_order_reviews_dataset.csv
# MAGIC ============================================================
# MAGIC ✅ Table created: data_engineering.olist_order_reviews_dataset
# MAGIC
# MAGIC ============================================================
# MAGIC 📄 Currently processing: olist_geolocation_dataset.csv
# MAGIC ============================================================
# MAGIC ✅ Table created: data_engineering.olist_geolocation_dataset
# MAGIC
# MAGIC ============================================================
# MAGIC 📄 Currently processing: olist_order_items_dataset.csv
# MAGIC ============================================================
# MAGIC ✅ Table created: data_engineering.olist_order_items_dataset
# MAGIC
# MAGIC ============================================================
# MAGIC 📄 Currently processing: product_category_name_translation.csv
# MAGIC ============================================================
# MAGIC ✅ Table created: data_engineering.product_category_name_translation
# MAGIC
# MAGIC ============================================================
# MAGIC 📄 Currently processing: olist_products_dataset.csv
# MAGIC ============================================================
# MAGIC ✅ Table created: data_engineering.olist_products_dataset
# MAGIC
# MAGIC ============================================================
# MAGIC 📄 Currently processing: olist_sellers_dataset.csv
# MAGIC ============================================================
# MAGIC ✅ Table created: data_engineering.olist_sellers_dataset
# MAGIC
# MAGIC ============================================================
# MAGIC 📄 Currently processing: olist_orders_dataset.csv
# MAGIC ============================================================
# MAGIC ✅ Table created: data_engineering.olist_orders_dataset
# MAGIC
# MAGIC ============================================================
# MAGIC 📄 Currently processing: olist_customers_dataset.csv
# MAGIC ============================================================
# MAGIC ✅ Table created: data_engineering.olist_customers_dataset
# MAGIC
# MAGIC */
# MAGIC
# MAGIC SELECT * FROM data_engineering.olist_order_payments_dataset;
# MAGIC
# MAGIC
# MAGIC SELECT * FROM data_engineering.olist_order_reviews_dataset;
# MAGIC
# MAGIC
# MAGIC select * from data_engineering.olist_geolocation_dataset;
# MAGIC
# MAGIC
# MAGIC select * from data_engineering.olist_order_items_dataset;
# MAGIC
# MAGIC
# MAGIC select * from data_engineering.product_category_name_translation;
# MAGIC
# MAGIC
# MAGIC select * from data_engineering.olist_products_dataset;
# MAGIC
# MAGIC
# MAGIC select * from data_engineering.olist_sellers_dataset;
# MAGIC
# MAGIC
# MAGIC select * from data_engineering.olist_orders_dataset;
# MAGIC
# MAGIC
# MAGIC select * from data_engineering.olist_customers_dataset
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_engineering.olist_geolocation_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_engineering.olist_orders_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(order_status) from data_engineering.olist_orders_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_orders,
# MAGIC
# MAGIC     COUNT(
# MAGIC         CASE 
# MAGIC             WHEN order_status = 'delivered' THEN 1
# MAGIC         END       
# MAGIC     ) AS total_delivered_orders,
# MAGIC
# MAGIC     COUNT(
# MAGIC         CASE 
# MAGIC             WHEN order_status = 'shipped' THEN 1
# MAGIC         END       
# MAGIC     ) AS total_shipped_orders,
# MAGIC
# MAGIC     COUNT(
# MAGIC         CASE 
# MAGIC             WHEN order_status = 'processing' THEN 1
# MAGIC         END       
# MAGIC     ) AS total_processing_orders,
# MAGIC
# MAGIC     COUNT(
# MAGIC         CASE 
# MAGIC             WHEN order_status = 'canceled' THEN 1
# MAGIC         END       
# MAGIC     ) AS total_canceled_orders,
# MAGIC     
# MAGIC     COUNT(
# MAGIC         CASE 
# MAGIC             WHEN order_status = 'created' THEN 1
# MAGIC         END       
# MAGIC     ) AS total_created_orders,
# MAGIC
# MAGIC     COUNT(
# MAGIC         CASE 
# MAGIC             WHEN order_status = 'unavailable' THEN 1
# MAGIC         END       
# MAGIC     ) AS total_unavailable_orders,
# MAGIC
# MAGIC     COUNT(
# MAGIC         CASE 
# MAGIC             WHEN order_status not in ('delivered','shipped','processing','unavailable','canceled','created','approved') THEN 1
# MAGIC         END
# MAGIC     ) AS unknow_status_orders,
# MAGIC
# MAGIC     COUNT(
# MAGIC         CASE 
# MAGIC             WHEN order_status is null THEN 1
# MAGIC         END
# MAGIC     ) AS orders_with_null_status
# MAGIC
# MAGIC FROM data_engineering.olist_orders_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from data_engineering.olist_orders_dataset limit 5;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_engineering.olist_customers_dataset limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC     o.customer_id,
# MAGIC     o.order_id,
# MAGIC     o.order_status,
# MAGIC     c.customer_city
# MAGIC     from data_engineering.olist_orders_dataset o
# MAGIC     left join data_engineering.olist_customers_dataset c
# MAGIC         on o.customer_id = c.customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_engineering.olist_order_reviews_dataset limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC     o.customer_id,
# MAGIC     o.order_id,
# MAGIC     o.order_status,
# MAGIC     r.review_id
# MAGIC     from data_engineering.olist_orders_dataset o
# MAGIC     left join data_engineering.olist_order_reviews_dataset r
# MAGIC         on o.order_id = r.order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC     o.customer_id,
# MAGIC     o.order_id,
# MAGIC     o.order_status,
# MAGIC     r.review_id
# MAGIC     from data_engineering.olist_orders_dataset o
# MAGIC     left join data_engineering.olist_order_reviews_dataset r
# MAGIC         on o.order_id = r.order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- payments table%sql
# MAGIC select 
# MAGIC     o.*,      -- orders table
# MAGIC     c.*,      -- customers table
# MAGIC     o_r.*,    -- reviews table
# MAGIC     op.*,     -- payments table
# MAGIC     oi.*      -- order items table
# MAGIC     from data_engineering.olist_orders_dataset o
# MAGIC      left join data_engineering.olist_customers_dataset c
# MAGIC         on o.customer_id = c.customer_id
# MAGIC     left join data_engineering.olist_order_reviews_dataset o_r
# MAGIC         on o.order_id = o_r.order_id
# MAGIC     left join data_engineering.olist_order_payments_dataset op 
# MAGIC         on o.order_id = op.order_id
# MAGIC     left join data_engineering.olist_order_items_dataset oi 
# MAGIC         on o.order_id = oi.order_id
# MAGIC     where o.order_status is not null
# MAGIC     limit 100;
# MAGIC     
# MAGIC
# MAGIC
# MAGIC     
# MAGIC    
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- payments table%sql
# MAGIC select 
# MAGIC     c.customer_city,
# MAGIC     count(o.order_id) as total_orders
# MAGIC     from data_engineering.olist_orders_dataset o
# MAGIC      left join data_engineering.olist_customers_dataset c
# MAGIC         on o.customer_id = c.customer_id
# MAGIC     left join data_engineering.olist_order_reviews_dataset o_r
# MAGIC         on o.order_id = o_r.order_id
# MAGIC     left join data_engineering.olist_order_payments_dataset op 
# MAGIC         on o.order_id = op.order_id
# MAGIC     left join data_engineering.olist_order_items_dataset oi 
# MAGIC         on o.order_id = oi.order_id
# MAGIC     where o.order_status is not null
# MAGIC group by c.customer_city;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- payments table%sql
# MAGIC select 
# MAGIC     c.customer_city,
# MAGIC     o.order_status,
# MAGIC     count(o.order_id) as total_orders
# MAGIC     from data_engineering.olist_orders_dataset o
# MAGIC      left join data_engineering.olist_customers_dataset c
# MAGIC         on o.customer_id = c.customer_id
# MAGIC     left join data_engineering.olist_order_reviews_dataset o_r
# MAGIC         on o.order_id = o_r.order_id
# MAGIC     left join data_engineering.olist_order_payments_dataset op 
# MAGIC         on o.order_id = op.order_id
# MAGIC     left join data_engineering.olist_order_items_dataset oi 
# MAGIC         on o.order_id = oi.order_id
# MAGIC where o.order_status != 'unavailable'
# MAGIC group by c.customer_city, o.order_status
# MAGIC order by c.customer_city desc;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- payments table%sql
# MAGIC select 
# MAGIC     op.payment_type,
# MAGIC     count(o.order_id) as total_orders
# MAGIC     from data_engineering.olist_orders_dataset o
# MAGIC      left join data_engineering.olist_customers_dataset c
# MAGIC         on o.customer_id = c.customer_id
# MAGIC     left join data_engineering.olist_order_reviews_dataset o_r
# MAGIC         on o.order_id = o_r.order_id
# MAGIC     left join data_engineering.olist_order_payments_dataset op 
# MAGIC         on o.order_id = op.order_id
# MAGIC     left join data_engineering.olist_order_items_dataset oi 
# MAGIC         on o.order_id = oi.order_id
# MAGIC where op.payment_type is not null
# MAGIC group by op.payment_type
# MAGIC order by total_orders desc;
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     o.order_id,
# MAGIC     o.order_status,
# MAGIC     c.customer_city,
# MAGIC     c.customer_state
# MAGIC FROM data_engineering.olist_orders_dataset o
# MAGIC INNER JOIN data_engineering.olist_customers_dataset c
# MAGIC     ON o.customer_id = c.customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- payments table%sql
# MAGIC select 
# MAGIC     o.order_id,
# MAGIC     o.order_status,
# MAGIC     op.payment_type,
# MAGIC     op.payment_value
# MAGIC     from data_engineering.olist_orders_dataset o
# MAGIC     left join data_engineering.olist_order_payments_dataset op 
# MAGIC         on o.order_id = op.order_id
# MAGIC where op.payment_type is null;
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- payments table%sql
# MAGIC select 
# MAGIC     o.order_id,
# MAGIC     op.*
# MAGIC     from data_engineering.olist_orders_dataset o
# MAGIC     left join data_engineering.olist_order_payments_dataset op 
# MAGIC         on o.order_id = op.order_id
# MAGIC where op.order_id is null;
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC     o.order_id,
# MAGIC     op.*
# MAGIC FROM data_engineering.olist_orders_dataset o
# MAGIC INNER JOIN data_engineering.olist_order_payments_dataset op
# MAGIC     ON o.order_id = op.order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(CASE WHEN customer_city  IS NULL THEN 1 END) AS missing_city,
# MAGIC     COUNT(CASE WHEN customer_state IS NULL THEN 1 END) AS missing_state,
# MAGIC     COUNT(CASE WHEN customer_zip_code_prefix IS NULL 
# MAGIC                THEN 1 END)                             AS missing_zip
# MAGIC FROM data_engineering.olist_customers_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, order_status, order_purchase_timestamp
# MAGIC FROM data_engineering.olist_orders_dataset
# MAGIC WHERE order_purchase_timestamp IS NOT NULL
# MAGIC   AND order_delivered_customer_date IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     order_id, 
# MAGIC     order_status, 
# MAGIC     order_purchase_timestamp
# MAGIC FROM data_engineering.olist_orders_dataset
# MAGIC WHERE order_purchase_timestamp IS NULL
# MAGIC   AND order_delivered_customer_date IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*)                        AS total_rows,
# MAGIC     COUNT(customer_city)            AS non_null_city,
# MAGIC     COUNT(customer_state)           AS non_null_state
# MAGIC FROM data_engineering.olist_customers_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC     o.order_id,
# MAGIC     SUM(COALESCE(op.payment_value, 0)) AS total_payment_value
# MAGIC FROM data_engineering.olist_orders_dataset o
# MAGIC INNER JOIN data_engineering.olist_order_payments_dataset op
# MAGIC     ON o.order_id = op.order_id
# MAGIC GROUP BY o.order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     coalesce(order_purchase_timestamp, current_timestamp) AS order_purchase_timestamp,
# MAGIC     coalesce(order_approved_at,current_timestamp) AS order_approved_at,
# MAGIC     coalesce(order_delivered_carrier_date,current_timestamp) AS order_delivered_carrier_date,
# MAGIC     coalesce(order_delivered_customer_date,current_timestamp) AS order_delivered_customer_date,
# MAGIC     coalesce(order_estimated_delivery_date,current_timestamp) AS order_estimated_delivery_date
# MAGIC FROM data_engineering.olist_orders_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_engineering.olist_orders_dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_engineering.olist_customers_dataset limit 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_engineering.olist_orders_dataset limit 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_engineering.olist_order_payments_dataset limit 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- payments table%sql
# MAGIC select 
# MAGIC     coalesce(c.customer_city,'Unknown') as customer_city,
# MAGIC     count(o.order_id) as total_order,
# MAGIC     round(sum(coalesce(op.payment_value,0)),2) total_payment
# MAGIC from data_engineering.olist_orders_dataset o
# MAGIC left join data_engineering.olist_customers_dataset c
# MAGIC         on o.customer_id = c.customer_id
# MAGIC left join data_engineering.olist_order_reviews_dataset o_r
# MAGIC         on o.order_id = o_r.order_id
# MAGIC left join data_engineering.olist_order_payments_dataset op 
# MAGIC         on o.order_id = op.order_id
# MAGIC left join data_engineering.olist_order_items_dataset oi 
# MAGIC         on o.order_id = oi.order_id
# MAGIC group by coalesce(c.customer_city,'Unknown')
# MAGIC order by total_payment desc;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_engineering.olist_order_reviews_dataset limit 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- payments table%sql
# MAGIC select 
# MAGIC     o.order_id,
# MAGIC     coalesce(r.review_score,0) as order_review,
# MAGIC     coalesce(op.payment_type,'no payment') as payment_type
# MAGIC from data_engineering.olist_orders_dataset o
# MAGIC left join data_engineering.olist_customers_dataset c
# MAGIC         on o.customer_id = c.customer_id
# MAGIC left join data_engineering.olist_order_reviews_dataset r
# MAGIC         on o.order_id = r.order_id
# MAGIC left join data_engineering.olist_order_payments_dataset op 
# MAGIC         on o.order_id = op.order_id
# MAGIC left join data_engineering.olist_order_items_dataset oi 
# MAGIC         on o.order_id = oi.order_id;