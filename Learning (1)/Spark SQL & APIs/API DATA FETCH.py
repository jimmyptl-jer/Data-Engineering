# Databricks notebook source
import requests

url = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/titanic.csv"
local_path = "/Volumes/workspace/default/data_engineering/fetch_data.csv"

def fetch_data(url, local_path):
    try:
        print(f"📥 Fetching data from url: {url}")

        response = requests.get(url)   # ✅ FIXED
        response.raise_for_status()

        with open(local_path, "wb") as file:
            file.write(response.content)

        print(f"✅ Data saved to: {local_path}")
        
    except Exception as e:
        print(f"❌ Error: {e}")


fetch_data(url, local_path)

df = spark.read.format("csv").option("header","true").option("inferschema","true").load(local_path)

display(df)

df.printSchema()

print(df.columns)

print(df.dtypes)



df.describe().show()

table_name = "titanic"

df.write.format("delta").mode("overwrite").saveAsTable(f"data_engineering.{table_name}")

print(f"✅ Table created: data_engineering.{table_name}")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_engineering.titanic

# COMMAND ----------

# MAGIC %sql
# MAGIC select embark_town as town,count(*) from data_engineering.titanic group by embark_town;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(sex),sum(fare) from from data_engineering.titanic group by sex;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
# MAGIC ============================================================
# MAGIC 📌 PURPOSE
# MAGIC Analyze Titanic passenger data by:
# MAGIC 1. Gender
# MAGIC 2. Embarkation town
# MAGIC
# MAGIC Also calculate:
# MAGIC - Total passengers
# MAGIC - Total fare collected
# MAGIC ============================================================
# MAGIC */
# MAGIC
# MAGIC SELECT 
# MAGIC     
# MAGIC     -- Passenger gender
# MAGIC     sex,
# MAGIC     
# MAGIC     -- Rename embark_town column as town
# MAGIC     embark_town AS town,
# MAGIC     
# MAGIC     -- Count total passengers
# MAGIC     COUNT(sex) AS passenger_count,
# MAGIC     
# MAGIC     -- Calculate total fare
# MAGIC     ROUND(SUM(fare), 2) AS total_fare
# MAGIC
# MAGIC FROM data_engineering.titanic
# MAGIC
# MAGIC -- Remove rows where embark_town is NULL
# MAGIC WHERE embark_town IS NOT NULL
# MAGIC
# MAGIC /*
# MAGIC ============================================================
# MAGIC GROUPING
# MAGIC Create aggregations based on:
# MAGIC - sex
# MAGIC - embark_town
# MAGIC ============================================================
# MAGIC */
# MAGIC GROUP BY 
# MAGIC     sex,
# MAGIC     embark_town
# MAGIC
# MAGIC /*
# MAGIC ============================================================
# MAGIC SORTING
# MAGIC Sort results by embarkation town
# MAGIC ============================================================
# MAGIC */
# MAGIC ORDER BY embark_town;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
# MAGIC ============================================================
# MAGIC 📌 PURPOSE
# MAGIC Analyze Titanic passenger data by:
# MAGIC 1. Gender
# MAGIC 2. Embarkation town
# MAGIC
# MAGIC Also calculate:
# MAGIC - Total passengers
# MAGIC - Total fare collected
# MAGIC ============================================================
# MAGIC */
# MAGIC
# MAGIC SELECT 
# MAGIC     
# MAGIC     sex,
# MAGIC     
# MAGIC     embark_town AS town,
# MAGIC     
# MAGIC     COUNT(sex) AS passenger_count,
# MAGIC     
# MAGIC     ROUND(SUM(fare), 2) AS total_fare
# MAGIC
# MAGIC FROM data_engineering.titanic
# MAGIC
# MAGIC WHERE embark_town IS NOT NULL
# MAGIC
# MAGIC GROUP BY 
# MAGIC     sex,
# MAGIC     embark_town
# MAGIC
# MAGIC HAVING SUM(fare) > 1000
# MAGIC
# MAGIC ORDER BY town;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_engineering.titanic;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC     sex,
# MAGIC     CASE
# MAGIC         WHEN fare <= 10 THEN 'low'
# MAGIC         WHEN fare BETWEEN 10 AND 30 THEN 'medium'
# MAGIC         WHEN fare > 30 THEN 'high'
# MAGIC     END AS fare_category
# MAGIC
# MAGIC FROM data_engineering.titanic;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC     sex,
# MAGIC     COUNT(
# MAGIC         CASE
# MAGIC             WHEN fare <= 10 THEN 1
# MAGIC         END
# MAGIC     ) AS low_fare_count,
# MAGIC
# MAGIC     COUNT(
# MAGIC         CASE
# MAGIC             WHEN fare BETWEEN 10 AND 30 THEN 1
# MAGIC         END
# MAGIC     ) AS medium_fare_count,
# MAGIC
# MAGIC     COUNT(
# MAGIC         CASE
# MAGIC             WHEN fare > 30 THEN 1
# MAGIC         END
# MAGIC     ) AS high_fare_count
# MAGIC
# MAGIC FROM data_engineering.titanic
# MAGIC
# MAGIC -- WHERE embark_town = 'Southampton'
# MAGIC
# MAGIC GROUP by sex;

# COMMAND ----------

