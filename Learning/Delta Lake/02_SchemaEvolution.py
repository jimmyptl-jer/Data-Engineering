# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# ============================================================
# 1. Spark Session
# ============================================================

spark = SparkSession.builder \
    .appName("DeltaSchemaEvolutionTest") \
    .getOrCreate()


# ============================================================
# 2. Table Configuration
# ============================================================

table_name = "datalake-graywolf.default.deltavolumne"


# ============================================================
# 3. Drop Existing Table
# ============================================================

spark.sql(f"""
DROP TABLE IF EXISTS {table_name}
""")


# ============================================================
# 4. Create Initial Delta Table
# ============================================================

spark.sql(f"""
CREATE TABLE {table_name} (
    id BIGINT,
    symbol STRING,
    price DECIMAL(18,4)
)
USING DELTA
""")


# ============================================================
# 5. Insert Initial Data
# ============================================================

spark.sql(f"""
INSERT INTO {table_name}
VALUES
    (1, 'AAPL', 250.50),
    (2, 'MSFT', 500.25)
""")


# ============================================================
# 6. Read Initial Data
# ============================================================

print("Initial Data:")
display(
    spark.table(table_name)
)


# ============================================================
# 7. Create New Data
#    Notice: NEW column "volume"
# ============================================================

new_data = [
    (3, "GOOG", 280.50, 40000000),
    (4, "AMZN", 230.75, 35000000)
]

new_df = spark.createDataFrame(
    new_data,
    ["id", "symbol", "price", "volume"]
)


# ============================================================
# 8. Add last_refreshed Column
# ============================================================

new_df = new_df.withColumn(
    "last_refreshed",
    current_timestamp()
)


# ============================================================
# 9. Display New Data
# ============================================================

print("New Data:")
display(new_df)


# ============================================================
# 10. Check Incoming Schema
# ============================================================

print("Incoming Data Schema:")
new_df.printSchema()


# ============================================================
# 11. Try Normal Append
#     This should FAIL because the incoming data
#     contains new columns.
# ============================================================

print("Attempting normal append...")

try:

    new_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(table_name)

except Exception as e:

    print("Expected Schema Mismatch:")
    print(e)


# ============================================================
# 12. Append with Schema Evolution
# ============================================================

print("Appending with Schema Evolution...")

new_df.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable(table_name)


# ============================================================
# 13. Check Final Schema
# ============================================================

print("Final Table Schema:")

spark.sql(f"""
DESCRIBE TABLE {table_name}
""").show(truncate=False)


# ============================================================
# 14. Read Final Data
# ============================================================

print("Final Table Data:")

display(
    spark.sql(f"""
        SELECT *
        FROM {table_name}
        ORDER BY id
    """)
)


# ============================================================
# 15. Show Delta Table History
# ============================================================

print("Delta Table History:")

display(
    spark.sql(f"""
        DESCRIBE HISTORY {table_name}
    """)
)