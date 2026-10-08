# Databricks notebook source
import kagglehub
import os
import shutil

# ============================================================
# STEP 1: Download Dataset
# ============================================================

print("📌 STEP 1: Downloading dataset from Kaggle")

# kagglehub downloads to its own cache — we capture that path
downloaded_path = kagglehub.dataset_download("dansbecker/powerlifting-database")
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

custom_path = "/Volumes/workspace/default/data_engineering/powerlifting_database"

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
spark.sql("CREATE DATABASE IF NOT EXISTS powerlifting_database")

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
        .saveAsTable(f"powerlifting_database.{table_name}")  # ✅ proper 2/3-part name

    print(f"✅ Table created: powerlifting_database.{table_name}")

print("\n🎉 All tables created successfully!")

    
