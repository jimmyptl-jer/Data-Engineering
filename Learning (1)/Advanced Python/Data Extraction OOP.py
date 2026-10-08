# Databricks notebook source
import pandas as pd

class DataExtraction:

        def __init__(self,file_path):
            self.file_path = file_path
        
        def fetch_json_file(self):
            df = pd.read_json(self.file_path)
            print(df.head)

        def fetch_parquet_file(self):
            df = pd.read_parquet(self.file_path)
            print(df.head)

        def fetch_csv_file(self):
            df = pd.read_csv(self.file_path)
            print(df.head)

reader = DataExtraction()
reader.fetch_json_file()




