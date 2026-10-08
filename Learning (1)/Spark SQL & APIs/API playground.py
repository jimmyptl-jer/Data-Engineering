# Databricks notebook source
import requests

url = "https://jsonplaceholder.typicode.com/posts/1"

response = requests.get(url)

# Status code
print(response.status_code)

# URL
print(response.url)

# Headers
print(response.headers)

# Response body
print(response.json())


# COMMAND ----------

