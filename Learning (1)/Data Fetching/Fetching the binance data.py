# Databricks notebook source
import json
import aiohttp
import asyncio
import logging
import boto3

from datetime import datetime

logging.basicConfig(level=logging.INFO)

# Async function
async def fetch_crypto_data():

    url = "https://api.coingecko.com/api/v3/coins/markets"

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params) as response:
                data = await response.json()
                return data
        except aiohttp.ClientResponseError as e:
            logging.error(f"Error fetching data: {e}")
            raise
        except aiohttp.ClientConnectionError as e:
            logging.error(f"Error connecting to server: {e}")
            raise
        except aiohttp.ServerDisconnectedError as e:
            logging.error(f"Server disconnected: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error: {e}")

async def list_s3_buckets():
    access_key = dbutils.secrets.get(
        scope="aws-creds",
        key="aws-access-key-id"
    )
    
    secret_key = dbutils.secrets.get(
        scope="aws-creds",
        key="aws-secret-access-key"
    )

    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    s3 = session.client("s3")

    response = s3.list_buckets()

    print(response)

async def push_data_to_s3_bucket(data):
    bucket_name = "graywolf-crypto-market-data"

    now = datetime.utcnow()
    
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    hour = now.strftime("%H")
    minute = now.strftime("%M")
    second = now.strftime("%S")

    file_name = (
        f"market-data/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"{hour}-{minute}-{second}.json"
    )

    access_key = dbutils.secrets.get(
        scope="aws-creds",
        key="aws-access-key-id"
    )

    secret_key = dbutils.secrets.get(
        scope="aws-creds",
        key="aws-secret-access-key"
    )

    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    s3 = session.client("s3")

    res = s3.put_object(
       Bucket=bucket_name, 
       Key=file_name, 
       Body=json.dumps(data),
       ContentType="application/json"
    )

    return res
    
# Main async function
async def main():
    data = await fetch_crypto_data()
    print(data)

    results = await list_s3_buckets()
    print(results)
    
    try:
        res = await push_data_to_s3_bucket(data)
        print(res)
    except Exception as e:
        print(f"Error uploading to S3: {e}")
    
# Run event loop
await main()